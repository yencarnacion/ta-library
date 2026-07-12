package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

type Config struct {
	ServerPort       int                    `yaml:"server_port" json:"server_port"`
	Timezone         string                 `yaml:"timezone" json:"timezone"`
	InputCSV         string                 `yaml:"input_csv" json:"input_csv"`
	OutputDir        string                 `yaml:"output_dir" json:"output_dir"`
	TradingAgentsDir string                 `yaml:"tradingagents_dir" json:"tradingagents_dir"`
	Models           map[string]ModelConfig `yaml:"models" json:"models"`
	UI               UIConfig               `yaml:"ui" json:"ui"`
	Logging          LoggingConfig          `yaml:"logging" json:"logging"`
}

type ModelConfig struct {
	Label   string            `yaml:"label" json:"label"`
	Script  string            `yaml:"script" json:"script"`
	Args    []string          `yaml:"args" json:"args"`
	Env     map[string]string `yaml:"env" json:"env,omitempty"`
	Enabled bool              `yaml:"enabled" json:"enabled"`
	Default bool              `yaml:"default" json:"default"`
}

type UIConfig struct {
	PollSeconds int `yaml:"poll_seconds" json:"poll_seconds"`
}

type LoggingConfig struct {
	Level string `yaml:"level" json:"level"`
}

type App struct {
	log     *slog.Logger
	cfg     Config
	tz      *time.Location
	symbols []string

	rootCtx context.Context

	mu                  sync.RWMutex
	job                 *JobStatus
	jobCancel           context.CancelFunc
	reports             []ReportRecord
	schedules           []Schedule
	lastScheduleModTime time.Time
}

type GenerateRequest struct {
	Symbols []string `json:"symbols"`
	Models  []string `json:"models"`
	Date    string   `json:"date"`
}

type ScheduleRequest struct {
	Name    string   `json:"name"`
	Symbols []string `json:"symbols"`
	Models  []string `json:"models"`
	Date    string   `json:"date,omitempty"`
	Time    string   `json:"time"`
	Days    []string `json:"days"`
	Enabled bool     `json:"enabled"`
}

type JobStatus struct {
	ID         string     `json:"id"`
	State      string     `json:"state"`
	Total      int        `json:"total"`
	Completed  int        `json:"completed"`
	Failed     int        `json:"failed"`
	Remaining  int        `json:"remaining"`
	Current    *JobItem   `json:"current,omitempty"`
	Items      []JobItem  `json:"items"`
	Message    string     `json:"message"`
	StartedAt  time.Time  `json:"started_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

type JobItem struct {
	Symbol    string    `json:"symbol"`
	Model     string    `json:"model"`
	Status    string    `json:"status"`
	Message   string    `json:"message,omitempty"`
	StartedAt time.Time `json:"started_at,omitempty"`
	EndedAt   time.Time `json:"ended_at,omitempty"`
	ReportURL string    `json:"report_url,omitempty"`
}

type ReportRecord struct {
	ID               string    `json:"id"`
	Ticker           string    `json:"ticker"`
	Model            string    `json:"model"`
	ModelLabel       string    `json:"model_label"`
	AnalysisDate     string    `json:"analysis_date"`
	RunID            string    `json:"run_id"`
	GeneratedAt      time.Time `json:"generated_at"`
	GeneratedDisplay string    `json:"generated_display"`
	Weekday          string    `json:"weekday"`
	Status           string    `json:"status"`
	ExitCode         int       `json:"exit_code"`
	DurationSeconds  float64   `json:"duration_seconds"`
	ReportURL        string    `json:"report_url"`
	FinalURL         string    `json:"final_url,omitempty"`
	IndexURL         string    `json:"index_url,omitempty"`
	ConsoleURL       string    `json:"console_url,omitempty"`
	SourceDir        string    `json:"source_dir"`
	OutputPath       string    `json:"output_path"`
	Latest           bool      `json:"latest"`
	Archived         bool      `json:"archived,omitempty"`
	Error            string    `json:"error,omitempty"`
}

type Schedule struct {
	Name        string     `json:"name"`
	Symbols     []string   `json:"symbols"`
	Models      []string   `json:"models"`
	Date        string     `json:"date,omitempty"`
	Time        string     `json:"time"`
	Days        []string   `json:"days"`
	Enabled     bool       `json:"enabled"`
	LastRunAt   *time.Time `json:"last_run_at,omitempty"`
	LastStatus  string     `json:"last_status,omitempty"`
	LastMessage string     `json:"last_message,omitempty"`
	LastJobID   string     `json:"last_job_id,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

type tradingMetadata struct {
	AnalysisDate string `json:"analysis_date"`
	FinishedAt   string `json:"finished_at"`
	ExitCode     int    `json:"exit_code"`
	IndexURL     string `json:"index_url"`
	FinalHTMLURL string `json:"final_html_url"`
	RawURL       string `json:"raw_url"`
}

func main() {
	_ = godotenv.Load()

	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "path to config.yaml")
	flag.Parse()

	cfg, err := loadConfig(configPath)
	if err != nil {
		panic(err)
	}
	logger := newLogger(cfg.Logging.Level)
	tz, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		panic(err)
	}
	if flag.NArg() > 0 && flag.Arg(0) == "cron" {
		if err := handleCronCLI(cfg, tz, flag.Args()[1:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	symbols, err := loadSymbols(cfg.InputCSV)
	if err != nil {
		panic(err)
	}
	if len(symbols) == 0 {
		panic("input csv did not contain any symbols")
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		panic(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	app := &App{
		log:     logger,
		cfg:     cfg,
		tz:      tz,
		symbols: symbols,
		rootCtx: ctx,
		job:     idleJob(tz),
	}
	if err := app.loadReportIndex(); err != nil {
		logger.Warn("could not load report index", "err", err)
	}
	if err := app.loadSchedules(); err != nil {
		logger.Warn("could not load schedules", "err", err)
	}
	go app.scheduleLoop(ctx)

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.ServerPort),
		Handler:           app.routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.Info("ta-library listening", "url", fmt.Sprintf("http://localhost:%d", cfg.ServerPort), "symbols", len(symbols))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}

func loadConfig(path string) (Config, error) {
	cfg := Config{
		ServerPort:       9090,
		Timezone:         "America/New_York",
		InputCSV:         "top-100.csv",
		OutputDir:        "output",
		TradingAgentsDir: "/home/yamir/Documents/daytrading/tradingagents",
		UI:               UIConfig{PollSeconds: 2},
		Logging:          LoggingConfig{Level: "info"},
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return cfg, err
	}
	if v := strings.TrimSpace(os.Getenv("SERVER_PORT")); v != "" {
		var port int
		if _, err := fmt.Sscanf(v, "%d", &port); err == nil && port > 0 {
			cfg.ServerPort = port
		}
	}
	if v := strings.TrimSpace(os.Getenv("TRADINGAGENTS_DIR")); v != "" {
		cfg.TradingAgentsDir = v
	}
	if v := strings.TrimSpace(os.Getenv("OUTPUT_DIR")); v != "" {
		cfg.OutputDir = v
	}
	if v := strings.TrimSpace(os.Getenv("INPUT_CSV")); v != "" {
		cfg.InputCSV = v
	}
	if cfg.UI.PollSeconds <= 0 {
		cfg.UI.PollSeconds = 2
	}
	if cfg.Models == nil {
		cfg.Models = map[string]ModelConfig{}
	}
	for name, model := range cfg.Models {
		if strings.TrimSpace(model.Label) == "" {
			model.Label = name
		}
		cfg.Models[name] = model
	}
	return cfg, nil
}

func loadSymbols(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := csv.NewReader(f)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	symbolCol := 0
	start := 0
	for i, header := range rows[0] {
		if strings.EqualFold(strings.TrimSpace(header), "symbol") {
			symbolCol = i
			start = 1
			break
		}
	}
	seen := map[string]bool{}
	var symbols []string
	for _, row := range rows[start:] {
		if symbolCol >= len(row) {
			continue
		}
		symbol := strings.ToUpper(strings.TrimSpace(row[symbolCol]))
		if symbol == "" || seen[symbol] {
			continue
		}
		seen[symbol] = true
		symbols = append(symbols, symbol)
	}
	return symbols, nil
}

func (a *App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join("web", "index.html"))
	})
	mux.HandleFunc("GET /app.js", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join("web", "app.js"))
	})
	mux.HandleFunc("GET /styles.css", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join("web", "styles.css"))
	})
	mux.Handle("GET /reports/", http.StripPrefix("/reports/", http.FileServer(http.Dir(a.cfg.OutputDir))))
	mux.HandleFunc("GET /api/health", a.handleHealth)
	mux.HandleFunc("GET /api/config", a.handleConfig)
	mux.HandleFunc("GET /api/status", a.handleStatus)
	mux.HandleFunc("GET /api/reports", a.handleReports)
	mux.HandleFunc("DELETE /api/reports", a.handleDeleteReport)
	mux.HandleFunc("POST /api/reports/archive", a.handleArchiveReport)
	mux.HandleFunc("POST /api/generate", a.handleGenerate)
	mux.HandleFunc("POST /api/abort", a.handleAbort)
	mux.HandleFunc("GET /api/schedules", a.handleSchedules)
	return mux
}

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	a.mu.RLock()
	state := a.job.State
	a.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": state, "symbols": len(a.symbols)})
}

func (a *App) handleConfig(w http.ResponseWriter, r *http.Request) {
	models := map[string]ModelConfig{}
	for name, model := range a.cfg.Models {
		if model.Enabled {
			models[name] = model
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"symbols": symbolsWithAll(a.symbols),
		"models":  models,
		"ui":      a.cfg.UI,
	})
}

func (a *App) handleStatus(w http.ResponseWriter, r *http.Request) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	writeJSON(w, http.StatusOK, a.job)
}

func (a *App) handleReports(w http.ResponseWriter, r *http.Request) {
	sortBy := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("sort")))
	a.mu.RLock()
	var reports []ReportRecord
	var archived []ReportRecord
	for _, report := range a.reports {
		if report.Archived {
			archived = append(archived, report)
			continue
		}
		reports = append(reports, report)
	}
	a.mu.RUnlock()
	sortReports(reports, sortBy)
	sortReports(archived, sortBy)
	writeJSON(w, http.StatusOK, map[string]any{"reports": reports, "archived_reports": archived})
}

func (a *App) handleDeleteReport(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "report id is required"})
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range a.reports {
		if a.reports[i].ID != id {
			continue
		}
		if !a.reports[i].Archived {
			writeJSON(w, http.StatusConflict, map[string]any{"error": "only archived reports can be deleted"})
			return
		}
		path := a.reportDiskPath(a.reports[i])
		if !a.pathInArchive(path) {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "archived report path is invalid"})
			return
		}
		if err := os.RemoveAll(path); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		a.removeEmptyParents(filepath.Dir(path), a.archiveRoot())
		a.reports = append(a.reports[:i], a.reports[i+1:]...)
		a.markLatestLocked()
		if err := a.saveReportIndexLocked(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	writeJSON(w, http.StatusNotFound, map[string]any{"error": "report was not found"})
}

func (a *App) handleArchiveReport(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "report id is required"})
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range a.reports {
		if a.reports[i].ID != id {
			continue
		}
		if a.reports[i].Archived {
			writeJSON(w, http.StatusOK, map[string]any{"ok": true})
			return
		}
		if err := a.archiveReportLocked(i); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		a.markLatestLocked()
		if err := a.saveReportIndexLocked(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	writeJSON(w, http.StatusNotFound, map[string]any{"error": "report was not found"})
}

func (a *App) handleSchedules(w http.ResponseWriter, r *http.Request) {
	if err := a.reloadSchedulesIfChanged(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	a.mu.RLock()
	schedules := append([]Schedule(nil), a.schedules...)
	a.mu.RUnlock()
	sort.Slice(schedules, func(i, j int) bool { return schedules[i].Name < schedules[j].Name })
	writeJSON(w, http.StatusOK, map[string]any{"schedules": schedules})
}

func (a *App) handleGenerate(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req GenerateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	symbols, err := a.resolveSymbols(req.Symbols)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	models, err := a.resolveModels(req.Models)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	date := strings.TrimSpace(req.Date)
	if date != "" {
		if _, err := time.Parse("2006-01-02", date); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "date must use YYYY-MM-DD"})
			return
		}
	}

	job := newJob(a.tz, symbols, models)
	a.mu.Lock()
	if a.job != nil && a.job.State == "running" {
		a.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]any{"error": "a report job is already running"})
		return
	}
	a.job = job
	a.mu.Unlock()

	go a.runJob(job.ID, symbols, models, date)
	writeJSON(w, http.StatusAccepted, job)
}

func (a *App) handleAbort(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	if a.job == nil || a.job.State != "running" {
		a.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]any{"error": "no report job is running"})
		return
	}
	if a.jobCancel != nil {
		a.jobCancel()
	}
	a.job.Message = "abort requested"
	a.job.UpdatedAt = time.Now().In(a.tz)
	job := a.job
	a.mu.Unlock()
	writeJSON(w, http.StatusAccepted, job)
}

func (a *App) resolveSymbols(raw []string) ([]string, error) {
	if len(raw) == 0 {
		return nil, errors.New("select at least one symbol")
	}
	if len(raw) == 1 && strings.EqualFold(strings.TrimSpace(raw[0]), "all") {
		return append([]string(nil), a.symbols...), nil
	}
	seen := map[string]bool{}
	var symbols []string
	for _, item := range raw {
		symbol := strings.ToUpper(strings.TrimSpace(item))
		if symbol == "" || seen[symbol] {
			continue
		}
		if !validTicker(symbol) {
			return nil, fmt.Errorf("%s is not a valid ticker", symbol)
		}
		seen[symbol] = true
		symbols = append(symbols, symbol)
	}
	if len(symbols) == 0 {
		return nil, errors.New("select at least one symbol")
	}
	return symbols, nil
}

func validTicker(symbol string) bool {
	if len(symbol) == 0 || len(symbol) > 20 {
		return false
	}
	for _, r := range symbol {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-' {
			continue
		}
		return false
	}
	return true
}

func (a *App) resolveModels(raw []string) ([]string, error) {
	if len(raw) == 0 {
		return nil, errors.New("select at least one model")
	}
	seen := map[string]bool{}
	var models []string
	for _, item := range raw {
		name := strings.ToLower(strings.TrimSpace(item))
		if name == "" || seen[name] {
			continue
		}
		model, ok := a.cfg.Models[name]
		if !ok || !model.Enabled {
			return nil, fmt.Errorf("%s is not an enabled model", name)
		}
		seen[name] = true
		models = append(models, name)
	}
	if len(models) == 0 {
		return nil, errors.New("select at least one model")
	}
	return models, nil
}

func (a *App) runJob(jobID string, symbols, models []string, date string) {
	jobCtx, cancel := context.WithCancel(a.rootCtx)
	a.mu.Lock()
	if a.job != nil && a.job.ID == jobID {
		a.jobCancel = cancel
	}
	a.mu.Unlock()
	defer func() {
		cancel()
		a.mu.Lock()
		if a.job != nil && a.job.ID == jobID {
			a.jobCancel = nil
		}
		a.mu.Unlock()
	}()
	for _, symbol := range symbols {
		for _, model := range models {
			select {
			case <-jobCtx.Done():
				a.finishJob(jobID, "canceled", "server stopped")
				return
			default:
			}
			item := JobItem{Symbol: symbol, Model: model, Status: "running", StartedAt: time.Now().In(a.tz)}
			a.startItem(jobID, item)
			record, err := a.generateOne(jobCtx, symbol, model, date)
			if record.ID != "" {
				a.addReport(record)
			}
			if err != nil {
				item.Status = "failed"
				item.Message = err.Error()
				if errors.Is(jobCtx.Err(), context.Canceled) {
					item.Status = "canceled"
					item.Message = "job aborted"
				}
				item.ReportURL = record.ReportURL
				item.EndedAt = time.Now().In(a.tz)
				a.completeItem(jobID, item, nil)
				if errors.Is(jobCtx.Err(), context.Canceled) {
					a.finishJob(jobID, "canceled", "job aborted")
					return
				}
				continue
			}
			item.Status = "done"
			item.Message = "report ready"
			item.ReportURL = record.ReportURL
			item.EndedAt = time.Now().In(a.tz)
			a.completeItem(jobID, item, &record)
		}
	}
	a.finishJob(jobID, "done", "all requested reports finished")
}

func (a *App) generateOne(ctx context.Context, symbol, modelName, date string) (ReportRecord, error) {
	model := a.cfg.Models[modelName]
	started := time.Now()
	sourceRoot := filepath.Join(a.cfg.TradingAgentsDir, "output")
	before := listDirNames(sourceRoot)

	script := model.Script
	if !filepath.IsAbs(script) {
		script = filepath.Join(a.cfg.TradingAgentsDir, script)
	}
	args := append([]string(nil), model.Args...)
	args = append(args, symbol)
	if date != "" {
		args = append(args, date)
	}

	cmd := exec.CommandContext(ctx, script, args...)
	cmd.Dir = a.cfg.TradingAgentsDir
	cmd.Env = modelEnv(model.Env)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	err := cmd.Run()
	if errors.Is(ctx.Err(), context.Canceled) {
		err = ctx.Err()
	}
	exitCode := 0
	if err != nil {
		exitCode = 1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		}
	}

	sourceDir, findErr := findNewRunDir(sourceRoot, before, symbol, started.Add(-2*time.Second), date)
	if findErr != nil {
		if err != nil {
			return ReportRecord{}, fmt.Errorf("tradingagents command failed and no output directory was found: %s", tailString(output.String(), 3000))
		}
		return ReportRecord{}, findErr
	}
	record, copyErr := a.copyRun(symbol, modelName, model.Label, sourceDir, started, exitCode, err)
	if copyErr != nil {
		return ReportRecord{}, copyErr
	}
	if err != nil {
		record.Status = "failed"
		record.Error = tailString(output.String(), 3000)
		if errors.Is(err, context.Canceled) {
			record.Status = "canceled"
			record.Error = "job aborted"
		}
		return record, fmt.Errorf("%s %s exited with %d: %s", symbol, modelName, exitCode, tailString(output.String(), 1000))
	}
	return record, nil
}

func modelEnv(extra map[string]string) []string {
	env := os.Environ()
	for key, value := range extra {
		env = append(env, fmt.Sprintf("%s=%s", key, value))
	}
	return env
}

func listDirNames(root string) map[string]bool {
	entries, err := os.ReadDir(root)
	if err != nil {
		return map[string]bool{}
	}
	names := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() {
			names[entry.Name()] = true
		}
	}
	return names
}

func findNewRunDir(root string, before map[string]bool, symbol string, minTime time.Time, analysisDate string) (string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", err
	}
	type candidate struct {
		path string
		mod  time.Time
	}
	var candidates []candidate
	prefix := strings.ToLower(symbol) + "-"
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(strings.ToLower(entry.Name()), prefix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if before[entry.Name()] && info.ModTime().Before(minTime) {
			continue
		}
		path := filepath.Join(root, entry.Name())
		if analysisDate != "" {
			meta, _ := readTradingMetadata(filepath.Join(path, "metadata.json"))
			if meta.AnalysisDate != "" && meta.AnalysisDate != analysisDate {
				continue
			}
		}
		candidates = append(candidates, candidate{path: path, mod: info.ModTime()})
	}
	if len(candidates) == 0 {
		return "", errors.New("no new tradingagents output directory found")
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].mod.After(candidates[j].mod) })
	return candidates[0].path, nil
}

func (a *App) copyRun(symbol, modelName, modelLabel, sourceDir string, started time.Time, exitCode int, runErr error) (ReportRecord, error) {
	runID := filepath.Base(sourceDir)
	targetDir := filepath.Join(a.cfg.OutputDir, symbol, modelName, runID)
	targetDir = uniquePath(targetDir)
	if err := copyDir(sourceDir, targetDir); err != nil {
		return ReportRecord{}, err
	}

	meta, _ := readTradingMetadata(filepath.Join(targetDir, "metadata.json"))
	generatedAt := time.Now().In(a.tz)
	if meta.FinishedAt != "" {
		if parsed, err := parseMetadataTime(meta.FinishedAt, a.tz); err == nil {
			generatedAt = parsed
		}
	}
	if meta.AnalysisDate == "" {
		meta.AnalysisDate = generatedAt.Format("2006-01-02")
	}
	rel := filepath.ToSlash(strings.TrimPrefix(targetDir, strings.TrimRight(a.cfg.OutputDir, string(os.PathSeparator))+string(os.PathSeparator)))
	baseURL := "/reports/" + rel
	status := "ready"
	errText := ""
	if runErr != nil {
		status = "failed"
		errText = runErr.Error()
	}
	record := ReportRecord{
		ID:               fmt.Sprintf("%s:%s:%s", symbol, modelName, filepath.Base(targetDir)),
		Ticker:           symbol,
		Model:            modelName,
		ModelLabel:       modelLabel,
		AnalysisDate:     meta.AnalysisDate,
		RunID:            filepath.Base(targetDir),
		GeneratedAt:      generatedAt,
		GeneratedDisplay: generatedAt.Format("Monday, January 2, 2006 15:04:05 MST"),
		Weekday:          generatedAt.Weekday().String(),
		Status:           status,
		ExitCode:         exitCode,
		DurationSeconds:  time.Since(started).Seconds(),
		ReportURL:        baseURL + "/report.html",
		FinalURL:         optionalURL(baseURL, "final.html"),
		IndexURL:         optionalURL(baseURL, "index.html"),
		ConsoleURL:       optionalURL(baseURL, "console.txt"),
		SourceDir:        sourceDir,
		OutputPath:       targetDir,
		Latest:           true,
		Error:            errText,
	}
	if err := writeReportMetadata(targetDir, record); err != nil {
		return ReportRecord{}, err
	}
	if err := writeReportHTML(targetDir, record); err != nil {
		return ReportRecord{}, err
	}
	return record, nil
}

func optionalURL(baseURL, name string) string {
	return baseURL + "/" + name
}

func uniquePath(path string) string {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return path
	}
	for i := 2; ; i++ {
		candidate := fmt.Sprintf("%s-%d", path, i)
		if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
			return candidate
		}
	}
}

func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return copyFile(path, target, info.Mode())
	})
}

func copyFile(src, dst string, mode fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}

func readTradingMetadata(path string) (tradingMetadata, error) {
	var meta tradingMetadata
	raw, err := os.ReadFile(path)
	if err != nil {
		return meta, err
	}
	return meta, json.Unmarshal(raw, &meta)
}

func parseMetadataTime(raw string, tz *time.Location) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return parsed.In(tz), nil
	}
	if parsed, err := time.ParseInLocation("2006-01-02T15:04:05", raw, tz); err == nil {
		return parsed.In(tz), nil
	}
	return time.ParseInLocation("2006-01-02 15:04:05", raw, tz)
}

func writeReportMetadata(dir string, record ReportRecord) error {
	raw, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "ta-library-report.json"), raw, 0o644)
}

func writeReportHTML(dir string, record ReportRecord) error {
	const page = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{.Ticker}} {{.ModelLabel}} Report</title>
  <style>
    :root { color-scheme: light; --bg: #f7f8fa; --panel: #fff; --text: #1e252d; --muted: #627080; --line: #dde3ea; --accent: #166a5b; font-family: "Atkinson Hyperlegible", Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--text); }
    header { display: flex; justify-content: space-between; gap: 16px; align-items: center; padding: 16px 22px; background: var(--panel); border-bottom: 1px solid var(--line); position: sticky; top: 0; z-index: 2; }
    h1 { margin: 0; font-size: 21px; letter-spacing: 0; }
    p { margin: 4px 0 0; color: var(--muted); }
    nav { display: flex; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
    a { color: var(--accent); font-weight: 750; text-decoration: none; }
    .button { height: 34px; border: 1px solid var(--line); background: #fff; border-radius: 6px; padding: 7px 12px; }
    main { height: calc(100vh - 75px); }
    iframe { width: 100%; height: 100%; border: 0; background: #fff; }
    @media (max-width: 760px) { header { align-items: flex-start; flex-direction: column; } nav { justify-content: flex-start; } main { height: calc(100vh - 132px); } }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>{{.Ticker}} {{.ModelLabel}} Report</h1>
      <p>Generated {{.GeneratedDisplay}} by {{.ModelLabel}} for analysis date {{.AnalysisDate}}.</p>
    </div>
    <nav>
      <a class="button" href="final.html" target="_blank" rel="noopener">Final</a>
      <a class="button" href="index.html" target="_blank" rel="noopener">Index</a>
      <a class="button" href="console.txt" target="_blank" rel="noopener">Console</a>
      <a class="button" href="ta-library-report.json" target="_blank" rel="noopener">Metadata</a>
    </nav>
  </header>
  <main><iframe src="final.html" title="{{.Ticker}} {{.ModelLabel}} final report"></iframe></main>
</body>
</html>
`
	tpl, err := template.New("report").Parse(page)
	if err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(dir, "report.html"))
	if err != nil {
		return err
	}
	defer f.Close()
	return tpl.Execute(f, record)
}

func (a *App) loadReportIndex() error {
	path := a.indexPath()
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var payload struct {
		Reports []ReportRecord `json:"reports"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return err
	}
	a.reports = payload.Reports
	if changed, err := a.reconcileReportsLocked(); err != nil {
		return err
	} else if changed {
		if err := a.saveReportIndexLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) saveReportIndexLocked() error {
	payload := struct {
		UpdatedAt time.Time      `json:"updated_at"`
		Reports   []ReportRecord `json:"reports"`
	}{
		UpdatedAt: time.Now().In(a.tz),
		Reports:   a.reports,
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	tmp := a.indexPath() + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, a.indexPath())
}

func (a *App) indexPath() string {
	return filepath.Join(a.cfg.OutputDir, "report-index.json")
}

func (a *App) addReport(record ReportRecord) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range a.reports {
		if a.reports[i].ID == record.ID {
			a.reports[i] = record
			if _, err := a.reconcileReportsLocked(); err != nil {
				a.log.Warn("could not archive old reports", "err", err)
			}
			if err := a.saveReportIndexLocked(); err != nil {
				a.log.Warn("could not save report index", "err", err)
			}
			return
		}
	}
	a.reports = append(a.reports, record)
	if _, err := a.reconcileReportsLocked(); err != nil {
		a.log.Warn("could not archive old reports", "err", err)
	}
	if err := a.saveReportIndexLocked(); err != nil {
		a.log.Warn("could not save report index", "err", err)
	}
}

func (a *App) markLatestLocked() {
	latest := map[string]int{}
	for i := range a.reports {
		a.reports[i].Latest = false
		if a.reports[i].Archived {
			continue
		}
		key := a.reports[i].Ticker + ":" + a.reports[i].Model
		current, ok := latest[key]
		if !ok || a.reports[i].GeneratedAt.After(a.reports[current].GeneratedAt) {
			latest[key] = i
		}
	}
	for _, idx := range latest {
		a.reports[idx].Latest = true
	}
}

func (a *App) reconcileReportsLocked() (bool, error) {
	changed := a.normalizeReportLocationsLocked()
	a.markLatestLocked()
	for i := range a.reports {
		if a.reports[i].Archived || a.reports[i].Latest {
			continue
		}
		if err := a.archiveReportLocked(i); err != nil {
			return changed, err
		}
		changed = true
	}
	a.markLatestLocked()
	return changed, nil
}

func (a *App) normalizeReportLocationsLocked() bool {
	changed := false
	for i := range a.reports {
		if a.reports[i].OutputPath == "" {
			continue
		}
		archived := a.pathInArchive(a.reportDiskPath(a.reports[i]))
		if a.reports[i].Archived != archived {
			a.reports[i].Archived = archived
			changed = true
		}
	}
	return changed
}

func (a *App) archiveReportLocked(idx int) error {
	record := a.reports[idx]
	src := a.reportDiskPath(record)
	if !a.pathInOutput(src) || a.pathInArchive(src) {
		return fmt.Errorf("cannot archive report outside output directory: %s", src)
	}
	if _, err := os.Stat(src); err != nil {
		return err
	}
	dst := uniquePath(filepath.Join(a.archiveRoot(), record.Ticker, record.Model, record.RunID))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	a.removeEmptyParents(filepath.Dir(src), a.cfg.OutputDir)
	record.Archived = true
	record.Latest = false
	a.setReportPath(&record, dst)
	if err := writeReportMetadata(dst, record); err != nil {
		return err
	}
	a.reports[idx] = record
	return nil
}

func (a *App) setReportPath(record *ReportRecord, path string) {
	record.OutputPath = path
	record.RunID = filepath.Base(path)
	record.ID = fmt.Sprintf("%s:%s:%s", record.Ticker, record.Model, record.RunID)
	baseURL := a.reportBaseURL(path)
	record.ReportURL = baseURL + "/report.html"
	record.FinalURL = optionalURL(baseURL, "final.html")
	record.IndexURL = optionalURL(baseURL, "index.html")
	record.ConsoleURL = optionalURL(baseURL, "console.txt")
}

func (a *App) reportBaseURL(path string) string {
	outputAbs, err := filepath.Abs(a.cfg.OutputDir)
	if err != nil {
		return "/reports/" + filepath.ToSlash(filepath.Base(path))
	}
	pathAbs, err := filepath.Abs(path)
	if err != nil {
		return "/reports/" + filepath.ToSlash(filepath.Base(path))
	}
	rel, err := filepath.Rel(outputAbs, pathAbs)
	if err != nil {
		return "/reports/" + filepath.ToSlash(filepath.Base(path))
	}
	return "/reports/" + filepath.ToSlash(rel)
}

func (a *App) reportDiskPath(record ReportRecord) string {
	if strings.TrimSpace(record.OutputPath) != "" {
		return filepath.Clean(record.OutputPath)
	}
	if strings.HasPrefix(record.ReportURL, "/reports/") {
		rel := strings.TrimPrefix(record.ReportURL, "/reports/")
		rel = strings.TrimSuffix(rel, "/report.html")
		return filepath.Join(a.cfg.OutputDir, filepath.FromSlash(rel))
	}
	return filepath.Join(a.cfg.OutputDir, record.Ticker, record.Model, record.RunID)
}

func (a *App) archiveRoot() string {
	return filepath.Join(a.cfg.OutputDir, "archived")
}

func (a *App) pathInOutput(path string) bool {
	return pathWithin(path, a.cfg.OutputDir)
}

func (a *App) pathInArchive(path string) bool {
	return pathWithin(path, a.archiveRoot())
}

func pathWithin(path, root string) bool {
	pathAbs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(rootAbs, pathAbs)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
}

func (a *App) removeEmptyParents(start, stop string) {
	stopAbs, err := filepath.Abs(stop)
	if err != nil {
		return
	}
	for dir := filepath.Clean(start); ; dir = filepath.Dir(dir) {
		dirAbs, err := filepath.Abs(dir)
		if err != nil || dirAbs == stopAbs || !pathWithin(dir, stop) {
			return
		}
		if err := os.Remove(dir); err != nil {
			return
		}
	}
}

func (a *App) scheduleLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	a.checkSchedules()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.checkSchedules()
		}
	}
}

func (a *App) checkSchedules() {
	if err := a.reloadSchedulesIfChanged(); err != nil {
		a.log.Warn("could not reload schedules", "err", err)
		return
	}
	now := time.Now().In(a.tz)
	a.mu.RLock()
	schedules := append([]Schedule(nil), a.schedules...)
	a.mu.RUnlock()
	for _, schedule := range schedules {
		if !scheduleDue(schedule, now) {
			continue
		}
		a.startScheduledJob(schedule, now)
	}
}

func scheduleDue(schedule Schedule, now time.Time) bool {
	if !schedule.Enabled || strings.TrimSpace(schedule.Time) == "" {
		return false
	}
	if now.Format("15:04") != schedule.Time {
		return false
	}
	if !dayAllowed(schedule.Days, now.Weekday()) {
		return false
	}
	if schedule.LastRunAt != nil && schedule.LastRunAt.In(now.Location()).Format("2006-01-02 15:04") == now.Format("2006-01-02 15:04") {
		return false
	}
	return true
}

func dayAllowed(days []string, weekday time.Weekday) bool {
	if len(days) == 0 {
		return true
	}
	want := strings.ToLower(weekday.String()[:3])
	for _, day := range days {
		if strings.ToLower(strings.TrimSpace(day)) == want {
			return true
		}
	}
	return false
}

func (a *App) startScheduledJob(schedule Schedule, now time.Time) {
	symbols, err := a.resolveSymbols(schedule.Symbols)
	if err != nil {
		a.updateScheduleRun(schedule.Name, now, "failed", err.Error(), "")
		return
	}
	models, err := a.resolveModels(schedule.Models)
	if err != nil {
		a.updateScheduleRun(schedule.Name, now, "failed", err.Error(), "")
		return
	}
	date := strings.TrimSpace(schedule.Date)
	if date == "" || strings.EqualFold(date, "today") {
		date = now.Format("2006-01-02")
	}
	job := newJob(a.tz, symbols, models)
	job.Message = "scheduled by " + schedule.Name
	a.mu.Lock()
	if a.job != nil && a.job.State == "running" {
		a.mu.Unlock()
		a.updateScheduleRun(schedule.Name, now, "skipped", "another report job was running", "")
		return
	}
	a.job = job
	a.mu.Unlock()
	a.updateScheduleRun(schedule.Name, now, "started", "job started", job.ID)
	go func() {
		a.runJob(job.ID, symbols, models, date)
		a.updateScheduleRun(schedule.Name, time.Now().In(a.tz), "finished", "job finished", job.ID)
	}()
}

func (a *App) loadSchedules() error {
	schedules, modTime, err := readSchedules(a.schedulePath())
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.schedules = schedules
	a.lastScheduleModTime = modTime
	a.mu.Unlock()
	return nil
}

func (a *App) reloadSchedulesIfChanged() error {
	info, err := os.Stat(a.schedulePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	a.mu.RLock()
	known := a.lastScheduleModTime
	a.mu.RUnlock()
	if !info.ModTime().After(known) {
		return nil
	}
	return a.loadSchedules()
}

func (a *App) saveSchedulesLocked() error {
	if err := writeSchedules(a.schedulePath(), a.schedules); err != nil {
		return err
	}
	if info, err := os.Stat(a.schedulePath()); err == nil {
		a.lastScheduleModTime = info.ModTime()
	}
	return nil
}

func (a *App) updateScheduleRun(name string, when time.Time, status, message, jobID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range a.schedules {
		if a.schedules[i].Name != name {
			continue
		}
		at := when.In(a.tz)
		a.schedules[i].LastRunAt = &at
		a.schedules[i].LastStatus = status
		a.schedules[i].LastMessage = message
		if jobID != "" {
			a.schedules[i].LastJobID = jobID
		}
		a.schedules[i].UpdatedAt = at
		if err := a.saveSchedulesLocked(); err != nil {
			a.log.Warn("could not save schedule status", "schedule", name, "err", err)
		}
		return
	}
}

func (a *App) schedulePath() string {
	return filepath.Join(a.cfg.OutputDir, "schedules.json")
}

func readSchedules(path string) ([]Schedule, time.Time, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, time.Time{}, nil
	}
	if err != nil {
		return nil, time.Time{}, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, time.Time{}, err
	}
	var payload struct {
		Schedules []Schedule `json:"schedules"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, time.Time{}, err
	}
	return payload.Schedules, info.ModTime(), nil
}

func writeSchedules(path string, schedules []Schedule) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	sort.Slice(schedules, func(i, j int) bool { return schedules[i].Name < schedules[j].Name })
	payload := struct {
		UpdatedAt time.Time  `json:"updated_at"`
		Schedules []Schedule `json:"schedules"`
	}{
		UpdatedAt: time.Now(),
		Schedules: schedules,
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func handleCronCLI(cfg Config, tz *time.Location, args []string) error {
	if len(args) == 0 {
		printCronUsage()
		return nil
	}
	path := filepath.Join(cfg.OutputDir, "schedules.json")
	switch args[0] {
	case "add":
		fs := flag.NewFlagSet("cron add", flag.ContinueOnError)
		symbolsRaw := fs.String("symbols", "", "comma separated symbols or all")
		modelsRaw := fs.String("models", "", "comma separated model names")
		timeRaw := fs.String("time", "", "HH:MM in configured timezone")
		daysRaw := fs.String("days", "mon,tue,wed,thu,fri", "comma separated days")
		dateRaw := fs.String("date", "today", "YYYY-MM-DD or today")
		enabled := fs.Bool("enabled", true, "enable the schedule")
		if len(args) < 2 {
			return errors.New("usage: go run . -config config.yaml cron add NAME --symbols INTC,TSM --models qwen --time 08:00")
		}
		name := strings.TrimSpace(args[1])
		if err := fs.Parse(args[2:]); err != nil {
			return err
		}
		schedule, err := buildSchedule(name, *symbolsRaw, *modelsRaw, *timeRaw, *daysRaw, *dateRaw, *enabled, tz)
		if err != nil {
			return err
		}
		schedules, _, err := readSchedules(path)
		if err != nil {
			return err
		}
		upsertSchedule(&schedules, schedule)
		if err := writeSchedules(path, schedules); err != nil {
			return err
		}
		fmt.Printf("saved schedule %s in %s\n", schedule.Name, path)
	case "list":
		return printSchedules(path)
	case "status":
		if err := printSchedules(path); err != nil {
			return err
		}
		return printServerStatus(cfg)
	case "remove":
		if len(args) != 2 {
			return errors.New("usage: go run . -config config.yaml cron remove NAME")
		}
		return removeSchedule(path, args[1])
	case "enable", "disable":
		if len(args) != 2 {
			return fmt.Errorf("usage: go run . -config config.yaml cron %s NAME", args[0])
		}
		return setScheduleEnabled(path, args[1], args[0] == "enable")
	case "run":
		if len(args) != 2 {
			return errors.New("usage: go run . -config config.yaml cron run NAME")
		}
		return postScheduleRun(cfg, path, args[1])
	case "abort":
		return postAbort(cfg)
	default:
		printCronUsage()
		return fmt.Errorf("unknown cron command %q", args[0])
	}
	return nil
}

func buildSchedule(name, symbolsRaw, modelsRaw, timeRaw, daysRaw, dateRaw string, enabled bool, tz *time.Location) (Schedule, error) {
	if strings.TrimSpace(name) == "" {
		return Schedule{}, errors.New("schedule name is required")
	}
	if _, err := time.ParseInLocation("15:04", strings.TrimSpace(timeRaw), tz); err != nil {
		return Schedule{}, errors.New("time must use HH:MM")
	}
	symbols := csvList(symbolsRaw)
	models := csvList(modelsRaw)
	if len(symbols) == 0 {
		return Schedule{}, errors.New("--symbols is required")
	}
	if len(models) == 0 {
		return Schedule{}, errors.New("--models is required")
	}
	if dateRaw != "" && !strings.EqualFold(dateRaw, "today") {
		if _, err := time.Parse("2006-01-02", dateRaw); err != nil {
			return Schedule{}, errors.New("--date must be YYYY-MM-DD or today")
		}
	}
	now := time.Now().In(tz)
	return Schedule{
		Name:      name,
		Symbols:   symbols,
		Models:    models,
		Date:      strings.TrimSpace(dateRaw),
		Time:      strings.TrimSpace(timeRaw),
		Days:      normalizeDays(daysRaw),
		Enabled:   enabled,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

func upsertSchedule(schedules *[]Schedule, schedule Schedule) {
	for i := range *schedules {
		if (*schedules)[i].Name == schedule.Name {
			schedule.CreatedAt = (*schedules)[i].CreatedAt
			schedule.LastRunAt = (*schedules)[i].LastRunAt
			schedule.LastStatus = (*schedules)[i].LastStatus
			schedule.LastMessage = (*schedules)[i].LastMessage
			schedule.LastJobID = (*schedules)[i].LastJobID
			(*schedules)[i] = schedule
			return
		}
	}
	*schedules = append(*schedules, schedule)
}

func printSchedules(path string) error {
	schedules, _, err := readSchedules(path)
	if err != nil {
		return err
	}
	if len(schedules) == 0 {
		fmt.Printf("no schedules in %s\n", path)
		return nil
	}
	sort.Slice(schedules, func(i, j int) bool { return schedules[i].Name < schedules[j].Name })
	for _, schedule := range schedules {
		last := "never"
		if schedule.LastRunAt != nil {
			last = schedule.LastRunAt.Format("2006-01-02 15:04:05 MST")
		}
		fmt.Printf("%s enabled=%t time=%s days=%s symbols=%s models=%s last=%s status=%s %s\n",
			schedule.Name,
			schedule.Enabled,
			schedule.Time,
			strings.Join(schedule.Days, ","),
			strings.Join(schedule.Symbols, ","),
			strings.Join(schedule.Models, ","),
			last,
			schedule.LastStatus,
			schedule.LastMessage,
		)
	}
	return nil
}

func removeSchedule(path, name string) error {
	schedules, _, err := readSchedules(path)
	if err != nil {
		return err
	}
	next := schedules[:0]
	removed := false
	for _, schedule := range schedules {
		if schedule.Name == name {
			removed = true
			continue
		}
		next = append(next, schedule)
	}
	if !removed {
		return fmt.Errorf("schedule %s was not found", name)
	}
	if err := writeSchedules(path, next); err != nil {
		return err
	}
	fmt.Printf("removed schedule %s\n", name)
	return nil
}

func setScheduleEnabled(path, name string, enabled bool) error {
	schedules, _, err := readSchedules(path)
	if err != nil {
		return err
	}
	for i := range schedules {
		if schedules[i].Name == name {
			schedules[i].Enabled = enabled
			schedules[i].UpdatedAt = time.Now()
			if err := writeSchedules(path, schedules); err != nil {
				return err
			}
			fmt.Printf("%s schedule %s\n", map[bool]string{true: "enabled", false: "disabled"}[enabled], name)
			return nil
		}
	}
	return fmt.Errorf("schedule %s was not found", name)
}

func printServerStatus(cfg Config) error {
	var status JobStatus
	if err := getJSON(localURL(cfg, "/api/status"), &status); err != nil {
		fmt.Printf("server status unavailable: %v\n", err)
		return nil
	}
	fmt.Printf("server job state=%s completed=%d failed=%d remaining=%d message=%s\n", status.State, status.Completed, status.Failed, status.Remaining, status.Message)
	return nil
}

func postScheduleRun(cfg Config, path, name string) error {
	schedules, _, err := readSchedules(path)
	if err != nil {
		return err
	}
	for _, schedule := range schedules {
		if schedule.Name != name {
			continue
		}
		req := GenerateRequest{Symbols: schedule.Symbols, Models: schedule.Models, Date: schedule.Date}
		if req.Date == "" || strings.EqualFold(req.Date, "today") {
			req.Date = time.Now().Format("2006-01-02")
		}
		raw, _ := json.Marshal(req)
		resp, err := http.Post(localURL(cfg, "/api/generate"), "application/json", bytes.NewReader(raw))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode >= 300 {
			return fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		fmt.Printf("started schedule %s: %s\n", name, strings.TrimSpace(string(body)))
		return nil
	}
	return fmt.Errorf("schedule %s was not found", name)
}

func postAbort(cfg Config) error {
	resp, err := http.Post(localURL(cfg, "/api/abort"), "application/json", strings.NewReader("{}"))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	fmt.Printf("abort requested: %s\n", strings.TrimSpace(string(body)))
	return nil
}

func getJSON(url string, target any) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(target)
}

func localURL(cfg Config, path string) string {
	return fmt.Sprintf("http://localhost:%d%s", cfg.ServerPort, path)
}

func csvList(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]bool{}
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, value)
	}
	return out
}

func normalizeDays(raw string) []string {
	if strings.EqualFold(strings.TrimSpace(raw), "all") {
		return []string{"sun", "mon", "tue", "wed", "thu", "fri", "sat"}
	}
	aliases := map[string]string{
		"sunday": "sun", "sun": "sun",
		"monday": "mon", "mon": "mon",
		"tuesday": "tue", "tue": "tue", "tues": "tue",
		"wednesday": "wed", "wed": "wed",
		"thursday": "thu", "thu": "thu", "thur": "thu", "thurs": "thu",
		"friday": "fri", "fri": "fri",
		"saturday": "sat", "sat": "sat",
	}
	var out []string
	for _, item := range csvList(raw) {
		value := strings.ToLower(item)
		if strings.Contains(value, "-") {
			out = append(out, expandDayRange(value, aliases)...)
			continue
		}
		if normalized, ok := aliases[value]; ok {
			out = append(out, normalized)
		}
	}
	if len(out) == 0 {
		return []string{"mon", "tue", "wed", "thu", "fri"}
	}
	seen := map[string]bool{}
	deduped := out[:0]
	for _, day := range out {
		if !seen[day] {
			seen[day] = true
			deduped = append(deduped, day)
		}
	}
	return deduped
}

func expandDayRange(raw string, aliases map[string]string) []string {
	order := []string{"sun", "mon", "tue", "wed", "thu", "fri", "sat"}
	parts := strings.SplitN(raw, "-", 2)
	start, startOK := aliases[strings.TrimSpace(parts[0])]
	end, endOK := aliases[strings.TrimSpace(parts[1])]
	if !startOK || !endOK {
		return nil
	}
	startIdx, endIdx := -1, -1
	for i, day := range order {
		if day == start {
			startIdx = i
		}
		if day == end {
			endIdx = i
		}
	}
	if startIdx == -1 || endIdx == -1 {
		return nil
	}
	var out []string
	for {
		out = append(out, order[startIdx])
		if startIdx == endIdx {
			break
		}
		startIdx = (startIdx + 1) % len(order)
	}
	return out
}

func printCronUsage() {
	fmt.Println("cron commands:")
	fmt.Println("  cron add NAME --symbols INTC,TSM|all --models qwen,gemini --time 08:00 [--days mon-fri] [--date today]")
	fmt.Println("  cron list")
	fmt.Println("  cron status")
	fmt.Println("  cron run NAME")
	fmt.Println("  cron enable NAME")
	fmt.Println("  cron disable NAME")
	fmt.Println("  cron remove NAME")
	fmt.Println("  cron abort")
}

func idleJob(tz *time.Location) *JobStatus {
	now := time.Now().In(tz)
	return &JobStatus{ID: "idle", State: "idle", Message: "ready", StartedAt: now, UpdatedAt: now}
}

func newJob(tz *time.Location, symbols, models []string) *JobStatus {
	now := time.Now().In(tz)
	items := make([]JobItem, 0, len(symbols)*len(models))
	for _, symbol := range symbols {
		for _, model := range models {
			items = append(items, JobItem{Symbol: symbol, Model: model, Status: "queued"})
		}
	}
	return &JobStatus{
		ID:        now.Format("20060102-150405"),
		State:     "running",
		Total:     len(items),
		Remaining: len(items),
		Items:     items,
		Message:   "queued",
		StartedAt: now,
		UpdatedAt: now,
	}
}

func (a *App) startItem(jobID string, item JobItem) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.job == nil || a.job.ID != jobID {
		return
	}
	a.job.Current = &item
	a.job.Message = fmt.Sprintf("generating %s with %s", item.Symbol, item.Model)
	a.job.UpdatedAt = time.Now().In(a.tz)
	for i := range a.job.Items {
		if a.job.Items[i].Symbol == item.Symbol && a.job.Items[i].Model == item.Model && a.job.Items[i].Status == "queued" {
			a.job.Items[i] = item
			break
		}
	}
}

func (a *App) completeItem(jobID string, item JobItem, record *ReportRecord) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.job == nil || a.job.ID != jobID {
		return
	}
	if item.Status == "done" {
		a.job.Completed++
	} else {
		a.job.Failed++
	}
	a.job.Remaining = max(0, a.job.Total-a.job.Completed-a.job.Failed)
	a.job.Current = nil
	a.job.Message = item.Message
	a.job.UpdatedAt = time.Now().In(a.tz)
	for i := range a.job.Items {
		if a.job.Items[i].Symbol == item.Symbol && a.job.Items[i].Model == item.Model && a.job.Items[i].Status == "running" {
			a.job.Items[i] = item
			break
		}
	}
}

func (a *App) finishJob(jobID, state, message string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.job == nil || a.job.ID != jobID {
		return
	}
	now := time.Now().In(a.tz)
	a.job.State = state
	a.job.Message = message
	a.job.Current = nil
	a.job.UpdatedAt = now
	a.job.FinishedAt = &now
}

func symbolsWithAll(symbols []string) []string {
	out := append([]string(nil), symbols...)
	sort.Strings(out)
	return out
}

func sortReports(reports []ReportRecord, sortBy string) {
	switch sortBy {
	case "updated", "generated":
		sort.Slice(reports, func(i, j int) bool {
			return reports[i].GeneratedAt.After(reports[j].GeneratedAt)
		})
	case "model":
		sort.Slice(reports, func(i, j int) bool {
			if reports[i].Model == reports[j].Model {
				return reports[i].Ticker < reports[j].Ticker
			}
			return reports[i].Model < reports[j].Model
		})
	default:
		sort.Slice(reports, func(i, j int) bool {
			if reports[i].Ticker == reports[j].Ticker {
				if reports[i].Model == reports[j].Model {
					return reports[i].GeneratedAt.After(reports[j].GeneratedAt)
				}
				return reports[i].Model < reports[j].Model
			}
			return reports[i].Ticker < reports[j].Ticker
		})
	}
}

func tailString(s string, limit int) string {
	s = strings.TrimSpace(s)
	if len(s) <= limit {
		return s
	}
	return s[len(s)-limit:]
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func newLogger(level string) *slog.Logger {
	lvl := new(slog.LevelVar)
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		lvl.Set(slog.LevelDebug)
	case "warn":
		lvl.Set(slog.LevelWarn)
	case "error":
		lvl.Set(slog.LevelError)
	default:
		lvl.Set(slog.LevelInfo)
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
