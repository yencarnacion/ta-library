# TA Library

Go web app for generating and organizing TradingAgents reports from a CSV watchlist.

## Run

```sh
cp .env.example .env
./go.sh
```

Open:

```text
http://localhost:9090
```

The port, CSV path, output directory, TradingAgents checkout, and model scripts are configured in `config.yaml`.

GPT-5.6 Luna runs through TradingAgents' `go-luna.sh`, which sets the OpenAI provider,
`gpt-5.6-luna` for both thinking modes, and maximum reasoning effort. Luna is selected
by default on a new page load; check any of the other enabled models to run them alongside
it. Set a model's `default: true` value in `config.yaml` to change the initial selection.

The single ticker input can run a report for any ticker-like symbol. It does not add that ticker to `top-100.csv`; the CSV only controls the selectable company list and `all` runs.

## Reports

Reports are generated serially and copied into:

```text
output/<TICKER>/<model>/<run-id>/
```

Each copied report includes `report.html` and `ta-library-report.json`, which record the model, ticker, New York generation timestamp, weekday, analysis date, and original TradingAgents output directory.
