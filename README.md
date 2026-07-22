# Indian Markets — Historical Data Download

Scripts for downloading historical NIFTY and SENSEX market data from Indian broker
and data-vendor APIs into local DuckDB databases and CSVs for backtesting.

## Sources

| Folder | Source | Data |
|---|---|---|
| [icici_historical_data_1min/](icici_historical_data_1min/) | ICICI Direct Breeze API | 1-minute NIFTY & SENSEX spot and options bars → DuckDB |
| [gdfl_historical_data_1min/](gdfl_historical_data_1min/) | GDFL Nimble websocket feed | Full-session tick data → CSV, and 1-minute bars → DuckDB |

Each folder is self-contained — its own README, `requirements.txt` and `.env`.
Start with the folder README for setup and usage.

## Credentials

No API keys live in this repo. Each source folder ships a `.env.example` — copy it to
`.env`, replace the `******` with your own credentials, and keep it out of commits
(`.env` is git-ignored).

| Folder | Variables |
|---|---|
| `icici_historical_data_1min/` | `BREEZE_API_KEY`, `BREEZE_API_SECRET`, `BREEZE_SESSION_TOKEN` |
| `gdfl_historical_data_1min/` | `GDFL_NSE_API_KEY`, `GDFL_BSE_API_KEY` |

Both config modules load `.env` from their own directory, so scripts run correctly
from any working directory, and they log masked values only — a credential is never
printed in full.

## Output data

Generated data stays local. `*.duckdb` and `*.csv` are git-ignored, which covers the
GDFL instrument masters, all tick CSVs, and every DuckDB database.
