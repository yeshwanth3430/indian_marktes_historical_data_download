# Indian Markets — Historical Data Download

Scripts for downloading historical NIFTY and SENSEX market data from Indian broker
and data-vendor APIs into local DuckDB databases for backtesting.

## Sources

| Folder | Source | Data |
|---|---|---|
| [icici_historical_data/](icici_historical_data/) | ICICI Direct Breeze API | 1-minute NIFTY & SENSEX spot and options bars |

See each folder's README for setup and usage.

## Credentials

No API keys live in this repo. Each source folder ships a `.env.example` — copy it to
`.env`, fill in your own credentials, and keep it out of commits (`.env` is git-ignored).
