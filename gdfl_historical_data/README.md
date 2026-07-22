# GDFL — NIFTY & SENSEX Historical Data

Downloads NIFTY and SENSEX index/options history from the
[GDFL (Global Financial Datafeeds)](https://globaldatafeeds.in/) Nimble websocket
feed, via the `GetHistory` API.

Two output shapes are supported:

- **TICK CSVs** — one file per contract, replicating the GFDL daily-dump layout.
- **1-minute DuckDB** — spot and options bars in queryable tables.

## Credentials

Two separate API keys, each tied to its own exchanges. **Do not mix them.**

| Key | Exchanges | Products |
|---|---|---|
| `GDFL_NSE_API_KEY` | `NFO`, `NSE_IDX` | NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY |
| `GDFL_BSE_API_KEY` | `BFO`, `BSE_IDX` | SENSEX, BANKEX |

Each key is **single-session** — only one websocket at a time per key. The NSE and
BSE keys are independent sessions, so an NSE script and a BSE script can run
concurrently, but never run two scripts that use the same key.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env      # then replace the ****** with your real keys
```

[gdfl_config.py](gdfl_config.py) reads `.env` from its own directory, so the
scripts work no matter which directory you run them from. A key that isn't set
fails immediately with a clear message instead of an opaque auth error later.

## Step 1 — download the instrument masters

Everything else reads these. Run once, and again whenever new contracts list:

```bash
python download_nse_master.py     # -> gdfl_master_nse.csv
python download_bse_master.py     # -> gdfl_master_bse.csv
```

## Step 2 — pick a downloader

### Tick data (one CSV per contract)

```bash
python gdfl_daily_download.py [YYYY-MM-DD]        # NIFTY / NFO
python gdfl_daily_download_bse.py [YYYY-MM-DD]    # SENSEX / BFO
```

Defaults to today. Pulls the full session (09:15–15:30 IST) for every futures and
options contract of that underlying, writing the GFDL dump layout:

```
JUL_2026/GFDLNFO_TICK_06072026/GFDLNFO_TICK_06072026/
    NIFTY-I.NFO.csv                 <- near-month future
    NIFTY-II.NFO.csv                <- next-month future
    NIFTY04AUG2622100PE.NFO.csv     <- one file per option
```

Columns: `Ticker, Date, Time, LTP, BuyPrice, BuyQty, SellPrice, SellQty, LTQ, OpenInterest`

These runs are long and rate-limited. Two things make them restartable:

- A `_processed.log` manifest in the output directory records every contract that
  finished, including ones that legitimately returned no ticks. Re-running skips them.
- On a per-hour quota error the script sleeps `RATE_WAIT` (600s), reconnects, and
  retries, up to `RATE_RETRIES` (12) cycles.

Contracts with no ticks produce no file, so only contracts that actually traded appear.

### 1-minute bars (DuckDB)

| Script | Index | Expiries | Strike range | Output |
|---|---|---|---|---|
| [nifty_near_week_duckdb.py](nifty_near_week_duckdb.py) | NIFTY | nearest weekly | — | `nifty_nearest_week.duckdb` |
| [sensex_gdfl_newar week_duck_db.py](sensex_gdfl_newar%20week_duck_db.py) | SENSEX | nearest weekly | — | `sensex_data_nearest_week_gdfl.duckdb` |
| [nifty_all_expiry_duckdb.py](nifty_all_expiry_duckdb.py) | NIFTY | all available | spot high/low ±1000 | `nifty_all_expiry_gdfl.duckdb` |
| [sensex_all_expiry_duckdb.py](sensex_all_expiry_duckdb.py) | SENSEX | all available | spot high/low ±3000 | `sensex_all_expiry_gdfl.duckdb` |

Each writes two tables, `spot_data` and `options_data`. The strike range is computed
after spot data is fetched, so it tracks the day's actual high and low.

The near-week scripts prompt for the trading day:

```
Fetch data for: [1] today  [2] historical date  (default 1):
```

Spot uses the index exchanges (`NSE_IDX` / `BSE_IDX`). If your key doesn't have those
enabled, the script warns and continues with options only.

## Probes

Small diagnostic scripts for when the feed misbehaves — they print raw API replies
rather than writing anything:

- [probe_gethistory.py](probe_gethistory.py) — dump the raw `GetHistory` reply for one contract.
- [probe_tick_coverage.py](probe_tick_coverage.py) — check which part of a session actually has ticks.

## Notes

- `.env`, `*.duckdb` and `*.csv` are git-ignored — that covers the generated master
  CSVs and all tick output, so only code is committed.
- `gdfl_master_nse.csv` header labels are offset from the data: the underlying index
  is in the `Product` column and `FUTIDX`/`OPTIDX` in the `Name` column. The loaders
  account for this.
- One filename carries a typo from the original — `sensex_gdfl_newar week_duck_db.py`
  ("newar", plus a space). Left as-is so existing shortcuts and cron entries keep
  working; the space means it must be quoted when run.
