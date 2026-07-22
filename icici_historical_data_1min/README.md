# ICICI Direct — NIFTY & SENSEX Intraday Data Downloader

Downloads 1-minute intraday **spot and options** data for NIFTY and SENSEX from the
[ICICI Direct Breeze API](https://api.icicidirect.com/apiuser/home) and stores it in
local DuckDB databases for backtesting.

For each trading day in a date range, the scripts:

1. Pick the **nearest expiry** on or after that day (from a hardcoded expiry list).
2. Fetch **1-minute spot bars** for the full session (09:15–15:30) and derive the day's high/low.
3. Build a **strike range** around that high/low, then fetch 1-minute CE and PE bars
   for every strike in parallel (5 worker threads).
4. Write everything to DuckDB with `INSERT OR REPLACE`, so re-running a day is safe.

## Scripts

| Script | Index | Spot | Options | Strike range | Interval | Output DB |
|---|---|---|---|---|---|---|
| [nifty_icici_1min.py](nifty_icici_1min.py) | NIFTY 50 | `NIFTY` / NSE | `NIFTY` / NFO | high +600, low −600 | 50 | `nifty_data.duckdb` |
| [sensex_icici_1min.py](sensex_icici_1min.py) | SENSEX | `BSESEN` / BSE | `BSESEN` / BFO | high +1500, low −1500 | 100 | `sensex_data.duckdb` |

[icici_config.py](icici_config.py) holds the shared credential loading and session setup.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Then create your `.env`:

```bash
cp .env.example .env
```

Replace the `******` placeholders with three real values:

| Variable | Where to get it |
|---|---|
| `BREEZE_API_KEY` | Breeze **App details** page |
| `BREEZE_API_SECRET` | Breeze **App details** page |
| `BREEZE_SESSION_TOKEN` | Changes daily — see below |

### Refreshing the session token

The session token expires every day. To mint a new one, print the login URL:

```bash
python -c "import icici_config; print(icici_config.login_url())"
```

Open it, log in, and the browser is redirected to a URL containing `API_Session=12345678`.
Copy that value into `BREEZE_SESSION_TOKEN` in `.env`.

That URL embeds your API key, which is why it is printed on demand rather than on
every run — don't paste it into a shared log. Normal startup only ever shows masked
credentials:

```
Breeze API key    : abcd******wxyz
Breeze session    : ********
```

## Usage

```bash
python nifty_icici_1min.py
# or
python sensex_icici_1min.py
```

Each script prompts for a date range:

```
Enter start date (YYYY-MM-DD): 2025-01-01
Enter end date (YYYY-MM-DD): 2025-01-31
```

Every calendar day in the range is attempted, including weekends and holidays —
non-trading days simply return no data and are skipped. There is a 10-second pause
between days to stay within API rate limits.

To write somewhere other than the default, set `NIFTY_DB_PATH` / `SENSEX_DB_PATH`.

## Schema

Both databases use the same two tables.

**`spot_data`**

| Column | Type | |
|---|---|---|
| `date` | DATE | part of primary key |
| `datetime` | TIMESTAMP | part of primary key |
| `open`, `high`, `low`, `close` | DOUBLE | |
| `volume` | BIGINT | |

**`options_data`**

| Column | Type | |
|---|---|---|
| `date` | DATE | part of primary key |
| `datetime` | TIMESTAMP | part of primary key |
| `strike_price` | INTEGER | part of primary key |
| `option_type` | VARCHAR | part of primary key — `CALL` or `PUT` |
| `expiry_date` | DATE | |
| `open`, `high`, `low`, `close` | DOUBLE | |
| `volume` | BIGINT | |
| `open_interest` | BIGINT | |

### Querying

```python
import duckdb

db = duckdb.connect('nifty_data.duckdb')

# All spot bars for one day
db.execute("SELECT * FROM spot_data WHERE date = '2025-01-10'").fetchdf()

# One strike's calls
db.execute("""
    SELECT * FROM options_data
    WHERE strike_price = 23000 AND option_type = 'CALL'
""").fetchdf()

# Daily spot high/low
db.execute("""
    SELECT date, MAX(high) AS daily_high, MIN(low) AS daily_low
    FROM spot_data GROUP BY date ORDER BY date
""").fetchdf()
```

## Expiry lists

Expiry dates are hardcoded near the bottom of each script, in `dd-mm-yyyy` format:

- **NIFTY** — weekly expiries, 2022-01-06 through 2026-07-21
- **SENSEX** — weekly expiries, 2025-01-03 through 2026-06-25

`get_nearest_expiry()` picks the first expiry on or after the target date, and rolls
back to the previous weekday if that date lands on a weekend. **To download dates
beyond the ranges above, add the new expiries to the list first** — otherwise the day
is skipped with "Could not find a valid expiry date".

## Notes

- `.env`, `*.duckdb` and `*.csv` are git-ignored. Keep your credentials out of commits.
- Options fetching runs 5 threads in parallel. Raising `max_workers` risks API throttling.
- Strikes with no traded data are logged and skipped rather than failing the day.
