"""
NIFTY spot 1-second downloader (ICICI Direct Breeze API -> DuckDB + CSV).

Spot only, no options. Same credential/session setup as nifty_icici_duck.py
(see icici_config.py and README.md). All the fetching/chunking/storage logic
lives in spot_1sec_core.py; this file only pins the index.

Spot comes from stock_code="NIFTY" on exchange_code="NSE" with
product_type="cash", matching fetch_spot_daily_data() in nifty_icici_duck.py.

Usage:
    python nifty_spot_1sec.py 2026-07-01 2026-07-29
    python nifty_spot_1sec.py 2026-07-29                 # single day
    python nifty_spot_1sec.py                            # prompts for dates

Options:
    --db PATH            DuckDB file (default $NIFTY_1SEC_DB_PATH or nifty_spot_1sec.duckdb)
    --csv-dir DIR        Per-day CSV output directory (default spot_1sec_csv)
    --csv-combined PATH  Also write one merged CSV for the whole range
    --no-csv             Skip CSV output, DuckDB only
    --chunk-seconds N    Window size per API call (default 900, must stay under 1000)
    --workers N          Parallel windows per day (default 4)
    --force              Re-download days that already have rows
    --include-weekends   Also attempt Sat/Sun (normally skipped without an API call)
"""

import sys

from spot_1sec_core import IndexProfile, main

NIFTY = IndexProfile(
    label="NIFTY 50",
    stock_code="NIFTY",
    exchange="NSE",
    db_env="NIFTY_1SEC_DB_PATH",
    db_default="nifty_spot_1sec.duckdb",
    csv_env="NIFTY_1SEC_CSV_DIR",
    csv_default="nifty_spot_1sec_csv",
)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:], NIFTY))


# Querying afterwards:
#
#   import duckdb
#   db = duckdb.connect('nifty_spot_1sec.duckdb')
#
#   # every second of one day
#   db.execute("SELECT * FROM spot_1sec WHERE date = '2026-07-29' "
#              "ORDER BY datetime").fetchdf()
#
#   # resample 1-second -> 1-minute bars
#   db.execute("""
#       SELECT date_trunc('minute', datetime) AS minute,
#              first(open  ORDER BY datetime) AS open,
#              max(high)                      AS high,
#              min(low)                       AS low,
#              last(close ORDER BY datetime)  AS close,
#              sum(volume)                    AS volume
#       FROM spot_1sec
#       WHERE date = '2026-07-29'
#       GROUP BY 1 ORDER BY 1
#   """).fetchdf()
#
#   # coverage check — seconds captured per day
#   db.execute("SELECT date, COUNT(*) AS ticks FROM spot_1sec "
#              "GROUP BY date ORDER BY date").fetchdf()
