"""
GDFL daily TICK downloader — NIFTY index futures & options.

For a given trading day it pulls full-session (09:15 -> 15:30) TICK data for
every NIFTY futures & options contract listed in gdfl_master_nse.csv and writes
one CSV per contract, replicating the GFDL daily-dump layout:

    <MON_YEAR>/GFDLNFO_TICK_<DDMMYYYY>/GFDLNFO_TICK_<DDMMYYYY>/
        NIFTY-I.NFO.csv          <- near-month future
        NIFTY-II.NFO.csv         <- next-month future
        NIFTY-III.NFO.csv        <- far-month future
        NIFTY04AUG2622100PE.NFO.csv   <- one file per option (TradeSymbol)
        ...

CSV columns: Ticker,Date,Time,LTP,BuyPrice,BuyQty,SellPrice,SellQty,LTQ,OpenInterest

Notes
-----
* The GDFL NSE key is single-session — this script uses ONE websocket and
  never runs two probes at once. Do not run a second downloader in parallel.
* Contracts that return no ticks (far-dated / non-trading) are skipped, so only
  contracts that actually traded produce a file.
"""

import asyncio
import csv
import json
import os
import sys
from datetime import datetime, timezone, timedelta

import websockets

from gdfl_config import NSE_API_KEY as API_KEY, WS_ENDPOINT as WS_URL

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
IST = timezone(timedelta(hours=5, minutes=30))
HERE = os.path.dirname(os.path.abspath(__file__))
MASTER_CSV = os.path.join(HERE, "gdfl_master_nse.csv")

# Trading day to download. Defaults to today; override with:  python gdfl_daily_download.py 2026-07-06
RUN_DATE = datetime.now(IST).date()

SESSION_START = (9, 15, 0)      # 09:15:00 IST
SESSION_END   = (15, 30, 0)     # 15:30:00 IST

EXCHANGE   = "NFO"
UNDERLYING = "NIFTY"            # exact underlying (excludes NIFTYNXT50, BANKNIFTY, ...)
MAX_TICKS  = 1_000_000         # per-request cap (a full day is ~12-14k ticks)
REQ_TIMEOUT = 40.0             # seconds to wait for a GetHistory reply
FUT_LABELS = ["I", "II", "III", "IV", "V"]   # continuous-future rank names

RATE_WAIT   = 600             # seconds to cool down when GDFL says "calls per hour limited"
RATE_RETRIES = 12             # how many cooldown cycles before giving up on a contract
RESUME      = True            # skip contracts whose output CSV already exists

CSV_HEADER = ["Ticker", "Date", "Time", "LTP", "BuyPrice", "BuyQty",
              "SellPrice", "SellQty", "LTQ", "OpenInterest"]
# server-pushed control / keepalive messages we ignore
SKIP = {"AllowVMRunningResult", "AllowServerOSRunningResult", "ServerPingResult", "Echo"}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def epoch(dt: datetime) -> int:
    """Interpret a naive datetime as IST and return a unix timestamp."""
    return int(dt.replace(tzinfo=IST).timestamp())


def fmt(v):
    """Format a number like the GFDL dump: drop a trailing '.0', keep decimals."""
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def parse_expiry(s: str) -> datetime:
    return datetime.strptime(s, "%d%b%Y")


def load_instruments():
    """Return (futures, options) lists of dicts from the master CSV.

    NOTE: the master's header labels are offset from the data — the underlying
    index lives in the 'Product' column and FUTIDX/OPTIDX in the 'Name' column.
    """
    futures, options = [], []
    with open(MASTER_CSV, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("Product") != UNDERLYING:
                continue
            kind = row.get("Name")
            if kind == "FUTIDX":
                futures.append(row)
            elif kind == "OPTIDX":
                options.append(row)

    # futures sorted near -> far, tagged with continuous label (I/II/III)
    futures.sort(key=lambda r: parse_expiry(r["Expiry"]))
    fut_jobs = []
    for i, r in enumerate(futures):
        label = FUT_LABELS[i] if i < len(FUT_LABELS) else str(i + 1)
        fut_jobs.append({"identifier": r["Identifier"], "ticker": f"{UNDERLYING}-{label}"})

    # options keyed / named by TradeSymbol (e.g. NIFTY04AUG2622100PE)
    opt_jobs = [{"identifier": r["Identifier"], "ticker": r["TradeSymbol"]} for r in options]
    return fut_jobs, opt_jobs


def out_dir_for(day) -> str:
    mon_year = day.strftime("%b_%Y").upper()          # JUL_2026
    ddmmyyyy = day.strftime("%d%m%Y")                 # 06072026
    folder = f"GFDLNFO_TICK_{ddmmyyyy}"
    return os.path.join(HERE, mon_year, folder, folder)


def write_csv(out_dir, ticker, rows, day):
    """Write one contract's ticks to <ticker>.NFO.csv (sorted oldest->newest)."""
    rows.sort(key=lambda r: r["LastTradeTime"])
    date_str = day.strftime("%d/%m/%Y")
    path = os.path.join(out_dir, f"{ticker}.NFO.csv")
    tkr = f"{ticker}.NFO"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(CSV_HEADER)
        for r in rows:
            t = datetime.fromtimestamp(r["LastTradeTime"], IST)
            w.writerow([
                tkr, date_str, t.strftime("%H:%M:%S"),
                fmt(r.get("LastTradePrice")),
                fmt(r.get("BuyPrice")), fmt(r.get("BuyQty")),
                fmt(r.get("SellPrice")), fmt(r.get("SellQty")),
                fmt(r.get("TradedQty")), fmt(r.get("OpenInterest")),
            ])
    return path


# --------------------------------------------------------------------------- #
# Websocket
# --------------------------------------------------------------------------- #
class SessionBusy(Exception):
    """Server refused because the single-session key is momentarily in use."""


class RateLimited(Exception):
    """Server refused because the per-hour call quota is exhausted."""


async def connect_and_auth():
    uri = f"{WS_URL}?api_key={API_KEY}"
    ws = await websockets.connect(uri, ping_interval=None, max_size=200 * 1024 * 1024)
    await ws.send(json.dumps({"MessageType": "Authenticate", "Password": API_KEY}))
    while True:
        data = json.loads(await ws.recv())
        if data.get("MessageType") == "AuthenticateResult":
            if data.get("Complete") in (True, "true"):
                return ws
            await ws.close()
            raise RuntimeError(f"Authentication failed: {data}")


async def fetch_ticks(ws, identifier, frm_ts, to_ts):
    """Send one GetHistory TICK request; return list of tick rows (may be [])."""
    req = {
        "MessageType": "GetHistory", "Exchange": EXCHANGE,
        "InstrumentIdentifier": identifier, "Periodicity": "TICK",
        "From": frm_ts, "To": to_ts, "Max": MAX_TICKS, "isShortIdentifier": False,
    }
    await ws.send(json.dumps(req))
    while True:
        data = json.loads(await asyncio.wait_for(ws.recv(), timeout=REQ_TIMEOUT))
        mt = data.get("MessageType", "")
        if mt in SKIP:
            continue
        if mt == "HistoryTickResult":
            return data.get("Result", []) or []
        msg = data.get("Message", "") or ""
        low = msg.lower()
        if "already in use" in low or "access denied" in low:
            raise SessionBusy(msg)
        if "calls per hour" in low or "limit" in low:
            raise RateLimited(msg)
        # RequestError or anything else is terminal for this instrument
        raise RuntimeError(f"{mt}: {msg}")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
async def run(day):
    frm = datetime(day.year, day.month, day.day, *SESSION_START)
    to  = datetime(day.year, day.month, day.day, *SESSION_END)
    frm_ts, to_ts = epoch(frm), epoch(to)

    fut_jobs, opt_jobs = load_instruments()
    jobs = fut_jobs + opt_jobs
    out_dir = out_dir_for(day)
    os.makedirs(out_dir, exist_ok=True)

    # manifest of contracts already processed this day (incl. confirmed-empty),
    # so a re-run after a rate-limit only retries the ones that never completed.
    manifest_path = os.path.join(out_dir, "_processed.log")
    done = set()
    if RESUME and os.path.exists(manifest_path):
        with open(manifest_path) as mf:
            done = {ln.strip() for ln in mf if ln.strip()}

    print(f"Trading day : {day:%Y-%m-%d}  session {frm:%H:%M}->{to:%H:%M} IST")
    print(f"Output dir  : {out_dir}")
    print(f"Contracts   : {len(fut_jobs)} futures + {len(opt_jobs)} options = {len(jobs)}")
    print(f"Already done: {len(done)} (from manifest)\n")

    ws = await connect_and_auth()
    written = empty = errors = resumed = rate_pauses = 0
    mf = open(manifest_path, "a")     # append processed tickers as we go
    try:
        MAX_ATTEMPTS = 6
        for n, job in enumerate(jobs, 1):
            ident, ticker = job["identifier"], job["ticker"]

            # resume: skip anything already processed (written OR confirmed empty)
            if RESUME and (ticker in done or
                           os.path.exists(os.path.join(out_dir, f"{ticker}.NFO.csv"))):
                resumed += 1
                continue

            tag = "ERROR unhandled"
            rate_tries = 0
            attempt = 0
            while True:
                attempt += 1
                try:
                    rows = await fetch_ticks(ws, ident, frm_ts, to_ts)
                    if rows:
                        write_csv(out_dir, ticker, rows, day)
                        written += 1
                        tag = f"OK  {len(rows):>6} ticks"
                    else:
                        empty += 1
                        tag = "empty"
                    mf.write(ticker + "\n")     # mark processed (written or empty)
                    mf.flush()
                    break
                except RateLimited:
                    rate_tries += 1
                    if rate_tries > RATE_RETRIES:
                        errors += 1
                        tag = "ERROR RateLimited (gave up)"
                        break
                    rate_pauses += 1
                    print(f"  … per-hour limit reached at #{n} ({ticker}); "
                          f"cooling down {RATE_WAIT}s [{rate_tries}/{RATE_RETRIES}]")
                    try:
                        await ws.close()
                    except Exception:
                        pass
                    await asyncio.sleep(RATE_WAIT)
                    ws = await connect_and_auth()
                    continue
                except (websockets.ConnectionClosed, asyncio.TimeoutError, SessionBusy) as e:
                    if attempt < MAX_ATTEMPTS:
                        wait = min(3 * attempt, 15)       # 3,6,9,12,15s backoff
                        print(f"  ! {type(e).__name__} on {ticker} "
                              f"(attempt {attempt}) — waiting {wait}s & reconnecting")
                        try:
                            await ws.close()
                        except Exception:
                            pass
                        await asyncio.sleep(wait)
                        ws = await connect_and_auth()
                        continue
                    errors += 1
                    tag = f"ERROR {type(e).__name__}"
                    break
                except RuntimeError as e:
                    errors += 1
                    tag = f"ERROR {e}"
                    break
            if n % 50 == 0 or (job in fut_jobs):
                print(f"  [{n:>4}/{len(jobs)}] {ticker:<26} {tag}")
    finally:
        mf.close()
        await ws.close()

    print(f"\nDone. files written: {written}   empty: {empty}   "
          f"resumed(existing): {resumed}   errors: {errors}   rate-pauses: {rate_pauses}")
    print(f"Saved under: {out_dir}")


def main():
    global RUN_DATE
    if len(sys.argv) > 1:
        RUN_DATE = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    asyncio.run(run(RUN_DATE))


if __name__ == "__main__":
    main()
