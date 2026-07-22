"""
Fetch intraday 1-minute historical data from GDFL for ALL available NIFTY expiries.
Saves to nifty_all_expiry_gdfl.duckdb:
  - spot_data:    NIFTY 50 index 1-min bars
  - options_data: NIFTY options 1-min bars (all future expiries, dynamic strike range)

Strike range: spot high/low ± 1000 (computed after spot data is downloaded).
Time window: today 09:15–15:30 IST.
"""

import asyncio
import csv
import json
import os
import duckdb
import pandas as pd
from datetime import datetime, time, date, timedelta, timezone
from typing import Any, Dict, List, Optional, Set
import websockets

from gdfl_config import NSE_API_KEY as API_KEY, WS_ENDPOINT


def _resolve_trade_date() -> date:
    """Ask the user which day to fetch.

        [1] today (default)
        [2] a single historical day (prompts for YYYY-MM-DD)
    """
    try:
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo("Asia/Kolkata")).date()
    except Exception:
        today = datetime.now().date()

    choice = input("Fetch data for: [1] today  [2] historical date  (default 1): ").strip() or "1"
    if choice == "2":
        while True:
            ds = input("Enter date (YYYY-MM-DD): ").strip()
            try:
                return datetime.strptime(ds, "%Y-%m-%d").date()
            except ValueError:
                print(f"  Invalid date '{ds}' — expected YYYY-MM-DD, try again.")
    return today

SPOT_EXCHANGE    = "NSE_IDX"
OPTIONS_EXCHANGE = "NFO"
SPOT_SYMBOL      = "NIFTY 50"
STRIKE_PADDING   = 1000          # ± points around day high/low
DB_PATH          = "nifty_all_expiry_gdfl.duckdb"
MASTER_CSV       = "gdfl_master_nse.csv"


# ---------------------------------------------------------------------------
# Master CSV helpers
# ---------------------------------------------------------------------------

def load_master_from_csv(csv_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(csv_path):
        print(f"✗ Master file not found: {csv_path} — run download_nse_master.py first")
        return []
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            instruments = list(csv.DictReader(f))
        print(f"✓ Loaded {len(instruments)} instruments from {csv_path}")
        return instruments
    except Exception as e:
        print(f"⚠️  Error reading {csv_path}: {e}")
        return []


def filter_nifty_all_expiry_symbols(
    instruments: List[Dict[str, Any]],
    min_strikes: int = 100,
    ref_date: Optional[date] = None,
) -> List[str]:
    """Return ALL NIFTY options across every future expiry (no strike filter yet).

    Expiries with fewer than `min_strikes` OPTIDX strikes are skipped: the
    master carries thin, long-dated series (e.g. far Thursdays / year-end
    contracts with only a handful of strikes) that are not active weekly/monthly
    expiries and have no real intraday data.
    """
    today = ref_date or datetime.now().date()

    # Count OPTIDX strikes per future expiry so we can skip sparse series.
    strike_counts: Dict[str, int] = {}
    for inst in instruments:
        if inst.get("Product", "").strip() != "NIFTY":
            continue
        if inst.get("Name", "").strip() != "OPTIDX":
            continue
        expiry_str = inst.get("Expiry", "").strip()
        try:
            if datetime.strptime(expiry_str, "%d%b%Y").date() < today:
                continue
        except ValueError:
            continue
        strike_counts[expiry_str] = strike_counts.get(expiry_str, 0) + 1

    active_expiries = {e for e, c in strike_counts.items() if c >= min_strikes}

    symbols: List[str] = []
    expiries_seen: Set[str] = set()

    for inst in instruments:
        if inst.get("Product", "").strip() != "NIFTY":
            continue
        if inst.get("Name", "").strip() != "OPTIDX":
            continue
        expiry_str = inst.get("Expiry", "").strip()
        identifier  = inst.get("Identifier", "").strip()
        if not expiry_str or not identifier:
            continue
        if expiry_str not in active_expiries:
            continue          # skip expired or thin (non-active) expiries
        if "_CE_" in identifier or "_PE_" in identifier:
            symbols.append(identifier)
            expiries_seen.add(expiry_str)

    sorted_exp = sorted(expiries_seen)
    skipped = sorted(e for e in strike_counts if e not in active_expiries)
    print(f"✓ Loaded {len(symbols)} NIFTY options across {len(expiries_seen)} expiries")
    print(f"  Expiries: {sorted_exp}")
    if skipped:
        print(f"  Skipped {len(skipped)} thin expiries (<{min_strikes} strikes): {skipped}")
    return symbols


def _filter_symbols_by_strike(
    symbols: List[str], strike_min: float, strike_max: float
) -> List[str]:
    result = []
    for sym in symbols:
        try:
            if "_CE_" in sym:
                strike = float(sym.split("_CE_")[1].split("_")[0])
            elif "_PE_" in sym:
                strike = float(sym.split("_PE_")[1].split("_")[0])
            else:
                continue
            if strike_min <= strike <= strike_max:
                result.append(sym)
        except (ValueError, IndexError):
            continue
    return result


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def _get_window_ist(d: date) -> tuple[int, int]:
    try:
        from zoneinfo import ZoneInfo
        ist = ZoneInfo("Asia/Kolkata")
        start = datetime.combine(d, time(9, 15), tzinfo=ist)
        end   = datetime.combine(d, time(15, 30), tzinfo=ist)
        return int(start.timestamp()), int(end.timestamp())
    except Exception:
        start = datetime.combine(d, time(9, 15))
        end   = datetime.combine(d, time(15, 30))
        epoch = datetime(1970, 1, 1)
        return int((start - epoch).total_seconds()), int((end - epoch).total_seconds())


def _epoch_to_ist(ts: float) -> datetime:
    try:
        from zoneinfo import ZoneInfo
        dt = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC")).astimezone(ZoneInfo("Asia/Kolkata"))
    except ImportError:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=5, minutes=30)
    return dt.replace(tzinfo=None)


# ---------------------------------------------------------------------------
# WebSocket client
# ---------------------------------------------------------------------------

class GDFLHistoryClient:
    def __init__(self, api_key: str, endpoint: str, exchange: str):
        self.api_key  = api_key
        self.endpoint = endpoint
        self.exchange = exchange
        self.ws: Optional[websockets.WebSocketClientProtocol] = None

    async def __aenter__(self) -> "GDFLHistoryClient":
        await self._connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self.ws:
            await self.ws.close()
            self.ws = None

    async def _connect(self) -> None:
        uri = f"{self.endpoint}?api_key={self.api_key}"
        print(f"Connecting ({self.exchange})...")
        self.ws = await websockets.connect(uri, ping_interval=None, max_size=100 * 1024 * 1024)
        print("✓ Connected")
        await self._authenticate()

    async def _authenticate(self) -> None:
        assert self.ws is not None
        await self.ws.send(json.dumps({"MessageType": "Authenticate", "Password": self.api_key}))
        _skip = {"Echo", "AllowVMRunningResult", "AllowServerOSRunningResult"}
        for attempt in range(20):
            try:
                raw = await asyncio.wait_for(self.ws.recv(), timeout=5.0)
            except asyncio.TimeoutError:
                print(f"  Auth timeout (attempt {attempt + 1}/20)...")
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            msg_type = data.get("MessageType", "")
            if msg_type in _skip:
                continue
            if msg_type == "AuthenticateResult":
                if data.get("Complete") in (True, "true"):
                    print("✓ Authenticated")
                    return
                raise RuntimeError(f"Authentication failed: {data}")
        raise RuntimeError("Authentication timed out")

    async def get_history(self, identifier: str, from_epoch: int, to_epoch: int) -> List[Dict[str, Any]]:
        assert self.ws is not None
        user_tag = f"HIST_{identifier}"
        await self.ws.send(json.dumps({
            "MessageType": "GetHistory",
            "Exchange": self.exchange,
            "InstrumentIdentifier": identifier,
            "Periodicity": "MINUTE",
            "Period": 1,
            "From": from_epoch,
            "To": to_epoch,
            "Max": 0,
            "UserTag": user_tag,
            "isShortIdentifier": False,
        }))

        while True:
            try:
                raw = await asyncio.wait_for(self.ws.recv(), timeout=30.0)
            except asyncio.TimeoutError:
                print(f"  ⚠️  Timeout waiting for history: {identifier}")
                return []
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = data.get("MessageType", "")
            if msg_type in ("HistoryOHLCResult", "HistoryResult"):
                if data.get("UserTag") not in (None, user_tag):
                    continue
                result = data.get("Result") or data.get("History") or data.get("Data") or []
                rows = [dict(r) for r in result if isinstance(r, dict)]
                for r in rows:
                    r.setdefault("InstrumentIdentifier", identifier)
                print(f"  ✓ {identifier}: {len(rows)} bars")
                return rows

            if msg_type in ("RequestError",) or "Error" in msg_type or data.get("Error"):
                err = data.get("Message", "")
                print(f"  ⚠️  Error for {identifier}: {data}")
                if "disabled" in err.lower():
                    raise RuntimeError(f"Exchange {self.exchange} disabled: {err}")
                return []


# ---------------------------------------------------------------------------
# DuckDB setup
# ---------------------------------------------------------------------------

def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS spot_data (
            date DATE, datetime TIMESTAMP,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS options_data (
            date DATE, datetime TIMESTAMP,
            strike_price INTEGER, option_type VARCHAR, expiry_date DATE,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume BIGINT, open_interest BIGINT
        )
    """)
    print("✓ DuckDB tables ready")


def parse_option(identifier: str) -> Optional[Dict[str, Any]]:
    """Parse OPTIDX_NIFTY_DDMONYYYY_CE/PE_STRIKE → dict, or None if not an option."""
    if not identifier.startswith("OPTIDX_NIFTY_"):
        return None
    parts = identifier.split("_")
    if len(parts) < 5:
        return None
    try:
        return {
            "expiry_date":  datetime.strptime(parts[2], "%d%b%Y").date(),
            "option_type":  "CALL" if parts[3] == "CE" else "PUT",
            "strike_price": int(float(parts[4])),
        }
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    instruments = load_master_from_csv(MASTER_CSV)
    if not instruments:
        return

    # Resolve which day to fetch (today by default, or a historical date via mode 2)
    today = _resolve_trade_date()

    print("\nLoading all NIFTY options for ALL expiries...")
    all_symbols = filter_nifty_all_expiry_symbols(instruments, ref_date=today)
    if not all_symbols:
        print("✗ No symbols found")
        return

    from_epoch, to_epoch = _get_window_ist(today)
    print(f"\nFetching data for {today}  (09:15–15:30 IST)")
    print(f"  epoch window: {from_epoch} → {to_epoch}")

    conn = duckdb.connect(DB_PATH)
    create_tables(conn)

    spot_rows:    List[Dict[str, Any]] = []
    options_rows: List[Dict[str, Any]] = []

    try:
        # ── 1. Spot ──────────────────────────────────────────────────────────
        print(f"\n{'='*70}\nFetching NIFTY SPOT ({SPOT_EXCHANGE})\n{'='*70}")
        try:
            async with GDFLHistoryClient(API_KEY, WS_ENDPOINT, SPOT_EXCHANGE) as c:
                raw_bars = await c.get_history(SPOT_SYMBOL, from_epoch, to_epoch)
            for bar in raw_bars:
                ts = bar.get("LastTradeTime")
                if ts:
                    spot_rows.append({
                        "date": today, "datetime": _epoch_to_ist(ts),
                        "open":  bar.get("Open",  0.0),
                        "high":  bar.get("High",  0.0),
                        "low":   bar.get("Low",   0.0),
                        "close": bar.get("Close", 0.0),
                        "volume": bar.get("TradedQty", 0),
                    })
            print(f"  ✓ {len(spot_rows)} spot bars received")
        except RuntimeError as e:
            if "disabled" in str(e).lower():
                print(f"  ⚠️  {SPOT_EXCHANGE} disabled for this key — skipping spot")
            else:
                raise

        # ── 2. Dynamic strike range ──────────────────────────────────────────
        if spot_rows:
            day_high = max(r["high"] for r in spot_rows)
            day_low  = min(r["low"]  for r in spot_rows)
            s_min, s_max = day_low - STRIKE_PADDING, day_high + STRIKE_PADDING
            options_symbols = _filter_symbols_by_strike(all_symbols, s_min, s_max)
            print(f"\n  Spot {day_low:.0f}–{day_high:.0f}  →  strikes {s_min:.0f}–{s_max:.0f}")
            print(f"  {len(options_symbols)} options selected across all expiries")
        else:
            options_symbols = all_symbols
            print("\n  ⚠️  No spot data — fetching all symbols as fallback")

        # ── 3. Options ───────────────────────────────────────────────────────
        print(f"\n{'='*70}\nFetching NIFTY OPTIONS ({OPTIONS_EXCHANGE}) — {len(options_symbols)} symbols\n{'='*70}")
        async with GDFLHistoryClient(API_KEY, WS_ENDPOINT, OPTIONS_EXCHANGE) as c:
            for identifier in options_symbols:
                try:
                    bars = await c.get_history(identifier, from_epoch, to_epoch)
                except Exception as e:
                    print(f"  ✗ {identifier}: {e}")
                    continue
                if not bars:
                    continue
                parsed = parse_option(identifier)
                if not parsed:
                    continue
                for bar in bars:
                    ts = bar.get("LastTradeTime")
                    if not ts:
                        continue
                    options_rows.append({
                        "date": today, "datetime": _epoch_to_ist(ts),
                        "strike_price": parsed["strike_price"],
                        "option_type":  parsed["option_type"],
                        "expiry_date":  parsed["expiry_date"],
                        "open":  bar.get("Open",  0.0),
                        "high":  bar.get("High",  0.0),
                        "low":   bar.get("Low",   0.0),
                        "close": bar.get("Close", 0.0),
                        "volume":        bar.get("TradedQty",     0),
                        "open_interest": bar.get("OpenInterest",  0),
                    })

    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
    finally:
        if spot_rows:
            df = pd.DataFrame(spot_rows)
            df["datetime"] = pd.to_datetime(df["datetime"])
            df["date"]     = pd.to_datetime(df["date"]).dt.date
            conn.execute("INSERT INTO spot_data SELECT * FROM df")
            print(f"\n✓ Inserted {len(spot_rows)} spot rows")

        if options_rows:
            df = pd.DataFrame(options_rows)
            df["datetime"]    = pd.to_datetime(df["datetime"])
            df["date"]        = pd.to_datetime(df["date"]).dt.date
            df["expiry_date"] = pd.to_datetime(df["expiry_date"]).dt.date
            conn.execute("INSERT INTO options_data SELECT * FROM df")
            print(f"✓ Inserted {len(options_rows)} options rows")

        conn.close()
        print(f"✓ Saved to {DB_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
