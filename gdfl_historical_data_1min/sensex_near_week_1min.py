"""
Fetch intraday 1-minute historical data from GDFL using GetHistory API
and store in DuckDB with same structure as nifty_data_gdfl.duckdb:
- spot_data: SENSEX spot/index data
- options_data: SENSEX options data

Time window: today only, 09:15 to 15:30 (IST).
"""

import asyncio
import json
import duckdb
import pandas as pd
from datetime import datetime, time, date, timedelta, timezone
from typing import Any, Dict, List, Optional
import websockets


def _patch_websockets_for_older_python() -> None:
    """On Python < 3.11, asyncio.create_connection() does not accept read_limit/write_limit."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    orig = loop.create_connection
    if getattr(orig, "_read_limit_patched", False):
        return

    def create_connection(*args: Any, **kwargs: Any) -> Any:
        kwargs.pop("read_limit", None)
        kwargs.pop("write_limit", None)
        return orig(*args, **kwargs)

    create_connection._read_limit_patched = True  # type: ignore[attr-defined]
    loop.create_connection = create_connection


import csv
import os

from gdfl_config import BSE_API_KEY as API_KEY, WS_ENDPOINT

# BSE exchanges for SENSEX
SPOT_EXCHANGE = "BSE_IDX"
OPTIONS_EXCHANGE = "BFO"


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


def load_master_from_csv(csv_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(csv_path):
        print(f"✗ Master file not found: {csv_path}")
        return []
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            instruments = list(csv.DictReader(f))
        print(f"✓ Loaded {len(instruments)} instruments from {csv_path}")
        return instruments
    except Exception as e:
        print(f"⚠️  Error reading {csv_path}: {e}")
        return []


def _get_window_ist_for_date(d: date) -> tuple[int, int]:
    """Compute UNIX epoch seconds for a given date's 09:15–15:30 in IST."""
    try:
        from zoneinfo import ZoneInfo
        ist = ZoneInfo("Asia/Kolkata")
        start_dt = datetime.combine(d, time(9, 15), tzinfo=ist)
        end_dt = datetime.combine(d, time(15, 30), tzinfo=ist)
        return int(start_dt.timestamp()), int(end_dt.timestamp())
    except Exception:
        start_dt = datetime.combine(d, time(9, 15))
        end_dt = datetime.combine(d, time(15, 30))
        epoch = datetime(1970, 1, 1)
        return int((start_dt - epoch).total_seconds()), int((end_dt - epoch).total_seconds())


def find_nearest_sensex_expiry(
    instruments: List[Dict[str, Any]],
    ref_date: Optional[date] = None,
    min_strikes: int = 100,
) -> Optional[str]:
    """Return the nearest SENSEX expiry (on or after ref_date) that has a real
    options chain.

    The master contains thin, newly-listed SENSEX series (e.g. a Tuesday monthly
    with only ~40 strikes) that carry no trade history in GDFL. Counting IO
    strikes per expiry and requiring at least ``min_strikes`` skips those phantom
    series so we pick the genuine active near expiry (which actually returns bars).
    """
    today_date = ref_date or datetime.now().date()

    # Count IO option strikes per expiry so we can skip sparse (non-active) series.
    strike_counts: Dict[str, int] = {}
    for inst in instruments:
        if (
            inst.get("Product", "").strip() == "SENSEX"
            and inst.get("Name", "").strip() == "IO"
        ):
            expiry_str = inst.get("Expiry", "").strip()
            if expiry_str:
                strike_counts[expiry_str] = strike_counts.get(expiry_str, 0) + 1

    nearest_expiry = None
    min_days_diff = float("inf")
    for expiry_str, count in strike_counts.items():
        if count < min_strikes:
            continue
        try:
            expiry_date = datetime.strptime(expiry_str, "%d%b%Y").date()
        except ValueError:
            continue
        if expiry_date >= today_date:
            days_diff = (expiry_date - today_date).days
            if days_diff < min_days_diff:
                min_days_diff = days_diff
                nearest_expiry = expiry_str

    return nearest_expiry


def filter_sensex_current_week_symbols(
    instruments: List[Dict[str, Any]], ref_date: Optional[date] = None
) -> List[str]:
    """Return ALL SENSEX options for the nearest weekly expiry (no strike filter)."""
    nearest_expiry = find_nearest_sensex_expiry(instruments, ref_date=ref_date)
    if not nearest_expiry:
        print("Could not find SENSEX expiry dates")
        return []

    print(f"Found nearest SENSEX expiry: {nearest_expiry}")

    symbols: List[str] = []
    for inst in instruments:
        if (
            inst.get("Product", "").strip() == "SENSEX"
            and inst.get("Expiry", "").strip() == nearest_expiry
            and inst.get("Name", "").strip() == "IO"
        ):
            identifier = inst.get("Identifier", "").strip()
            if identifier and ("_CE_" in identifier or "_PE_" in identifier):
                symbols.append(identifier)

    print(f"Loaded {len(symbols)} SENSEX options for expiry {nearest_expiry} (all strikes)")
    return symbols


def _filter_symbols_by_strike(
    symbols: List[str], strike_min: float, strike_max: float
) -> List[str]:
    """Keep only symbols whose strike falls within [strike_min, strike_max]."""
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


class GDFLHistoryClient:
    """WebSocket client for GDFL GetHistory API calls."""

    def __init__(self, api_key: str, endpoint: str = WS_ENDPOINT, exchange: str = OPTIONS_EXCHANGE):
        self.api_key = api_key
        self.endpoint = endpoint
        self.exchange = exchange
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None

    async def __aenter__(self) -> "GDFLHistoryClient":
        await self.connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.disconnect()

    async def connect(self) -> None:
        uri = f"{self.endpoint}?api_key={self.api_key}"
        print(f"Connecting to {uri}...")
        self.websocket = await websockets.connect(
            uri,
            ping_interval=None,
            max_size=100 * 1024 * 1024,
        )
        print("✓ Connected")
        await self._authenticate()

    async def disconnect(self) -> None:
        if self.websocket is not None:
            await self.websocket.close()
            self.websocket = None
            print("Disconnected")

    async def _authenticate(self) -> None:
        assert self.websocket is not None
        msg = {"MessageType": "Authenticate", "Password": self.api_key}
        await self.websocket.send(json.dumps(msg))
        _skip = {"Echo", "AllowVMRunningResult", "AllowServerOSRunningResult"}
        for attempt in range(20):
            try:
                raw = await asyncio.wait_for(self.websocket.recv(), timeout=5.0)
            except asyncio.TimeoutError:
                print(f"  Auth wait timeout (attempt {attempt + 1}/20), retrying...")
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
            print(f"  Auth: skipping unexpected message type '{msg_type}'")
        raise RuntimeError("Authentication timed out after 20 attempts")

    async def get_history_for_symbol(
        self,
        identifier: str,
        from_epoch: int,
        to_epoch: int,
        max_bars: int = 0,
    ) -> List[Dict[str, Any]]:
        """Call GetHistory for a single instrument identifier and return OHLC rows."""
        assert self.websocket is not None

        user_tag = f"HIST_{identifier}"
        req = {
            "MessageType": "GetHistory",
            "Exchange": self.exchange,
            "InstrumentIdentifier": identifier,
            "Periodicity": "MINUTE",
            "Period": 1,
            "From": from_epoch,
            "To": to_epoch,
            "Max": max_bars,
            "UserTag": user_tag,
            "isShortIdentifier": False,
        }
        await self.websocket.send(json.dumps(req))

        rows: List[Dict[str, Any]] = []

        while True:
            raw = await self.websocket.recv()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = data.get("MessageType", "")

            if msg_type in ("HistoryOHLCResult", "HistoryResult"):
                if data.get("UserTag") not in (None, user_tag):
                    continue
                result = data.get("Result") or data.get("History") or data.get("Data")
                if isinstance(result, list):
                    for item in result:
                        if isinstance(item, dict):
                            item = dict(item)
                            item.setdefault("InstrumentIdentifier", identifier)
                            rows.append(item)
                print(f"  ✓ {identifier}: received {len(rows)} bars")
                return rows

            if msg_type == "RequestError" or "Error" in msg_type or data.get("Error"):
                error_msg = data.get("Message", "")
                print(f"  ⚠️  History error for {identifier}: {data}")
                if "disabled" in error_msg.lower():
                    raise RuntimeError(f"Exchange {self.exchange} is disabled: {error_msg}")
                return []


def parse_symbol(identifier: str) -> Dict[str, Any]:
    """
    Parse GDFL SENSEX identifier to extract option components.
    Examples:
    - SENSEX (from BSE_IDX) -> spot/index
    - OPTIDX_SENSEX_03FEB2026_CE_80000 -> option
    - FUTIDX_SENSEX_03FEB2026_XX_0 -> future (skip)
    """
    result = {
        "is_spot": False,
        "is_option": False,
        "strike_price": None,
        "option_type": None,
        "expiry_date": None,
    }

    if identifier == "SENSEX":
        result["is_spot"] = True
        return result

    # BFO options format: IO_SENSEX_30APR2026_CE_80000
    if identifier.startswith("IO_SENSEX_"):
        parts = identifier.split("_")
        if len(parts) >= 5:
            try:
                result["expiry_date"] = datetime.strptime(parts[2], "%d%b%Y").date()
                result["option_type"] = parts[3]  # CE or PE
                result["strike_price"] = int(float(parts[4]))
                result["is_option"] = True
            except (ValueError, IndexError):
                pass

    return result


def create_duckdb_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create spot_data and options_data tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS spot_data (
            date DATE,
            datetime TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS options_data (
            date DATE,
            datetime TIMESTAMP,
            strike_price INTEGER,
            option_type VARCHAR,
            expiry_date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            open_interest BIGINT
        )
    """)

    print("✓ DuckDB tables created/verified")


async def main() -> None:
    _patch_websockets_for_older_python()

    instruments = load_master_from_csv("gdfl_master_bse.csv")
    if not instruments:
        print("✗ Could not load gdfl_master_bse.csv — run download_bse_master.py first")
        return

    # Resolve which day to fetch (today by default, or a historical date via mode 2)
    trade_date = _resolve_trade_date()

    print("\nLoading all SENSEX options for nearest week expiry...")
    all_options_symbols = filter_sensex_current_week_symbols(instruments, ref_date=trade_date)
    if not all_options_symbols:
        print("✗ No symbols found in master")
        return

    spot_symbol = "SENSEX"
    print(f"\nSpot symbol: {spot_symbol} (from {SPOT_EXCHANGE} exchange)")

    dates_to_fetch = [
        (trade_date, *_get_window_ist_for_date(trade_date)),
    ]

    print(f"\nRequesting 1-minute history for {trade_date} (09:15–15:30 IST)")

    db_path = "sensex_data_nearest_week_gdfl.duckdb"
    conn = duckdb.connect(db_path)
    create_duckdb_tables(conn)

    spot_rows: List[Dict[str, Any]] = []
    options_rows: List[Dict[str, Any]] = []

    try:
        for trade_date, from_epoch, to_epoch in dates_to_fetch:
            date_str = trade_date.isoformat()
            print(f"\n--- {date_str} (From={from_epoch}, To={to_epoch}) ---")

            # Fetch SENSEX spot from BSE_IDX exchange
            print(f"\n{'='*70}")
            print(f"Fetching SENSEX SPOT for {date_str} ({SPOT_EXCHANGE})...")
            print(f"{'='*70}")
            try:
                async with GDFLHistoryClient(API_KEY, WS_ENDPOINT, SPOT_EXCHANGE) as spot_client:
                    print(f"\nFetching history for {spot_symbol} ({SPOT_EXCHANGE} exchange)...")
                    spot_data_rows = await spot_client.get_history_for_symbol(spot_symbol, from_epoch, to_epoch)

                    if spot_data_rows:
                        print(f"  ✓ Received {len(spot_data_rows)} bars for SENSEX spot")
                        for row in spot_data_rows:
                            last_trade_time = row.get("LastTradeTime")
                            if not last_trade_time:
                                continue
                            try:
                                from zoneinfo import ZoneInfo
                                dt_utc = datetime.fromtimestamp(last_trade_time, tz=ZoneInfo("UTC"))
                                dt_ist = dt_utc.astimezone(ZoneInfo("Asia/Kolkata"))
                            except ImportError:
                                dt_utc = datetime.fromtimestamp(last_trade_time, tz=timezone.utc)
                                dt_ist = dt_utc + timedelta(hours=5, minutes=30)
                            dt_ist = dt_ist.replace(tzinfo=None)
                            spot_rows.append({
                                "date": trade_date,
                                "datetime": dt_ist,
                                "open": row.get("Open", 0.0),
                                "high": row.get("High", 0.0),
                                "low": row.get("Low", 0.0),
                                "close": row.get("Close", 0.0),
                                "volume": row.get("TradedQty", 0),
                            })
                    else:
                        print(f"  ⚠️  No spot data returned")
            except RuntimeError as e:
                if "disabled" in str(e).lower():
                    print(f"\n  ⚠️  {SPOT_EXCHANGE} exchange is DISABLED for your API key.")
                    print(f"  ⚠️  Skipping spot for {date_str}.")
                else:
                    raise
            except Exception as e:
                print(f"  ✗ Error fetching spot data: {e}")

            # Compute dynamic strike range from today's spot high/low ± 3000
            date_spot = [r for r in spot_rows if r["date"] == trade_date]
            if date_spot:
                day_high = max(r["high"] for r in date_spot)
                day_low  = min(r["low"]  for r in date_spot)
                strike_min = day_low  - 3000
                strike_max = day_high + 3000
                options_symbols = _filter_symbols_by_strike(all_options_symbols, strike_min, strike_max)
                print(f"\n  Spot range: {day_low:.0f}–{day_high:.0f}  →  strikes {strike_min:.0f}–{strike_max:.0f}")
                print(f"  Selected {len(options_symbols)} options within range")
            else:
                options_symbols = all_options_symbols
                print("\n  ⚠️  No spot data — using all symbols as fallback")

            # Fetch SENSEX options from BFO exchange
            print(f"\n{'='*70}")
            print(f"Fetching SENSEX OPTIONS for {date_str} ({OPTIONS_EXCHANGE})...")
            print(f"{'='*70}")
            async with GDFLHistoryClient(API_KEY, WS_ENDPOINT, OPTIONS_EXCHANGE) as options_client:
                for identifier in options_symbols:
                    print(f"\nFetching history for {identifier}...")
                    try:
                        rows = await options_client.get_history_for_symbol(identifier, from_epoch, to_epoch)
                    except Exception as e:
                        print(f"  ✗ Error for {identifier}: {e}")
                        continue

                    if not rows:
                        print(f"  ⚠️  No data returned for {identifier}")
                        continue

                    parsed = parse_symbol(identifier)
                    if not parsed["is_option"]:
                        continue

                    for row in rows:
                        last_trade_time = row.get("LastTradeTime")
                        if not last_trade_time:
                            continue
                        try:
                            from zoneinfo import ZoneInfo
                            dt_utc = datetime.fromtimestamp(last_trade_time, tz=ZoneInfo("UTC"))
                            dt_ist = dt_utc.astimezone(ZoneInfo("Asia/Kolkata"))
                        except ImportError:
                            dt_utc = datetime.fromtimestamp(last_trade_time, tz=timezone.utc)
                            dt_ist = dt_utc + timedelta(hours=5, minutes=30)
                        dt_ist = dt_ist.replace(tzinfo=None)
                        opt_type = "CALL" if parsed["option_type"] == "CE" else "PUT"
                        options_rows.append({
                            "date": trade_date,
                            "datetime": dt_ist,
                            "strike_price": parsed["strike_price"],
                            "option_type": opt_type,
                            "expiry_date": parsed["expiry_date"],
                            "open": row.get("Open", 0.0),
                            "high": row.get("High", 0.0),
                            "low": row.get("Low", 0.0),
                            "close": row.get("Close", 0.0),
                            "volume": row.get("TradedQty", 0),
                            "open_interest": row.get("OpenInterest", 0),
                        })

    except Exception as e:
        print(f"\n✗ Error while fetching history: {e}")
    finally:
        if spot_rows:
            print(f"\nInserting {len(spot_rows)} spot rows into DuckDB...")
            spot_df = pd.DataFrame(spot_rows)
            spot_df["datetime"] = pd.to_datetime(spot_df["datetime"])
            spot_df["date"] = pd.to_datetime(spot_df["date"]).dt.date
            conn.execute("INSERT INTO spot_data SELECT * FROM spot_df")
            print(f"✓ Inserted {len(spot_rows)} spot rows")

        if options_rows:
            print(f"\nInserting {len(options_rows)} options rows into DuckDB...")
            options_df = pd.DataFrame(options_rows)
            options_df["datetime"] = pd.to_datetime(options_df["datetime"])
            options_df["date"] = pd.to_datetime(options_df["date"]).dt.date
            options_df["expiry_date"] = pd.to_datetime(options_df["expiry_date"]).dt.date
            conn.execute("INSERT INTO options_data SELECT * FROM options_df")
            print(f"✓ Inserted {len(options_rows)} options rows")

        conn.close()
        print(f"\n✓ Data saved to {db_path}")


if __name__ == "__main__":
    asyncio.run(main())
