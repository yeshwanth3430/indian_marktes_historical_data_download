"""
Probe how far back GDFL serves TICK history for NIFTY futures & options.

For each (identifier, test-datetime) it asks for a 5-minute tick window and
reports how many ticks came back + the first/last tick time. Empty / error
means no tick data is retained that far back (or the contract wasn't trading).

All requests share ONE websocket connection — the NSE key is single-session,
so never run two probes at once.
"""

import asyncio
import json
from datetime import datetime, timezone, timedelta

import websockets

from gdfl_config import NSE_API_KEY as API_KEY, WS_ENDPOINT as WS_URL

IST = timezone(timedelta(hours=5, minutes=30))

# Each contract is probed at dates within (and before) its trading life so we
# can see both the contract lifetime and GDFL's tick-retention wall.
# (exchange, identifier, test_datetime_IST)
# Walk each trading day back from today (2026-06-30) to find the exact
# tick-retention wall for the front-month future and a liquid option.
_DAYS = [
    datetime(2026, 6, d, 11, 0)
    for d in (30, 29, 26, 25, 24, 23, 20, 19, 18, 17, 16)  # weekdays only
]
PROBES = (
    [("NFO", "FUTIDX_NIFTY_30JUN2026_XX_0", d) for d in _DAYS]
    + [("NFO", "OPTIDX_NIFTY_30JUN2026_CE_24000", d) for d in _DAYS]
)

WINDOW_MIN = 5      # minutes per probe window
MAX_TICKS  = 200    # cap per request
SKIP = {"AllowVMRunningResult", "AllowServerOSRunningResult", "ServerPingResult", "Echo"}


def epoch(dt: datetime) -> int:
    return int(dt.replace(tzinfo=IST).timestamp())


async def authenticate(ws):
    await ws.send(json.dumps({"MessageType": "Authenticate", "Password": API_KEY}))
    while True:
        data = json.loads(await ws.recv())
        if data.get("MessageType") == "AuthenticateResult":
            if data.get("Complete") in (True, "true"):
                return
            raise RuntimeError(f"Auth failed: {data}")


async def probe_one(ws, exchange, identifier, when):
    frm, to = when, when + timedelta(minutes=WINDOW_MIN)
    req = {
        "MessageType": "GetHistory",
        "Exchange": exchange,
        "InstrumentIdentifier": identifier,
        "Periodicity": "TICK",
        "From": epoch(frm),
        "To": epoch(to),
        "Max": MAX_TICKS,
        "isShortIdentifier": False,
    }
    await ws.send(json.dumps(req))
    while True:
        try:
            data = json.loads(await asyncio.wait_for(ws.recv(), timeout=20.0))
        except asyncio.TimeoutError:
            return ("TIMEOUT", 0, None, None)
        mt = data.get("MessageType", "")
        if mt in SKIP:
            continue
        if mt == "HistoryTickResult":
            rows = data.get("Result", []) or []
            if not rows:
                return ("EMPTY", 0, None, None)
            times = [r["LastTradeTime"] for r in rows]
            lo = datetime.fromtimestamp(min(times), IST)
            hi = datetime.fromtimestamp(max(times), IST)
            return ("OK", len(rows), lo, hi)
        # RequestError / anything else is terminal for this probe
        return (mt or "?", 0, data.get("Message", ""), None)


async def main():
    uri = f"{WS_URL}?api_key={API_KEY}"
    print(f"Connecting to {uri} ...\n")
    async with websockets.connect(uri, ping_interval=None, max_size=100 * 1024 * 1024) as ws:
        await authenticate(ws)
        print(f"{'INSTRUMENT':<34} {'TEST DATE':<12} {'STATUS':<10} {'TICKS':>6}  RANGE")
        print("-" * 92)
        for exch, ident, when in PROBES:
            status, n, lo, hi = await probe_one(ws, exch, ident, when)
            rng = ""
            if status == "OK":
                rng = f"{lo:%H:%M:%S} → {hi:%H:%M:%S}"
            elif lo:  # error message
                rng = str(lo)
            print(f"{ident:<34} {when:%Y-%m-%d}   {status:<10} {n:>6}  {rng}")


if __name__ == "__main__":
    asyncio.run(main())
