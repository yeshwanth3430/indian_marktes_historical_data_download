"""
Probe GetHistory on the GDFL Nimble feed for a NIFTY future and a NIFTY option.
Prints the raw reply so we can see exactly what the API replays.
"""

import asyncio
import json
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))

import websockets

from gdfl_config import NSE_API_KEY as API_KEY, WS_ENDPOINT as WS_URL

# (exchange, identifier, from_dt, to_dt) — NIFTY 30JUN2026 future only
TARGETS = [
    ("NFO", "FUTIDX_NIFTY_30JUN2026_XX_0",
     datetime(2026, 6, 23, 11, 0), datetime(2026, 6, 23, 11, 1)),
]


def epoch(dt: datetime) -> int:
    # naive datetimes are interpreted as IST (NSE market time)
    return int(dt.replace(tzinfo=IST).timestamp())


async def get_history(ws, exchange, identifier, frm, to):
    req = {
        "MessageType": "GetHistory",
        "Exchange": exchange,
        "InstrumentIdentifier": identifier,
        "Periodicity": "TICK",
        "From": epoch(frm),
        "To": epoch(to),
        "Max": 10,
        "UserTag": f"PROBE_{identifier}",
        "isShortIdentifier": False,
    }
    print(f"\n>>> REQUEST {identifier}")
    print(json.dumps(req, indent=2))
    await ws.send(json.dumps(req))

    print("<<< REPLY(IES):")
    rows = 0
    # server-pushed control/keepalive messages we just ignore
    SKIP = {"AllowVMRunningResult", "AllowServerOSRunningResult", "ServerPingResult", "Echo"}
    while True:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=20.0)
        except asyncio.TimeoutError:
            print("  (no more messages — timeout)")
            return
        data = json.loads(raw)
        mt = data.get("MessageType", "")
        if mt in SKIP:
            continue
        print(f"  [{mt}] {json.dumps(data)}")
        if mt in ("HistoryOHLCResult", "HistoryResult", "HistoryTickResult"):
            rows += 1
            if rows >= 5:
                return
        else:
            # error or terminal message
            return


async def main():
    uri = f"{WS_URL}?api_key={API_KEY}"
    print(f"Connecting to {uri} ...")
    async with websockets.connect(uri, ping_interval=None, max_size=100 * 1024 * 1024) as ws:
        await ws.send(json.dumps({"MessageType": "Authenticate", "Password": API_KEY}))
        while True:
            data = json.loads(await ws.recv())
            if data.get("MessageType") == "AuthenticateResult":
                print(f"Auth: {data}")
                if data.get("Complete") in (True, "true"):
                    break
                raise RuntimeError("Authentication failed")

        for exch, ident, frm, to in TARGETS:
            await get_history(ws, exch, ident, frm, to)


if __name__ == "__main__":
    asyncio.run(main())
