"""
Download NSE instrument master from GDFL into gdfl_master_nse.csv
Exchanges: NFO (options/futures) + NSE_IDX (spot indices)
Products:  NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY
Run this once before running the NIFTY intraday data script.
"""

import asyncio
import csv
import json
from typing import Any, Dict, List, Optional
import websockets

from gdfl_config import NSE_API_KEY as API_KEY, WS_ENDPOINT as WS_URL
OUT_CSV  = "gdfl_master_nse.csv"
PRODUCTS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]


async def _connect(api_key: str) -> websockets.WebSocketClientProtocol:
    uri = f"{WS_URL}?api_key={api_key}"
    print(f"Connecting to {uri}...")
    ws = await websockets.connect(uri, ping_interval=None, max_size=100 * 1024 * 1024)
    print("✓ Connected")
    return ws


async def _authenticate(ws: websockets.WebSocketClientProtocol, api_key: str) -> None:
    await ws.send(json.dumps({"MessageType": "Authenticate", "Password": api_key}))
    while True:
        data = json.loads(await ws.recv())
        if data.get("MessageType") == "AuthenticateResult":
            if data.get("Complete") in (True, "true"):
                print("✓ Authenticated")
                return
            raise RuntimeError(f"Authentication failed: {data}")


async def _get_instruments(
    ws: websockets.WebSocketClientProtocol,
    exchange: str,
    product: Optional[str] = None,
    instrument_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    msg: Dict[str, Any] = {
        "MessageType": "GetInstruments",
        "Exchange": exchange,
        "OnlyActive": True,
        "detailedInfo": True,
    }
    if product:
        msg["Product"] = product
    if instrument_type:
        msg["InstrumentType"] = instrument_type

    await ws.send(json.dumps(msg))
    print(f"  Requested {product or instrument_type or 'all'} from {exchange}...")

    for _ in range(60):
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=60.0)
            data = json.loads(raw)
            if data.get("MessageType") == "InstrumentsResult":
                instruments = data.get("Result") or data.get("Instruments") or []
                print(f"  ✓ Received {len(instruments)} instruments")
                return instruments
        except asyncio.TimeoutError:
            continue
        except websockets.exceptions.ConnectionClosed:
            print("  ⚠️  Connection closed (response too large?)")
            break
    return []


def _write_csv(instruments: List[Dict[str, Any]], path: str) -> None:
    if not instruments:
        print("No instruments to save.")
        return
    fieldnames = sorted({k for inst in instruments for k in inst.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(instruments)
    print(f"\n✓ Saved {len(instruments)} instruments → {path}")


async def main() -> None:
    ws = await _connect(API_KEY)
    try:
        await _authenticate(ws, API_KEY)
        all_instruments: List[Dict[str, Any]] = []

        # NFO: options and futures for each product
        print("\n--- NFO products ---")
        for product in PRODUCTS:
            rows = await _get_instruments(ws, exchange="NFO", product=product)
            all_instruments.extend(rows)
            await asyncio.sleep(0.5)

        # NFO: remaining futures not covered per-product
        print("\n--- NFO FUTIDX (catch-all) ---")
        existing_ids = {inst.get("Identifier") for inst in all_instruments}
        rows = await _get_instruments(ws, exchange="NFO", instrument_type="FUTIDX")
        new = [r for r in rows if r.get("Identifier") not in existing_ids]
        all_instruments.extend(new)
        print(f"  Added {len(new)} new FUTIDX rows")

        _write_csv(all_instruments, OUT_CSV)

    finally:
        await ws.close()
        print("Disconnected")


if __name__ == "__main__":
    asyncio.run(main())
