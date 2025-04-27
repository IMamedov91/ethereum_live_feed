#!/usr/bin/env python3
"""
kucoin_btc_feed.py — v2.1
Haalt 15‑min‑indicatoren (EMA20/50/200, RSI14, ATR14, VWAP) via TAAPI.io
en uploadt de laatste candle + 300‑bar snapshot naar je GitHub‑gist.

Wijzigingen v2.1
----------------
* `addResultTimestamp=True` zodat de API weer een `timestamp` meegeeft.
* Veilige fallback op `timestampMs` voor toekomstige API‑wijzigingen.
* Kleinere refactor / type hints.
"""

from __future__ import annotations
import os, time, json, requests, datetime as dt
from typing import Dict, List, Any

# ────────────────────────────────────────────────────────────────
# Configuratie uit environment
# ────────────────────────────────────────────────────────────────
SECRET     = os.getenv("TAAPI_SECRET")        # TAAPI‑key
GIST_ID    = os.getenv("GIST_ID")             # GitHub‑gist ID
GIST_TOKEN = os.getenv("GIST_TOKEN")          # GitHub‑token

PAIR   = "BTC/USDT"  # trading pair
TF     = "15m"       # timeframe
LIMIT  = 300         # snapshotlengte (aantal candles)

TAAPI_URL = "https://api.taapi.io/bulk"

# ────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────

def iso(ms: int | float) -> str:
    """Unix‑tijd in milliseconden ⇒ ISO‑8601 (UTC)."""
    return (
        dt.datetime.utcfromtimestamp(ms / 1000)
        .isoformat(timespec="seconds") + "Z"
    )

# ────────────────────────────────────────────────────────────────
# TAAPI integratie
# ────────────────────────────────────────────────────────────────

def fetch_indicators() -> List[Dict[str, Any]]:
    """Vraagt indicator‑data op bij TAAPI.io (Bulk‑endpoint)."""
    payload = {
        "secret": SECRET,
        "construct": {
            "exchange": "binance",
            "symbol": PAIR,
            "interval": TF,
            "backtrack": LIMIT,          # laatste 300 candles (zie docs / beperkingen)
            "addResultTimestamp": True,  # zorg dat `timestamp` aanwezig blijft
            "indicators": [
                {"id": "ema20",  "indicator": "ema", "optInTimePeriod": 20},
                {"id": "ema50",  "indicator": "ema", "optInTimePeriod": 50},
                {"id": "ema200", "indicator": "ema", "optInTimePeriod": 200},
                {"id": "rsi14",  "indicator": "rsi", "optInTimePeriod": 14},
                {"id": "atr14",  "indicator": "atr", "optInTimePeriod": 14},
                {"id": "vwap",   "indicator": "vwap", "anchorPeriod": "session"},
            ],
        },
    }

    r = requests.post(TAAPI_URL, json=payload, timeout=10)
    r.raise_for_status()
    return r.json()["data"]  # lijst van indicator‑dicts

# ────────────────────────────────────────────────────────────────
# Data transformeren
# ────────────────────────────────────────────────────────────────

def reshape(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Bundelt afzonderlijke indicator‑responses tot één candle‑snapshot."""
    bars: Dict[int, Dict[str, Any]] = {}

    for item in data:
        res = item["result"]
        # defensieve timestamp extractie
        ts_raw = res.get("timestamp") or res.get("timestampMs")
        if ts_raw is None:
            # sla veld over als er écht geen timestamp is (zou niet mogen gebeuren)
            continue
        bar = int(ts_raw)

        # verzamel indicatoren + OHLCV per bar
        cell = bars.setdefault(bar, {})
        cell[item["id"]] = res["value"]  # indicator zelf
        for k in ("open", "close", "high", "low", "volume"):
            cell.setdefault(k, res.get(k))

    # op volgorde & beperk tot LIMIT candles
    ordered = [{"ts": ts, **vals} for ts, vals in sorted(bars.items())[-LIMIT:]]
    last = ordered[-1]

    return {
        "timestamp": last["ts"],
        "datetime_utc": iso(last["ts"]),
        "symbol": PAIR.replace("/", ""),
        "granularity": TF,
        "price": last["close"],
        "high": last["high"],
        "low": last["low"],
        "vol": last["volume"],
        "ema20": last["ema20"],
        "ema50": last["ema50"],
        "ema200": last["ema200"],
        "rsi14": last["rsi14"],
        "vwap": last["vwap"],
        "atr14": last["atr14"],
        "last_300_candles": ordered,
        # placeholders voor toekomstige uitbreidingen
        "funding_rate": None,
        "open_interest": None,
        "order_book": None,
        "generated_at": iso(int(time.time() * 1000)),
    }

# ────────────────────────────────────────────────────────────────
# GitHub Gist
# ────────────────────────────────────────────────────────────────

def push_gist(payload: Dict[str, Any]) -> None:
    headers = {
        "Authorization": f"token {GIST_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    r = requests.patch(
        f"https://api.github.com/gists/{GIST_ID}",
        headers=headers,
        json={"files": {"kucoin_btc_feed.json": {"content": json.dumps(payload, indent=2)}}},
        timeout=10,
    )
    r.raise_for_status()

# ────────────────────────────────────────────────────────────────
# Entry‑point
# ────────────────────────────────────────────────────────────────

def main() -> None:
    data = fetch_indicators()
    payload = reshape(data)
    push_gist(payload)
    print("✅ BTC TAAPI‑feed geüpload:", payload["generated_at"])

if __name__ == "__main__":
    main()
