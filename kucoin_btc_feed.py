#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kucoin_btc_feed.py — v2.3

Fetches 15‑minute indicators (EMA20/50/200, RSI14, ATR14, VWAP) from TAAPI.io
and uploads the latest candle plus a snapshot to your GitHub Gist.

TAAPI LIMITS
------------
* The Bulk endpoint returns **max 20 candles** per indicator via the `results` parameter.
* Need >20 candles? Loop over the Direct endpoint or persist history yourself.

Changes v2.3
------------
* `addResultTimestamp=True` and `results=20` set on **each** indicator (construct‑level is ignored).
* Removed exotic Unicode symbols that occasionally break CI linters.
* Extra defensive checks and clearer error messages.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
from typing import Any, Dict, List

import requests

# ────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────
SECRET = os.getenv("TAAPI_SECRET")
GIST_ID = os.getenv("GIST_ID")
GIST_TOKEN = os.getenv("GIST_TOKEN")

PAIR = "BTC/USDT"
TF = "15m"
LIMIT = 300  # desired snapshot length (≤300)
TAAPI_URL = "https://api.taapi.io/bulk"

# ────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────

def iso(ms: int | float) -> str:
    """Convert epoch milliseconds to ISO‑8601 (UTC)."""
    return dt.datetime.utcfromtimestamp(ms / 1000).isoformat(timespec="seconds") + "Z"

# ────────────────────────────────────────────────────────────────
# TAAPI integration
# ────────────────────────────────────────────────────────────────

def fetch_indicators() -> List[Dict[str, Any]]:
    """Request indicator data from TAAPI (Bulk endpoint)."""
    indicators: List[Dict[str, Any]] = [
        {"id": "ema20", "indicator": "ema", "optInTimePeriod": 20},
        {"id": "ema50", "indicator": "ema", "optInTimePeriod": 50},
        {"id": "ema200", "indicator": "ema", "optInTimePeriod": 200},
        {"id": "rsi14", "indicator": "rsi", "optInTimePeriod": 14},
        {"id": "atr14", "indicator": "atr", "optInTimePeriod": 14},
        {"id": "vwap", "indicator": "vwap", "anchorPeriod": "session"},
    ]

    for ind in indicators:
        ind["addResultTimestamp"] = True
        ind["results"] = 20  # TAAPI Bulk max

    payload: Dict[str, Any] = {
        "secret": SECRET,
        "construct": {
            "exchange": "binance",
            "symbol": PAIR,
            "interval": TF,
            "indicators": indicators,
        },
    }

    resp = requests.post(TAAPI_URL, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()["data"]

# ────────────────────────────────────────────────────────────────
# Transform data
# ────────────────────────────────────────────────────────────────

def reshape(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge indicator arrays into candle snapshots."""
    bars: Dict[int, Dict[str, Any]] = {}

    for item in data:
        items = item["result"] if isinstance(item["result"], list) else [item["result"]]
        for res in items:
            ts_raw = res.get("timestamp") or res.get("timestampMs")
            if ts_raw is None:
                continue
            ts = int(ts_raw)
            cell = bars.setdefault(ts, {})
            cell[item["id"]] = res["value"]
            for key in ("open", "close", "high", "low", "volume"):
                cell.setdefault(key, res.get(key))

    if not bars:
        raise RuntimeError("TAAPI response contained no usable timestamps; check API key & limits.")

    ordered = [{"ts": t, **vals} for t, vals in sorted(bars.items())[-LIMIT:]]
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
        "ema20": last.get("ema20"),
        "ema50": last.get("ema50"),
        "ema200": last.get("ema200"),
        "rsi14": last.get("rsi14"),
        "vwap": last.get("vwap"),
        "atr14": last.get("atr14"),
        "last_candles": ordered,  # max 20 with Bulk
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

    gist_body = {
        "files": {
            "kucoin_btc_feed.json": {
                "content": json.dumps(payload, indent=2, ensure_ascii=False)
            }
        }
    }

    resp = requests.patch(
        f"https://api.github.com/gists/{GIST_ID}",
        headers=headers,
        json=gist_body,
        timeout=10,
    )
    resp.raise_for_status()

# ────────────────────────────────────────────────────────────────
# Entry point
# ────────────────────────────────────────────────────────────────

def main() -> None:
    data = fetch_indicators()
    payload = reshape(data)
    push_gist(payload)
    print("✅ BTC TAAPI feed uploaded:", payload["generated_at"])


if __name__ == "__main__":
    main()
