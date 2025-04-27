#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kucoin_btc_feed.py — v2.4

Fetches 15‑minute indicators (EMA20/50/200, RSI14, ATR14, VWAP) from TAAPI.io
and uploads the latest candle plus a snapshot to your GitHub Gist.

**TAAPI LIMITS & QUIRKS**
------------------------
* Bulk endpoint returns **max 20 candles** per indicator when using `results`.
* Depending on the indicator, the `result` payload can be **either**
  *a list of dicts* **or** a *dict containing arrays* (`timestamp`, `value`, …).

Changes v2.4
------------
* Handles *vector‑style* payloads where `timestamp` is an **array** (fixes
  `TypeError: int() argument must be … list`).
* Cleaner helper `_ingest_bar()` to merge indicator values.
* Minor: ensured `open´/`close`/etc. are ingested only when provided.
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
LIMIT = 3000  # desired snapshot length (≤300)
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
        ind.update({"addResultTimestamp": True, "results": 20})  # TAAPI Bulk max

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

def _ingest_bar(bars: Dict[int, Dict[str, Any]], ts: int, indicator_id: str, value: Any, ohlcv: Dict[str, Any]) -> None:
    """Helper: insert/update a bar dict."""
    cell = bars.setdefault(ts, {})
    cell[indicator_id] = value
    for k in ("open", "close", "high", "low", "volume"):
        v = ohlcv.get(k)
        if v is not None:
            cell.setdefault(k, v)


def reshape(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge indicator data into candle snapshots (handles list & vector styles)."""
    bars: Dict[int, Dict[str, Any]] = {}

    for item in data:
        indicator_id = item["id"]
        res_obj = item["result"]

        # Style 1: list[dict]
        if isinstance(res_obj, list):
            for res in res_obj:
                ts_raw = res.get("timestamp") or res.get("timestampMs")
                if ts_raw is None:
                    continue
                _ingest_bar(bars, int(ts_raw), indicator_id, res["value"], res)

        # Style 2: dict with arrays
        elif isinstance(res_obj, dict) and isinstance(res_obj.get("timestamp") or res_obj.get("timestampMs"), list):
            ts_arr = res_obj.get("timestamp") or res_obj.get("timestampMs")
            val_arr = res_obj["value"]
            # optional arrays for OHLCV
            open_arr = res_obj.get("open")
            close_arr = res_obj.get("close")
            high_arr = res_obj.get("high")
            low_arr = res_obj.get("low")
            vol_arr = res_obj.get("volume")

            for idx, ts_raw in enumerate(ts_arr):
                ohlcv = {
                    "open":   open_arr[idx]   if open_arr   else None,
                    "close":  close_arr[idx]  if close_arr  else None,
                    "high":   high_arr[idx]   if high_arr   else None,
                    "low":    low_arr[idx]    if low_arr    else None,
                    "volume": vol_arr[idx]    if vol_arr    else None,
                }
                _ingest_bar(bars, int(ts_raw), indicator_id, val_arr[idx], ohlcv)

        else:
            # Unexpected shape — skip/ignore to avoid breaking the feed
            continue

    if not bars:
        raise RuntimeError("TAAPI response contained no usable timestamps; check API key & limits.")

    ordered = [{"ts": t, **vals} for t, vals in sorted(bars.items())[-LIMIT:]]
    last = ordered[-1]

    return {
        "timestamp": last["ts"],
        "datetime_utc": iso(last["ts"]),
        "symbol": PAIR.replace("/", ""),
        "granularity": TF,
        "price": last.get("close"),
        "high": last.get("high"),
        "low": last.get("low"),
        "vol": last.get("volume"),
        "ema20": last.get("ema20"),
        "ema50": last.get("ema50"),
        "ema200": last.get("ema200"),
        "rsi14": last.get("rsi14"),
        "vwap": last.get("vwap"),
        "atr14": last.get("atr14"),
        "last_candles": ordered,  # max 20 with Bulk
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
