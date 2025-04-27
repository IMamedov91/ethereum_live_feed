#!/usr/bin/env python3
"""
kucoin_btc_feed.py — v2.2
Haalt 15-min-indicatoren (EMA20/50/200, RSI14, ATR14, VWAP) via TAAPI.io
en uploadt de laatste candle + 300-bar snapshot naar je GitHub-gist.

⚠️ Let op TAAPI-beperkingen
--------------------------
* Bulk-endpoint retourneert **max. 20 candles** per indicator (parameter `results`).
* Wil je écht 300 candles? Loop dan 15× de *Direct*-endpoint of bewaar historische waarden lokaal.

Wijzigingen v2.2
----------------
* `addResultTimestamp=True` **per indicator** (construct-niveau werd genegeerd).
* `results=20` toegevoegd — zo krijg je wél een array om op te bouwen.
* Fallback op `timestampMs` en defensieve check voor lege data.
* Kleinere verbeteringen & duidelijke foutmelding.
"""

from __future__ import annotations
import os, time, json, requests, datetime as dt
from typing import Dict, List, Any

# ────────────────────────────────────────────────────────────────
# Configuratie uit environment
# ────────────────────────────────────────────────────────────────
SECRET     = os.getenv("TAAPI_SECRET")        # TAAPI-key
GIST_ID    = os.getenv("GIST_ID")             # GitHub-gist ID
GIST_TOKEN = os.getenv("GIST_TOKEN")          # GitHub-token

PAIR   = "BTC/USDT"  # trading pair
TF     = "15m"       # timeframe
LIMIT  = 300         # gewenste snapshotlengte (<=300)
TAAPI_URL = "https://api.taapi.io/bulk"

# ────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────

def iso(ms: int | float) -> str:
    """Unix-tijd in ms ⇒ ISO-8601 (UTC)."""
    return dt.datetime.utcfromtimestamp(ms / 1000).isoformat(timespec="seconds") + "Z"

# ────────────────────────────────────────────────────────────────
# TAAPI integratie
# ────────────────────────────────────────────────────────────────

def fetch_indicators() -> List[Dict[str, Any]]:
    """Bulk-request naar TAAPI — retourneert lijst van indicator-dicts."""
    indicators = [
        {"id": "ema20",  "indicator": "ema",  "optInTimePeriod": 20},
        {"id": "ema50",  "indicator": "ema",  "optInTimePeriod": 50},
        {"id": "ema200", "indicator": "ema",  "optInTimePeriod": 200},
        {"id": "rsi14",  "indicator": "rsi",  "optInTimePeriod": 14},
        {"id": "atr14",  "indicator": "atr",  "optInTimePeriod": 14},
        {"id": "vwap",   "indicator": "vwap", "anchorPeriod": "session"},
    ]
    # Forceer timestamp + maximaal 20 resultaten per indicator
    for ind in indicators:
        ind.update({"addResultTimestamp": True, "results": 20})

    payload = {
        "secret": SECRET,
        "construct": {
            "exchange": "binance",
            "symbol": PAIR,
            "interval": TF,
            "indicators": indicators,
        },
    }

    r = requests.post(TAAPI_URL, json=payload, timeout=10)
    r.raise_for_status()
    return r.json()["data"]

# ────────────────────────────────────────────────────────────────
# Data transformeren
# ────────────────────────────────────────────────────────────────

def reshape(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Bundelt indicator-arrays tot candle-snapshots."""
    bars: Dict[int, Dict[str, Any]] = {}

    for item in data:
        res_list = item["result"]
        # `result` is nu een LIST dankzij `results`-parameter
        if not isinstance(res_list, list):
            res_list = [res_list]
        for res in res_list:
            ts_raw = res.get("timestamp") or res.get("timestampMs")
            if ts_raw is None:
                continue  # skip als ontbreekt
            bar = int(ts_raw)
            cell = bars.setdefault(bar, {})
            cell[item["id"]] = res["value"]  # indicatorwaarde
            for k in ("open", "close", "high", "low", "volume"):
                cell.setdefault(k, res.get(k))

    if not bars:
        raise RuntimeError("TAAPI antwoord bevatte geen timestamps — controleer API-key & limieten.")

    # op tijdsortering & houd MAX laatste LIMIT candles
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
        "ema20": last.get("ema20"),
        "ema50"
