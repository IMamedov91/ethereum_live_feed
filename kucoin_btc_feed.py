#!/usr/bin/env python3
"""
BTC Futures Feed – Donchian-Breakout Confluence
================================================
Fix v1.1 – TAAPI indicator-compat & closed-candle integrity
----------------------------------------------------------
* Switched Donchian call to officially supported `donchianchannels` endpoint.
* Added `backtrack=1` to Donchian + ATR to reference the last **closed** candle.
* Adjusted JSON parse → `value.upper / value.lower` keys (per TAAPI spec).
* Updated Workflow note: call `btc_donchian_feed.py` instead of legacy script.
"""

import os, json, datetime as dt, requests, sys, pathlib
from typing import Dict

# ── ENV ─────────────────────────────────────────────────────────────
TAAPI_SECRET = os.environ["TAAPI_SECRET"]
GIST_ID      = os.environ["GIST_ID"]
GIST_TOKEN   = os.environ["GIST_TOKEN"]

PAIR        = os.getenv("PAIR", "BTC/USDT")
LOW_TF      = os.getenv("LOW_TF", "15m")        # trigger timeframe
HIGH_TF     = os.getenv("HIGH_TF", "4h")         # trend filter timeframe
DON_PERIOD  = int(os.getenv("DON_PERIOD", "20"))  # Donchian length
ATR_MIN     = float(os.getenv("ATR_PCT_MIN", "0.003"))   # 0.3 %
EMA_SLOPE_EPS = float(os.getenv("EMA_SLOPE_EPS", "0.0"))  # optional flat-trend veto
FILE        = os.getenv("FILE_NAME", "btc_feed.json")
HISTDIR     = pathlib.Path("history_btc")

BASE = "https://api.taapi.io"
REQ  = requests.Session()

# ── Helpers ─────────────────────────────────────────────────────────

def build_body(interval: str, indicators) -> Dict:
    return {
        "secret": TAAPI_SECRET,
        "construct": {
            "exchange": "binance",
            "symbol": PAIR,
            "interval": interval,
            "indicators": indicators,
        },
    }


def fetch_low() -> Dict:
    """Lower-TF indicators: Donchian breakout & volatility gate."""
    ind = [
        {
            "id": "don",
            "indicator": "donchianchannels",
            "period": DON_PERIOD,
            "backtrack": 1,  # ensure closed-candle breakout
        },
        {"id": "atr", "indicator": "atr", "period": 14, "backtrack": 1},
        {"id": "price", "indicator": "price"},
    ]
    r = REQ.post(f"{BASE}/bulk", json=build_body(LOW_TF, ind), timeout=12)
    r.raise_for_status()
    data = {x["id"]: x["result"] for x in r.json()["data"]}
    return {
        "donHigh": data["don"]["value"]["upper"],
        "donLow":  data["don"]["value"]["lower"],
        "atr":     data["atr"]["value"],
        "price":   data["price"]["value"],
    }


def fetch_high() -> Dict:
    """Higher-TF trend filter (EMA-200 + slope)."""
    ind = [
        {"id": "ema200", "indicator": "ema", "period": 200, "backtrack": 0},
        {"id": "ema200prev", "indicator": "ema", "period": 200, "backtrack": 1},
    ]
    r = REQ.post(f"{BASE}/bulk", json=build_body(HIGH_TF, ind), timeout=12)
    r.raise_for_status()
    data = {x["id"]: x["result"] for x in r.json()["data"]}
    ema_now, ema_prev = data["ema200"]["value"], data["ema200prev"]["value"]
    return {
        "ema200": ema_now,
        "ema200Slope": (ema_now - ema_prev) / ema_prev if ema_prev else 0.0,
    }


# ── Decision Engine ─────────────────────────────────────────────────

def vol_gate(low: Dict) -> bool:
    return low["atr"] / low["price"] >= ATR_MIN


def decide(low: Dict, high: Dict) -> str:
    up   = low["price"] > high["ema200"]
    down = low["price"] < high["ema200"]
    slope_ok = abs(high["ema200Slope"]) >= EMA_SLOPE_EPS
    breakout_up   = low["price"] >= low["donHigh"]
    breakout_down = low["price"] <= low["donLow"]

    if vol_gate(low) and slope_ok:
        if up and breakout_up:
            return "long"
        if down and breakout_down:
            return "short"
    return "flat"


# ── Main │ History + Gist push ─────────────────────────────────────

def main():
    low  = fetch_low()
    high = fetch_high()
    bias = decide(low, high)

    reason = {
        "long":  "don-breakout-long",
        "short": "don-breakout-short",
        "flat":  "no-setup",
    }[bias]

    payload = {
        "symbol": PAIR.replace("/", ""),
        "timestamp": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "finalBias": bias,
        "biasReason": reason,
        "indicators": {**low, **high},
        "settings": dict(
            donPeriod=DON_PERIOD,
            atrMin=ATR_MIN,
            emaSlopeEps=EMA_SLOPE_EPS,
            lowTF=LOW_TF,
            highTF=HIGH_TF,
        ),
        "ttl_sec": 900,
    }

    HISTDIR.mkdir(exist_ok=True)
    (HISTDIR / f"{payload['timestamp']}.json").write_text(json.dumps(payload))

    body = {"files": {FILE: {"content": json.dumps(payload, indent=2)}}}
    r = REQ.patch(
        f"https://api.github.com/gists/{GIST_ID}",
        headers={
            "Authorization": f"token {GIST_TOKEN}",
            "Accept": "application/vnd.github+json",
        },
        json=body,
        timeout=12,
    )
    r.raise_for_status()

    print(r.json()["files"][FILE]["raw_url"])  # stdout → workflow output


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print("HTTP error:", e, file=sys.stderr)
        sys.exit(1)
