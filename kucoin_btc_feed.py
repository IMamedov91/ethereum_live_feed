#!/usr/bin/env python3
"""
kucoin_btc_feed.py — v2.0
Haalt 15-min-indicatoren (EMA20/50/200, RSI14, ATR14, VWAP) via TAAPI.io
en uploadt de laatste candle + 300-bar snapshot naar je GitHub-gist.
"""

import time, os, json, requests, datetime as dt

SECRET = os.getenv("TAAPI_SECRET")                 # je TAAPI key
GIST_ID = os.getenv("GIST_ID")
GIST_TOKEN = os.getenv("GIST_TOKEN")

PAIR   = "BTC/USDT"
TF     = "15m"
LIMIT  = 300                                       # snapshotlengte

def iso(ms: int) -> str:
    return dt.datetime.utcfromtimestamp(ms/1000).isoformat(timespec="seconds") + "Z"

def fetch_indicators() -> dict:
    payload = {
        "secret": SECRET,
        "construct": {
            "exchange": "binance",
            "symbol":  PAIR,
            "interval": TF,
            "backtrack": LIMIT,            # laatste 300 candles
            "indicators": [
                {"id":"ema20","indicator":"ema","optInTimePeriod":20},
                {"id":"ema50","indicator":"ema","optInTimePeriod":50},
                {"id":"ema200","indicator":"ema","optInTimePeriod":200},
                {"id":"rsi14","indicator":"rsi","optInTimePeriod":14},
                {"id":"atr14","indicator":"atr","optInTimePeriod":14},
                {"id":"vwap","indicator":"vwap","anchorPeriod":"session"}
            ]
        }
    }
    r = requests.post("https://api.taapi.io/bulk", json=payload, timeout=10)
    r.raise_for_status()
    return r.json()["data"]                          # lijst van indicator-dicts

def reshape(data: list[dict]) -> dict:
    # TAAPI bulk retourneert iedere indicator apart; we combineren op index
    bars = {}
    for item in data:
        bar = int(item["result"]["timestamp"])
        bars.setdefault(bar, {})
        key = item["id"]                   # bv. 'ema20'
        bars[bar][key] = item["result"]["value"]
        # voeg OHLCV alleen één keer toe (wordt herhaald per indicator)
        for k in ("open","close","high","low","volume"):
            bars[bar].setdefault(k, item["result"].get(k))
    # sorteer en pak laatste LIMIT candles
    ordered = [ {"ts":ts, **vals} for ts, vals in sorted(bars.items())[-LIMIT:] ]
    last    = ordered[-1]
    return {
        "timestamp": last["ts"],
        "datetime_utc": iso(last["ts"]),
        "symbol": PAIR.replace("/",""),
        "granularity": TF,
        "price": last["close"],
        "high":  last["high"],
        "low":   last["low"],
        "vol":   last["volume"],
        "ema20": last["ema20"],
        "ema50": last["ema50"],
        "ema200":last["ema200"],
        "rsi14": last["rsi14"],
        "vwap":  last["vwap"],
        "atr14": last["atr14"],
        "last_300_candles": ordered,
        "funding_rate": None,
        "open_interest": None,
        "order_book": None,
        "generated_at": iso(int(time.time()*1000))
    }

def push_gist(payload: dict) -> None:
    headers = {
        "Authorization": f"token {GIST_TOKEN}",
        "Accept": "application/vnd.github+json"
    }
    requests.patch(
        f"https://api.github.com/gists/{GIST_ID}",
        headers=headers,
        json={"files":{"kucoin_btc_feed.json":{"content":json.dumps(payload, indent=2)}}},
        timeout=10
    ).raise_for_status()

def main():
    data = fetch_indicators()
    payload = reshape(data)
    push_gist(payload)
    print("✅ BTC TAAPI-feed geüpload:", payload["generated_at"])

if __name__ == "__main__":
    main()
