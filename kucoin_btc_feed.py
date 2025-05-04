#!/usr/bin/env python3
"""
BTC feed – 15 m confluence (EMA, MACD, RSI) zonder funding-filter.
Print de commit-hash raw-URL zodat je ‘m in Automations kunt gebruiken.
"""

import os, json, datetime as dt, requests, sys, time, pathlib

# ── ENV ─────────────────────────────────────────────────────────────
TAAPI_SECRET = os.environ["TAAPI_SECRET"]
GIST_ID      = os.environ["GIST_ID"]
GIST_TOKEN   = os.environ["GIST_TOKEN"]

PAIR   = os.getenv("PAIR", "BTC/USDT")
TF     = os.getenv("TF", "15m")
ATR_MIN = float(os.getenv("ATR_PCT_MIN", "0.003"))     # 0.3 %
MACD_EPS= float(os.getenv("MACD_EPS", "5")) / 10000    # 0.0005 default
RSI_H   = float(os.getenv("RSI_HIGH", "55"))
RSI_L   = float(os.getenv("RSI_LOW",  "45"))
FILE    = os.getenv("FILE_NAME", "btc_feed.json")
HISTDIR = pathlib.Path("history_btc")

BASE = "https://api.taapi.io"
REQ  = requests.Session()

# ── Bulk body ───────────────────────────────────────────────────────
def bulk_body():
    return {
        "secret": TAAPI_SECRET,
        "construct": {
            "exchange": "binance",
            "symbol": PAIR,
            "interval": TF,
            "indicators": [
                {"id":"ema50",  "indicator":"ema", "period":50},
                {"id":"ema200", "indicator":"ema", "period":200},
                {"id":"rsi",    "indicator":"rsi", "period":14},
                {"id":"macd",   "indicator":"macd"},
                {"id":"atr",    "indicator":"atr", "period":14},
                {"id":"price",  "indicator":"price"}
            ]
        }
    }

def fetch():
    r = REQ.post(f"{BASE}/bulk", json=bulk_body(), timeout=12)
    r.raise_for_status()
    res = {x["id"]: x["result"] for x in r.json()["data"]}
    return {
        "ema50":  res["ema50"]["value"],
        "ema200": res["ema200"]["value"],
        "rsi":    res["rsi"]["value"],
        "macd":   res["macd"]["valueMACDHist"],
        "atr":    res["atr"]["value"],
        "price":  res["price"]["value"]
    }

def decide(d):
    up, down = d["ema50"] > d["ema200"], d["ema50"] < d["ema200"]
    bull, bear = d["macd"] > MACD_EPS, d["macd"] < -MACD_EPS
    if up   and bull and d["rsi"] > RSI_H: return "long"
    if down and bear and d["rsi"] < RSI_L: return "short"
    return "flat"

def vol_ok(d): return d["atr"]/d["price"] >= ATR_MIN

def main():
    d = fetch()
    bias = decide(d) if vol_ok(d) else "flat"
    reason = "vol-gate" if bias=="flat" and not vol_ok(d) else f"{bias} confirmed" if bias!="flat" else "no confluence"

    payload = {
        "symbol": PAIR.replace("/",""),
        "timestamp": dt.datetime.utcnow().isoformat(timespec="seconds")+"Z",
        "finalBias": bias,
        "biasReason": reason,
        "indicators": d,
        "settings": dict(atrMin=ATR_MIN, macdEps=MACD_EPS, rsiHigh=RSI_H, rsiLow=RSI_L),
        "ttl_sec": 900
    }

    # history
    HISTDIR.mkdir(exist_ok=True)
    (HISTDIR / f"{payload['timestamp']}.json").write_text(json.dumps(payload))

    # push to gist
    body = {"files": {FILE: {"content": json.dumps(payload, indent=2)}}}
    r = REQ.patch(f"https://api.github.com/gists/{GIST_ID}",
                  headers={"Authorization":f"token {GIST_TOKEN}",
                           "Accept":"application/vnd.github+json"},
                  json=body, timeout=12)
    r.raise_for_status()

    print(r.json()["files"][FILE]["raw_url"])   # stdout → workflow output

if __name__ == "__main__":
    try: main()
    except requests.HTTPError as e:
        print("HTTP error:", e, file=sys.stderr); sys.exit(1)
