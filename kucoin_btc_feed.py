#!/usr/bin/env python3
"""
kucoin_btc_feed.py — v1.4
• 15-min futures-candles (XBTUSDTM) van KuCoin
• Indicator-set: EMA-20/50/200, RSI-14, VWAP, ATR-14, vol_mean20
• Vult ontbrekende indicator-waarden (geen null meer in JSON!)
• Bewaart de laatste 300 candles in een GitHub Gist
• JSON-output is nu ‘pretty-printed’ (meerdere regels) zodat GitHub-viewer
  de inhoud netjes toont
"""

from __future__ import annotations
import datetime as dt
import json, math, os, time, typing as t

import pandas as pd
import requests, ta

# ───────────────────────── CONFIG ──────────────────────────────────
SYMBOL_DEFAULT = os.getenv("SYMBOL", "XBTUSDTM")          # officiële BTC-USDT-perp
TF_MIN         = int(os.getenv("GRANULARITY", "15"))      # minuten
API_URL        = "https://api-futures.kucoin.com/api/v1/kline/query"
MAX_LIMIT      = 500                                      # KuCoin-limiet per call

FETCH_LEN    = 550   # 300 snapshot + buffer zodat indicatoren zeker compleet zijn
SNAPSHOT_LEN = 300
MS_PER_BAR   = TF_MIN * 60_000
FILE_DEFAULT = os.getenv("FILE_NAME", "btc_feed.json")

pd.options.mode.copy_on_write = True

# ──────────────────────── HELPERS ──────────────────────────────────
def _get(params: dict, retries: int = 3) -> list[list[t.Any]]:
    """GET-helper met eenvoudige retry-logica."""
    err: Exception | None = None
    for _ in range(retries):
        try:
            r = requests.get(API_URL, params=params, timeout=10)
            r.raise_for_status()
            return r.json()["data"]
        except Exception as e:
            err = e
            time.sleep(1)
    raise RuntimeError(f"KuCoin-API blijft falen: {err}") from err


def fetch_frame(symbol: str, tf_min: int) -> pd.DataFrame:
    """Haalt raw candles op en voegt indicator-kolommen toe (zonder null)."""
    raw: list[list[t.Any]] = []
    end_ms = int(time.time() * 1000)

    while len(raw) < FETCH_LEN:
        batch = _get(
            {
                "symbol": symbol,
                "granularity": tf_min,  # minuten!
                "limit": MAX_LIMIT,
                "to": end_ms,
            }
        )
        if not batch:
            break
        raw.extend(batch)
        end_ms = int(batch[-1][0]) - MS_PER_BAR

    cols = ["ts", "open", "close", "high", "low", "vol"]  # futures = 6 velden
    df = (
        pd.DataFrame(raw, columns=cols)
        .astype(float)
        .drop_duplicates("ts")
        .sort_values("ts")
        .reset_index(drop=True)
    )

    # ───────── indicatoren ─────────
    df["ema20"] = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"] = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"] = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"] = ta.momentum.rsi(df["close"], 14)
    df["vwap"] = ta.volume.volume_weighted_average_price(
        df["high"], df["low"], df["close"], df["vol"], window=14
    )
    df["atr14"] = ta.volatility.average_true_range(
        df["high"], df["low"], df["close"], 14
    )
    df["vol_mean20"] = df["vol"].rolling(20, min_periods=1).mean()

    # Alle NaN/inf vervangen door dichtstbijzijnde geldige waarde (geen null meer)
    ind_cols = ["ema20", "ema50", "ema200", "rsi14", "vwap", "atr14"]
    df[ind_cols] = df[ind_cols].ffill().bfill()

    return df.tail(SNAPSHOT_LEN).reset_index(drop=True)


# ─────────────────────── utilities ────────────────────────────────
iso = (
    lambda ms: dt.datetime.utcfromtimestamp(ms / 1000).isoformat(
        timespec="seconds"
    )
    + "Z"
)
_clean = (
    lambda v: None
    if (isinstance(v, float) and (math.isnan(v) or math.isinf(v)))
    else v
)
row = lambda r: {**{k: _clean(v) for k, v in r.items()}, "ts": int(r.ts)}


def push_gist(token: str, gist_id: str, fname: str, payload: dict) -> None:
    """Uploadt JSON-payload naar een Gist-bestand (met inspringing)."""
    json_content = json.dumps(
        payload,
        indent=2,              # prettify → meerdere regels
        allow_nan=False,
        ensure_ascii=False,    # UTF-8 tekens direct in file
    )
    requests.patch(
        f"https://api.github.com/gists/{gist_id}",
        headers={
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
        },
        json={"files": {fname: {"content": json_content}}},
        timeout=10,
    ).raise_for_status()


# ────────────────────────── MAIN ──────────────────────────────────
def main() -> None:
    gist_id = os.environ["GIST_ID"]
    token = os.environ["GIST_TOKEN"]

    df = fetch_frame(SYMBOL_DEFAULT, TF_MIN)
    last = df.iloc[-1]

    payload = {
        "timestamp": int(last.ts),
        "datetime_utc": iso(int(last.ts)),
        "symbol": SYMBOL_DEFAULT,
        "granularity": str(TF_MIN),
        "price": _clean(last.close),
        "high": _clean(last.high),
        "low": _clean(last.low),
        "vol": _clean(last.vol),
        "ema20": _clean(last.ema20),
        "ema50": _clean(last.ema50),
        "ema200": _clean(last.ema200),
        "rsi14": _clean(last.rsi14),
        "vwap": _clean(last.vwap),
        "atr14": _clean(last.atr14),
        "vol_mean20": _clean(last.vol_mean20),
        "last_300_candles": [row(r) for _, r in df.iterrows()],
        "funding_rate": None,
        "open_interest": None,
        "order_book": None,
        "generated_at": iso(int(time.time() * 1000)),
    }

    push_gist(token, gist_id, FILE_DEFAULT, payload)
    print("✅ BTC-feed geüpload:", payload["generated_at"])


if __name__ == "__main__":
    main()
