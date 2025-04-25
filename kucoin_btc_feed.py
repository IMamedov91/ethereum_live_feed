#!/usr/bin/env python3
"""
kucoin_btc_feed.py — v1.3
•  download 15‑minute candles for KuCoin BTC perpetual (XBTUSDTM)
•  compute EMA‑20/50/200, RSI‑14, VWAP, ATR‑14, vol_mean20
•  keep the latest 300 candles and push snapshot to a GitHub Gist
"""

from __future__ import annotations
import datetime as dt
import json
import math
import os
import time
import typing as t

import pandas as pd
import requests
import ta

# ───────────────────────── CONFIG ────────────────────────────────
SYMBOL: str = os.getenv("SYMBOL", "XBTUSDTM")            # KuCoin BTC perpetual symbol
TF_MIN: int = int(os.getenv("GRANULARITY", "15"))        # timeframe in minutes
API_URL: str = "https://api-futures.kucoin.com/api/v1/kline/query"
MAX_LIMIT: int = 500                                       # KuCoin futures API hard‑limit

FETCH_LEN: int = 550                                       # ≥300 candles + buffer
SNAPSHOT_LEN: int = 300
MS_PER_BAR: int = TF_MIN * 60_000
FILE_NAME: str = os.getenv("FILE_NAME", "btc_feed.json")

pd.options.mode.copy_on_write = True  # silence pandas copy warnings

# ───────────────────────── HELPERS ───────────────────────────────

def _get(params: dict[str, t.Any], retries: int = 3) -> list[list[t.Any]]:
    """Wrapper around requests.get with very simple retry logic."""
    err: Exception | None = None
    for _ in range(retries):
        try:
            r = requests.get(API_URL, params=params, timeout=10)
            r.raise_for_status()
            return r.json()["data"]
        except Exception as exc:
            err = exc
            time.sleep(1)
    raise RuntimeError(f"KuCoin API bleef falen: {err}") from err


def fetch_frame(symbol: str, tf_min: int) -> pd.DataFrame:
    """Return a DataFrame with ≥200 raw candles and all indicators added."""
    raw: list[list[t.Any]] = []
    end_ms: int = int(time.time() * 1000)

    # Pull history until we have at least 200 rows (for a valid EMA‑200)
    # AND the exchange returns less than the max page size (no more data).
    while True:
        batch = _get({
            "symbol": symbol,
            "granularity": tf_min,   # KuCoin futures expects minutes here
            "limit": MAX_LIMIT,
            "to": end_ms
        })

        if not batch:
            break

        raw.extend(batch)

        # stop if we already have ≥200 rows AND this batch was not full → no older data
        if len(raw) >= 200 and len(batch) < MAX_LIMIT:
            break

        # otherwise continue fetching older data
        end_ms = int(batch[-1][0]) - MS_PER_BAR

    # ----- build DataFrame -----
    cols = ["ts", "open", "close", "high", "low", "vol"]  # futures stream has 6 columns
    df = (pd.DataFrame(raw, columns=cols)
          .astype(float)
          .drop_duplicates("ts")
          .sort_values("ts")
          .reset_index(drop=True))

    # ----- indicators -----
    df["ema20"] = ta.trend.ema_indicator(df["close"], 20)
    df["ema50"] = ta.trend.ema_indicator(df["close"], 50)
    df["ema200"] = ta.trend.ema_indicator(df["close"], 200)
    df["rsi14"] = ta.momentum.rsi(df["close"], 14)
    df["vwap"] = ta.volume.volume_weighted_average_price(
        df["high"], df["low"], df["close"], df["vol"], 14)
    df["atr14"] = ta.volatility.average_true_range(
        df["high"], df["low"], df["close"], 14)
    df["vol_mean20"] = df["vol"].rolling(20, min_periods=1).mean()

    return df.tail(SNAPSHOT_LEN).reset_index(drop=True)

# ───────────────────────── UTILITIES ─────────────────────────────

def iso(ms: int) -> str:
    return dt.datetime.utcfromtimestamp(ms / 1000).isoformat(timespec="seconds") + "Z"


def _clean(v: float | int) -> float | int | None:
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def row(r: pd.Series) -> dict[str, t.Any]:
    return {**{k: _clean(v) for k, v in r.items()}, "ts": int(r.ts)}


def push_gist(token: str, gist_id: str, fname: str, payload: dict[str, t.Any]) -> None:
    requests.patch(
        f"https://api.github.com/gists/{gist_id}",
        headers={
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
        },
        json={"files": {fname: {"content": json.dumps(payload, separators=(",", ":"), allow_nan=False)}}},
        timeout=10,
    ).raise_for_status()

# ───────────────────────── MAIN ──────────────────────────────────

def main() -> None:
    gist_id: str = os.environ["GIST_ID"]
    token: str = os.environ["GIST_TOKEN"]

    df = fetch_frame(SYMBOL, TF_MIN)
    last = df.iloc[-1]

    payload: dict[str, t.Any] = {
        "timestamp": int(last.ts),
        "datetime_utc": iso(int(last.ts)),
        "symbol": SYMBOL,
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

    push_gist(token, gist_id, FILE_NAME, payload)
    print("✅ BTC feed geüpload:", payload["generated_at"])


if __name__ == "__main__":
    main()
