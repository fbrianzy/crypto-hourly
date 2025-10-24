import json, os
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone

# ======= Config =======
TICKERS = ["BTC-USD", "ETH-USD"]
PERIOD   = "7d"
INTERVAL = "1h"

# Prediksi sederhana berbasis momentum & SMA(12)
def simple_signal(close_series: pd.Series):
    if len(close_series) < 13:
        return "HOLD"
    momentum = close_series.iloc[-1] / close_series.iloc[-2] - 1
    sma12 = close_series.tail(12).mean()
    last_close = close_series.iloc[-1]
    return "UP" if (momentum > 0) or (last_close > sma12) else "DOWN"

def fetch_prices(ticker: str):
    df = yf.download(ticker, period=PERIOD, interval=INTERVAL, progress=False, auto_adjust=True)
    if df.empty:
        raise RuntimeError(f"No data for {ticker}")
    df = df.reset_index()
    ts = df["Datetime"] if "Datetime" in df.columns else df["Date"]
    df["ts_utc"] = pd.to_datetime(ts, utc=True)
    return df[["ts_utc", "Close"]]

def to_records(df: pd.DataFrame):
    return [
        {"ts_utc": t.isoformat(), "close": float(c)}
        for t, c in zip(df["ts_utc"], df["Close"])
    ]

def write_json(payload: dict, relpath: str):
    repo_path = os.path.join("data", relpath)
    os.makedirs(os.path.dirname(repo_path), exist_ok=True)
    with open(repo_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def main():
    all_series = {}
    latest_block = {}
    preds = {}

    for tk in TICKERS:
        df = fetch_prices(tk)
        all_series[tk] = to_records(df)
        latest_block[tk] = {
            "last_ts_utc": df["ts_utc"].iloc[-1].isoformat(),
            "last_close": float(df["Close"].iloc[-1]),
        }
        preds[tk] = simple_signal(df["Close"])

    now_utc = datetime.now(timezone.utc).isoformat()

    prices_payload = {
        "generated_at_utc": now_utc,
        "interval": INTERVAL,
        "period": PERIOD,
        "series": all_series,
        "latest": latest_block,
    }
    write_json(prices_payload, "prices.json")

    pred_payload = {
        "generated_at_utc": now_utc,
        "next_1h_prediction": preds,
        "method": "momentum_or_close_gt_SMA12",
        "note": "Simple rule-based signal using last-hour momentum and SMA(12)."
    }
    write_json(pred_payload, "prediction.json")

    print("OK: data/*.json updated.")

if __name__ == "__main__":
    main()
