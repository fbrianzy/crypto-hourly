import json, os
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
import time

# ======= Config =======
TICKERS = ["BTC-USD", "ETH-USD"]
PERIOD   = "7d"
INTERVAL = "1h"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Prediksi sederhana berbasis momentum & SMA(12)
def simple_signal(close_series: pd.Series):
    if len(close_series) < 13:
        return "HOLD"
    momentum = close_series.iloc[-1] / close_series.iloc[-2] - 1
    sma12 = close_series.tail(12).mean()
    last_close = close_series.iloc[-1]
    return "UP" if (momentum > 0) or (last_close > sma12) else "DOWN"

def fetch_prices(ticker: str):
    """Fetch prices with retry logic"""
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Fetching {ticker} (attempt {attempt + 1}/{MAX_RETRIES})...")
            
            # Download dengan headers untuk menghindari blocking
            df = yf.download(
                ticker, 
                period=PERIOD, 
                interval=INTERVAL, 
                progress=False, 
                auto_adjust=True,
                timeout=30
            )
            
            if df.empty:
                print(f"Warning: Empty dataframe for {ticker}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                raise RuntimeError(f"No data for {ticker} after {MAX_RETRIES} attempts")
            
            df = df.reset_index()
            
            # Handle different column names
            if "Datetime" in df.columns:
                ts = df["Datetime"]
            elif "Date" in df.columns:
                ts = df["Date"]
            else:
                raise RuntimeError(f"No datetime column found for {ticker}")
            
            df["ts_utc"] = pd.to_datetime(ts, utc=True)
            
            # Verify we have Close data
            if "Close" not in df.columns:
                raise RuntimeError(f"No Close price data for {ticker}")
            
            print(f"✓ Successfully fetched {len(df)} rows for {ticker}")
            return df[["ts_utc", "Close"]]
            
        except Exception as e:
            print(f"Error fetching {ticker}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                raise RuntimeError(f"Failed to fetch {ticker} after {MAX_RETRIES} attempts: {str(e)}")

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

    print(f"Starting data fetch at {datetime.now(timezone.utc).isoformat()}")
    print(f"Tickers: {TICKERS}, Period: {PERIOD}, Interval: {INTERVAL}\n")

    for tk in TICKERS:
        try:
            df = fetch_prices(tk)
            all_series[tk] = to_records(df)
            latest_block[tk] = {
                "last_ts_utc": df["ts_utc"].iloc[-1].isoformat(),
                "last_close": float(df["Close"].iloc[-1]),
            }
            preds[tk] = simple_signal(df["Close"])
            print(f"✓ {tk}: {len(df)} records, prediction: {preds[tk]}\n")
        except Exception as e:
            print(f"✗ Failed to process {tk}: {str(e)}")
            raise  # Re-raise to fail the workflow

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

    print("\n✅ SUCCESS: data/*.json updated.")
    print(f"Generated at: {now_utc}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ FAILED: {str(e)}")
        exit(1)
