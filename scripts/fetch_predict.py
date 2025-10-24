import json
import os
import time
from datetime import datetime, timezone
import pandas as pd
import yfinance as yf

# ======= Config =======
TICKERS = ["BTC-USD", "ETH-USD"]
PERIOD = "7d"
INTERVAL = "1h"
MAX_RETRIES = 3
RETRY_DELAY = 5

def simple_signal(close_series):
    """Prediksi sederhana berbasis momentum & SMA(12)"""
    if len(close_series) < 13:
        return "HOLD"
    
    close_list = close_series.tolist() if hasattr(close_series, 'tolist') else list(close_series)
    momentum = close_list[-1] / close_list[-2] - 1
    sma12 = sum(close_list[-12:]) / 12
    last_close = close_list[-1]
    
    return "UP" if (momentum > 0) or (last_close > sma12) else "DOWN"

def fetch_prices_with_workaround(ticker):
    """
    Fetch dengan berbagai workaround untuk menghindari blocking
    """
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Fetching {ticker} (attempt {attempt + 1}/{MAX_RETRIES})...")
            
            # Buat Ticker object baru setiap kali (reset session)
            stock = yf.Ticker(ticker)
            
            # Method 1: Gunakan history() langsung dari Ticker object
            df = stock.history(period=PERIOD, interval=INTERVAL)
            
            # Jika gagal, coba method alternatif
            if df.empty:
                print(f"  Method 1 failed, trying alternative...")
                time.sleep(2)
                
                # Method 2: Gunakan download dengan prepost=False
                df = yf.download(
                    ticker,
                    period=PERIOD,
                    interval=INTERVAL,
                    progress=False,
                    prepost=False,
                    threads=False,  # Disable threading
                    ignore_tz=False
                )
            
            if df.empty:
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (attempt + 1)
                    print(f"  Empty data, waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise RuntimeError(f"No data for {ticker} after {MAX_RETRIES} attempts")
            
            # Process DataFrame
            df = df.reset_index()
            
            # Handle different datetime column names
            date_col = None
            for col in ['Datetime', 'Date', 'index']:
                if col in df.columns:
                    date_col = col
                    break
            
            if date_col is None:
                raise RuntimeError(f"Cannot find datetime column for {ticker}")
            
            df["ts_utc"] = pd.to_datetime(df[date_col], utc=True)
            
            # Verify Close column exists
            if "Close" not in df.columns:
                raise RuntimeError(f"No Close price data for {ticker}")
            
            # Remove any rows with NaN
            df = df.dropna(subset=['Close'])
            
            if len(df) == 0:
                raise RuntimeError(f"All Close prices are NaN for {ticker}")
            
            result_df = df[["ts_utc", "Close"]].copy()
            
            print(f"✓ Success: {len(result_df)} data points")
            print(f"  Range: {result_df['ts_utc'].iloc[0]} to {result_df['ts_utc'].iloc[-1]}")
            print(f"  Last price: ${result_df['Close'].iloc[-1]:,.2f}")
            
            return result_df
            
        except Exception as e:
            error_msg = str(e)
            print(f"  Error: {error_msg}")
            
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                print(f"  Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise RuntimeError(f"Failed to fetch {ticker}: {error_msg}")

def to_records(df):
    """Convert DataFrame to JSON records"""
    return [
        {
            "ts_utc": row["ts_utc"].isoformat(),
            "close": float(row["Close"])
        }
        for _, row in df.iterrows()
    ]

def write_json(payload, relpath):
    """Write JSON to file"""
    repo_path = os.path.join("data", relpath)
    os.makedirs(os.path.dirname(repo_path), exist_ok=True)
    with open(repo_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def main():
    all_series = {}
    latest_block = {}
    preds = {}

    print(f"\n{'='*60}")
    print(f"Starting data fetch at {datetime.now(timezone.utc).isoformat()}")
    print(f"Tickers: {TICKERS} | Period: {PERIOD} | Interval: {INTERVAL}")
    print(f"{'='*60}\n")

    for idx, ticker in enumerate(TICKERS):
        try:
            # Add delay between tickers to avoid rate limiting
            if idx > 0:
                print(f"\nWaiting 3 seconds before next ticker...")
                time.sleep(3)
            
            print(f"\n[{idx+1}/{len(TICKERS)}] Processing {ticker}:")
            print("-" * 40)
            
            df = fetch_prices_with_workaround(ticker)
            
            # Store series data
            all_series[ticker] = to_records(df)
            
            # Store latest data
            latest_block[ticker] = {
                "last_ts_utc": df["ts_utc"].iloc[-1].isoformat(),
                "last_close": float(df["Close"].iloc[-1])
            }
            
            # Generate prediction
            preds[ticker] = simple_signal(df["Close"])
            
            print(f"  Prediction: {preds[ticker]}")
            
        except Exception as e:
            print(f"\n✗ FAILED to process {ticker}: {str(e)}")
            raise

    now_utc = datetime.now(timezone.utc).isoformat()

    # Write prices.json
    prices_payload = {
        "generated_at_utc": now_utc,
        "interval": INTERVAL,
        "period": PERIOD,
        "series": all_series,
        "latest": latest_block
    }
    write_json(prices_payload, "prices.json")
    print(f"\n✓ Wrote prices.json")

    # Write prediction.json
    pred_payload = {
        "generated_at_utc": now_utc,
        "next_1h_prediction": preds,
        "method": "momentum_or_close_gt_SMA12",
        "note": "Simple rule-based signal using last-hour momentum and SMA(12)."
    }
    write_json(pred_payload, "prediction.json")
    print(f"✓ Wrote prediction.json")

    print(f"\n{'='*60}")
    print(f"✅ SUCCESS!")
    print(f"Generated at: {now_utc}")
    print(f"\nPredictions:")
    for ticker in TICKERS:
        print(f"  {ticker}: {preds[ticker]} (${latest_block[ticker]['last_close']:,.2f})")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n{'='*60}")
        print(f"❌ SCRIPT FAILED")
        print(f"Error: {str(e)}")
        print(f"{'='*60}\n")
        exit(1)
