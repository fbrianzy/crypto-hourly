import json
import os
import time
from datetime import datetime, timezone
import requests
import pandas as pd

# ======= Config =======
COINS = {
    "BTC-USD": "BTC",
    "ETH-USD": "ETH"
}
MAX_RETRIES = 3
RETRY_DELAY = 3

def simple_signal(close_series):
    """Prediksi sederhana berbasis momentum & SMA(12)"""
    if len(close_series) < 13:
        return "HOLD"
    momentum = close_series[-1] / close_series[-2] - 1
    sma12 = sum(close_series[-12:]) / 12
    last_close = close_series[-1]
    return "UP" if (momentum > 0) or (last_close > sma12) else "DOWN"

def fetch_cryptocompare_hourly(coin_symbol):
    """
    Fetch hourly data dari CryptoCompare (gratis, no API key)
    Endpoint: histohour (2000 hours limit, kita ambil 168 = 7 days)
    """
    url = "https://min-api.cryptocompare.com/data/v2/histohour"
    
    params = {
        "fsym": coin_symbol,  # From Symbol (BTC, ETH)
        "tsym": "USD",        # To Symbol (USD)
        "limit": 168          # 7 days * 24 hours
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Fetching {coin_symbol} from CryptoCompare (attempt {attempt + 1}/{MAX_RETRIES})...")
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            
            # Check response status
            if result.get("Response") == "Error":
                raise ValueError(f"API Error: {result.get('Message', 'Unknown error')}")
            
            # Get data array
            data_array = result.get("Data", {}).get("Data", [])
            
            if not data_array:
                raise ValueError(f"No data returned for {coin_symbol}")
            
            # Parse data
            records = []
            for item in data_array:
                timestamp = item.get("time")
                close_price = item.get("close")
                
                if timestamp and close_price:
                    records.append({
                        "timestamp": timestamp,
                        "close": float(close_price)
                    })
            
            if not records:
                raise ValueError(f"No valid records for {coin_symbol}")
            
            # Convert to DataFrame
            df = pd.DataFrame(records)
            df["ts_utc"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
            
            # Sort by time
            df = df.sort_values("ts_utc").reset_index(drop=True)
            
            print(f"✓ Successfully fetched {len(df)} hourly data points")
            print(f"  Range: {df['ts_utc'].iloc[0]} to {df['ts_utc'].iloc[-1]}")
            print(f"  Last price: ${df['close'].iloc[-1]:,.2f}")
            
            return df[["ts_utc", "close"]]
            
        except requests.exceptions.RequestException as e:
            print(f"  Request error: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                print(f"  Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                raise RuntimeError(f"Failed to fetch {coin_symbol} after {MAX_RETRIES} attempts")
        except Exception as e:
            print(f"  Error: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                raise

def to_records(df):
    """Convert DataFrame to JSON records"""
    return [
        {
            "ts_utc": row["ts_utc"].isoformat(),
            "close": float(row["close"])
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

    print(f"\n{'='*70}")
    print(f"Crypto Hourly Data Fetcher")
    print(f"Started at: {datetime.now(timezone.utc).isoformat()}")
    print(f"Source: CryptoCompare API (min-api.cryptocompare.com)")
    print(f"{'='*70}\n")

    for idx, (ticker, coin_symbol) in enumerate(COINS.items()):
        try:
            if idx > 0:
                print(f"\nWaiting 2 seconds before next request...")
                time.sleep(2)
            
            print(f"\n[{idx+1}/{len(COINS)}] {ticker} ({coin_symbol}):")
            print("-" * 50)
            
            df = fetch_cryptocompare_hourly(coin_symbol)
            
            # Store series data
            all_series[ticker] = to_records(df)
            
            # Store latest data
            latest_block[ticker] = {
                "last_ts_utc": df["ts_utc"].iloc[-1].isoformat(),
                "last_close": float(df["close"].iloc[-1])
            }
            
            # Generate prediction
            close_list = df["close"].tolist()
            preds[ticker] = simple_signal(close_list)
            
            print(f"  Prediction: {preds[ticker]}")
            
        except Exception as e:
            print(f"\n✗ FAILED to process {ticker}: {str(e)}")
            raise

    now_utc = datetime.now(timezone.utc).isoformat()

    # Write prices.json
    prices_payload = {
        "generated_at_utc": now_utc,
        "interval": "1h",
        "period": "7d",
        "series": all_series,
        "latest": latest_block
    }
    write_json(prices_payload, "prices.json")
    print(f"\n✓ Wrote: data/prices.json")

    # Write prediction.json
    pred_payload = {
        "generated_at_utc": now_utc,
        "next_1h_prediction": preds,
        "method": "momentum_or_close_gt_SMA12",
        "note": "Simple rule-based signal using last-hour momentum and SMA(12)."
    }
    write_json(pred_payload, "prediction.json")
    print(f"✓ Wrote: data/prediction.json")

    print(f"\n{'='*70}")
    print(f"✅ SUCCESS!")
    print(f"Generated at: {now_utc}")
    print(f"\nPredictions:")
    for ticker in COINS.keys():
        price = latest_block[ticker]['last_close']
        pred = preds[ticker]
        print(f"  {ticker}: {pred:5s} (${price:>10,.2f})")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n{'='*70}")
        print(f"❌ SCRIPT FAILED")
        print(f"Error: {str(e)}")
        print(f"{'='*70}\n")
        exit(1)
