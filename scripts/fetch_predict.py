import json
import os
import time
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd

# ======= Config =======
COINS = {
    "BTC-USD": "bitcoin",
    "ETH-USD": "ethereum"
}
DAYS = 7  # 7 hari data
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

def fetch_coingecko_data(coin_id, days=7):
    """Fetch data dari CoinGecko API"""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": "usd",
        "days": days,
        "interval": "hourly"
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Fetching {coin_id} from CoinGecko (attempt {attempt + 1}/{MAX_RETRIES})...")
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if "prices" not in data or not data["prices"]:
                raise ValueError(f"No price data for {coin_id}")
            
            # Convert to DataFrame
            df = pd.DataFrame(data["prices"], columns=["timestamp_ms", "close"])
            df["ts_utc"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
            
            # Sort by time
            df = df.sort_values("ts_utc").reset_index(drop=True)
            
            print(f"✓ Successfully fetched {len(df)} hourly data points for {coin_id}")
            return df[["ts_utc", "close"]]
            
        except requests.exceptions.RequestException as e:
            print(f"Request error for {coin_id}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                raise RuntimeError(f"Failed to fetch {coin_id} after {MAX_RETRIES} attempts")
        except Exception as e:
            print(f"Error processing {coin_id}: {str(e)}")
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

    print(f"Starting data fetch at {datetime.now(timezone.utc).isoformat()}")
    print(f"Using CoinGecko API for: {list(COINS.keys())}\n")

    for ticker, coin_id in COINS.items():
        try:
            df = fetch_coingecko_data(coin_id, days=DAYS)
            
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
            
            print(f"✓ {ticker}: {len(df)} records, Last price: ${df['close'].iloc[-1]:,.2f}, Prediction: {preds[ticker]}")
            
            # Rate limiting: delay between requests
            time.sleep(1.5)
            
        except Exception as e:
            print(f"✗ Failed to process {ticker}: {str(e)}")
            raise

    now_utc = datetime.now(timezone.utc).isoformat()

    # Write prices.json
    prices_payload = {
        "generated_at_utc": now_utc,
        "interval": "1h",
        "period": f"{DAYS}d",
        "series": all_series,
        "latest": latest_block
    }
    write_json(prices_payload, "prices.json")

    # Write prediction.json
    pred_payload = {
        "generated_at_utc": now_utc,
        "next_1h_prediction": preds,
        "method": "momentum_or_close_gt_SMA12",
        "note": "Simple rule-based signal using last-hour momentum and SMA(12)."
    }
    write_json(pred_payload, "prediction.json")

    print(f"\n✅ SUCCESS: data/*.json updated")
    print(f"Generated at: {now_utc}")
    print(f"BTC Prediction: {preds['BTC-USD']}")
    print(f"ETH Prediction: {preds['ETH-USD']}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ FAILED: {str(e)}")
        exit(1)
