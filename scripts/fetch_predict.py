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

# Discord
WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK", "")

# Coin meta for embeds
COIN_META = {
    "BTC-USD": {
        "name": "Bitcoin",
        "symbol": "BTC",
        "emoji": "🟡",
        "color": 0xF7931A,  # Bitcoin orange
        "icon": "https://s2.coinmarketcap.com/static/img/coins/64x64/1.png",
    },
    "ETH-USD": {
        "name": "Ethereum",
        "symbol": "ETH",
        "emoji": "🔵",
        "color": 0x627EEA,  # Ethereum blue
        "icon": "https://s2.coinmarketcap.com/static/img/coins/64x64/1027.png",
    },
}

SIGNAL_META = {
    "UP":   {"emoji": "📈", "label": "UP",   "color_hex": 0x10B981},
    "DOWN": {"emoji": "📉", "label": "DOWN", "color_hex": 0xEF4444},
    "HOLD": {"emoji": "➡️", "label": "HOLD", "color_hex": 0xF59E0B},
}


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
        "fsym": coin_symbol,
        "tsym": "USD",
        "limit": 168
    }

    for attempt in range(MAX_RETRIES):
        try:
            print(f"Fetching {coin_symbol} from CryptoCompare (attempt {attempt + 1}/{MAX_RETRIES})...")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            result = response.json()

            if result.get("Response") == "Error":
                raise ValueError(f"API Error: {result.get('Message', 'Unknown error')}")

            data_array = result.get("Data", {}).get("Data", [])
            if not data_array:
                raise ValueError(f"No data returned for {coin_symbol}")

            records = []
            for item in data_array:
                timestamp = item.get("time")
                close_price = item.get("close")
                if timestamp and close_price:
                    records.append({"timestamp": timestamp, "close": float(close_price)})

            if not records:
                raise ValueError(f"No valid records for {coin_symbol}")

            df = pd.DataFrame(records)
            df["ts_utc"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
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
    return [
        {"ts_utc": row["ts_utc"].isoformat(), "close": float(row["close"])}
        for _, row in df.iterrows()
    ]


def write_json(payload, relpath):
    repo_path = os.path.join("data", relpath)
    os.makedirs(os.path.dirname(repo_path), exist_ok=True)
    with open(repo_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# ─────────────────────────────────────────────
#  Discord Webhook
# ─────────────────────────────────────────────

def _price_change(df):
    """Return change % vs 1 hour ago."""
    if len(df) < 2:
        return 0.0
    return (df["close"].iloc[-1] / df["close"].iloc[-2] - 1) * 100


def _price_change_24h(df):
    """Return change % vs ~24 data points ago."""
    if len(df) < 25:
        return None
    return (df["close"].iloc[-1] / df["close"].iloc[-25] - 1) * 100


def _sma(df, n=12):
    if len(df) < n:
        return None
    return sum(df["close"].tolist()[-n:]) / n


def _high_low_24h(df):
    recent = df["close"].tail(24)
    return recent.max(), recent.min()


def build_coin_embed(ticker, df, signal, generated_at):
    meta = COIN_META[ticker]
    sig  = SIGNAL_META.get(signal, SIGNAL_META["HOLD"])

    last_close  = df["close"].iloc[-1]
    chg_1h      = _price_change(df)
    chg_24h     = _price_change_24h(df)
    sma12       = _sma(df, 12)
    high24, low24 = _high_low_24h(df)

    # Determine embed color: coin color normally, green/red on signal
    embed_color = sig["color_hex"]

    # Format helpers
    def fmt_usd(v):
        return f"${v:,.2f}"

    def fmt_pct(v):
        sign = "+" if v >= 0 else ""
        return f"{sign}{v:.2f}%"

    chg_1h_str  = fmt_pct(chg_1h)
    chg_24h_str = fmt_pct(chg_24h) if chg_24h is not None else "N/A"

    # Trend arrow for 1h change
    arrow_1h  = "▲" if chg_1h  >= 0 else "▼"
    arrow_24h = "▲" if (chg_24h or 0) >= 0 else "▼"

    # Build embed
    embed = {
        "author": {
            "name": f"{meta['emoji']} {meta['name']} ({meta['symbol']}/USD)",
            "icon_url": meta["icon"],
        },
        "color": embed_color,
        "fields": [
            {
                "name": "💰 Harga Sekarang",
                "value": f"```\n{fmt_usd(last_close)}\n```",
                "inline": True,
            },
            {
                "name": f"{sig['emoji']} Prediksi 1 Jam",
                "value": f"```\n{sig['label']}\n```",
                "inline": True,
            },
            {
                "name": "\u200b",
                "value": "\u200b",
                "inline": False,
            },
            {
                "name": "📊 Perubahan 1 Jam",
                "value": f"`{arrow_1h} {chg_1h_str}`",
                "inline": True,
            },
            {
                "name": "📅 Perubahan 24 Jam",
                "value": f"`{arrow_24h} {chg_24h_str}`",
                "inline": True,
            },
            {
                "name": "\u200b",
                "value": "\u200b",
                "inline": False,
            },
            {
                "name": "🔺 High 24H",
                "value": f"`{fmt_usd(high24)}`",
                "inline": True,
            },
            {
                "name": "🔻 Low 24H",
                "value": f"`{fmt_usd(low24)}`",
                "inline": True,
            },
            {
                "name": "📐 SMA(12)",
                "value": f"`{fmt_usd(sma12)}`" if sma12 else "`N/A`",
                "inline": True,
            },
        ],
        "footer": {
            "text": f"CryptoCompare · {generated_at}  •  Metode: momentum_or_close_gt_SMA12",
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return embed


def send_discord_webhook(all_data, preds, generated_at):
    """Send a rich Discord embed for each coin."""
    if not WEBHOOK_URL:
        print("⚠️  CRYPTO_WEBHOOK_DOLENCORD not set — skipping webhook.")
        return

    now_str = datetime.fromisoformat(generated_at).strftime("%d %b %Y, %H:%M UTC")

    embeds = []
    for ticker, df in all_data.items():
        signal = preds.get(ticker, "HOLD")
        embed  = build_coin_embed(ticker, df, signal, now_str)
        embeds.append(embed)

    # Overall summary line
    summary_parts = []
    for ticker, signal in preds.items():
        meta = COIN_META[ticker]
        sig  = SIGNAL_META.get(signal, SIGNAL_META["HOLD"])
        last = all_data[ticker]["close"].iloc[-1]
        summary_parts.append(
            f"{meta['emoji']} **{meta['symbol']}** `${last:,.2f}` → {sig['emoji']} **{signal}**"
        )

    content = (
        "## 🕐 Crypto Hourly Update\n"
        + "  ·  ".join(summary_parts)
        + f"\n-# Auto-update · {now_str}"
    )

    payload = {
        "content": content,
        "embeds": embeds,
        "username": "CryptoBot 🤖",
        "avatar_url": "https://s2.coinmarketcap.com/static/img/coins/64x64/1.png",
    }

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(
                WEBHOOK_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            if resp.status_code in (200, 204):
                print("✓ Discord webhook sent successfully.")
                return
            else:
                print(f"  Webhook returned {resp.status_code}: {resp.text}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    print("⚠️  Webhook failed after all retries — continuing anyway.")
        except requests.exceptions.RequestException as e:
            print(f"  Webhook request error: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                print("⚠️  Webhook failed after all retries — continuing anyway.")


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

def main():
    all_series   = {}
    all_dfs      = {}          # keep raw DataFrames for webhook
    latest_block = {}
    preds        = {}

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

            all_dfs[ticker]      = df
            all_series[ticker]   = to_records(df)
            latest_block[ticker] = {
                "last_ts_utc": df["ts_utc"].iloc[-1].isoformat(),
                "last_close":  float(df["close"].iloc[-1])
            }

            close_list   = df["close"].tolist()
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
        "period":   "7d",
        "series":   all_series,
        "latest":   latest_block,
    }
    write_json(prices_payload, "prices.json")
    print(f"\n✓ Wrote: data/prices.json")

    # Write prediction.json
    pred_payload = {
        "generated_at_utc":    now_utc,
        "next_1h_prediction":  preds,
        "method":              "momentum_or_close_gt_SMA12",
        "note":                "Simple rule-based signal using last-hour momentum and SMA(12)."
    }
    write_json(pred_payload, "prediction.json")
    print(f"✓ Wrote: data/prediction.json")

    # Send Discord webhook
    print(f"\nSending Discord webhook...")
    send_discord_webhook(all_dfs, preds, now_utc)

    print(f"\n{'='*70}")
    print(f"✅ SUCCESS!")
    print(f"Generated at: {now_utc}")
    print(f"\nPredictions:")
    for ticker in COINS.keys():
        price = latest_block[ticker]['last_close']
        pred  = preds[ticker]
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
