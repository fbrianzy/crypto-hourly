import io
import json
import os
import time
import textwrap
from datetime import datetime, timezone

import cairosvg
import requests
import pandas as pd
import numpy as np
import joblib

# ═══════════════════════════════════════════════
#  Config
# ═══════════════════════════════════════════════
COINS = {
    "BTC-USD": "BTC",
    "ETH-USD": "ETH",
}
MAX_RETRIES  = 3
RETRY_DELAY  = 3
WEBHOOK_URL  = os.environ.get("DISCORD_WEBHOOK", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
COINDESK_API_KEY = os.environ.get("COINDESK_API_KEY", "")

COIN_META = {
    "BTC-USD": {"name": "Bitcoin",  "symbol": "BTC", "hex": "#F7931A", "icon": "BTC"},
    "ETH-USD": {"name": "Ethereum", "symbol": "ETH", "hex": "#627EEA", "icon": "ETH"},
}
SIGNAL_STYLE = {
    "UP":   {"label": "UP",   "bg": "#10B981", "fg": "#ffffff"},
    "DOWN": {"label": "DOWN", "bg": "#EF4444", "fg": "#ffffff"},
    "HOLD": {"label": "HOLD", "bg": "#F59E0B", "fg": "#ffffff"},
}

# Model paths — loaded once at startup
MODEL_DIR = os.path.join(os.path.dirname(__file__), "..", "model")

_models = {}
_thresholds = {}
_forecast_models = {}

# BTC features (extended_features=False): 29 features excluding Date & Target
BTC_FEATURES = [
    "Open", "High", "Low", "Close", "Volume",
    "RSI_14", "EMA_12", "EMA_26", "ATR_14",
    "BB_Upper", "BB_Middle", "BB_Lower", "Dist_BB_Upper", "Dist_BB_Lower",
    "MA50_1D", "Dist_MA50_1D",
    "Close_Lag_1", "Close_Lag_2", "Close_Lag_3",
    "Return_1H", "Return_2H", "Return_3H",
    "EMA_Spread", "ATR_Ratio", "BB_Position", "Volatility_24H",
    "Volume_MA20", "Volume_Ratio",
    "Trend_1D",
]

# ETH features (extended_features=True): 37 features
ETH_FEATURES = BTC_FEATURES + [
    "Dist_EMA12", "Dist_EMA26",
    "Trend_Strength",
    "RSI_Overbought", "RSI_Oversold",
    "Candle_Range", "Body_Size",
    "Volume_Change",
]

ASSET_FEATURES = {
    "BTC-USD": BTC_FEATURES,
    "ETH-USD": ETH_FEATURES,
}


def load_models():
    """Load XGBoost models and optimal thresholds from disk."""
    for ticker in COINS:
        model_path     = os.path.join(MODEL_DIR, f"xgb_tuned_{ticker}.pkl")
        threshold_path = os.path.join(MODEL_DIR, f"threshold_{ticker}.pkl")

        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model not found: {model_path}")
        if not os.path.exists(threshold_path):
            raise FileNotFoundError(f"Threshold not found: {threshold_path}")

        _models[ticker]     = joblib.load(model_path)
        _thresholds[ticker] = float(joblib.load(threshold_path))
        print(f"  Loaded model: {ticker}  threshold={_thresholds[ticker]:.2f}")

        # Forecast model (optional — skip jika tidak ada)
        forecast_path = os.path.join(MODEL_DIR, f"xgb_forecast_{ticker}.pkl")
        if os.path.exists(forecast_path):
            _forecast_models[ticker] = joblib.load(forecast_path)
            print(f"  Loaded forecast model: {ticker}")
        else:
            print(f"  No forecast model for {ticker} — skipping")


# ═══════════════════════════════════════════════
#  Data fetching
# ═══════════════════════════════════════════════
def fetch_cryptocompare_hourly(coin_symbol: str) -> pd.DataFrame:
    """
    Fetch 168 candles dari CoinDesk Data API (spot OHLCV hourly).
    Mencoba beberapa market sebagai fallback.
    Returns DataFrame with columns: ts_utc, open, high, low, close, volume
    """
    MARKETS_TO_TRY = ["coinbase", "kraken", "bitstamp", "gemini"]

    headers = {
        "authorization": f"Apikey {COINDESK_API_KEY}",
    }

    last_error = None
    for market in MARKETS_TO_TRY:
        url    = "https://data-api.coindesk.com/spot/v1/historical/hours"
        params = {
            "market":     market,
            "instrument": f"{coin_symbol}-USD",
            "limit":      1300,  # MA50_1D needs 50d × 24h = 1200 candles + buffer
            "groups":     "OHLC,VOLUME",
        }

        for attempt in range(MAX_RETRIES):
            try:
                print(f"  Fetching {coin_symbol} [{market}] (attempt {attempt+1}/{MAX_RETRIES})...")
                r = requests.get(url, params=params, headers=headers, timeout=30)

                if r.status_code == 400:
                    body = r.json()
                    err_msg = body.get("Err", {}).get("message", r.text[:200])
                    print(f"  400 on [{market}]: {err_msg}")
                    last_error = err_msg
                    break

                r.raise_for_status()
                result = r.json()

                err = result.get("Err", {})
                if err and err.get("message"):
                    raise ValueError(f"API error: {err.get('message')}")

                data_list = result.get("Data", [])
                if not data_list:
                    raise ValueError("No records returned")

                rows = []
                for d in data_list:
                    ts    = d.get("TIMESTAMP")
                    close = d.get("CLOSE")
                    open_ = d.get("OPEN")
                    high  = d.get("HIGH")
                    low   = d.get("LOW")
                    vol   = d.get("VOLUME", 0)
                    if ts and close:
                        rows.append({
                            "timestamp": ts,
                            "open":  float(open_ or close),
                            "high":  float(high  or close),
                            "low":   float(low   or close),
                            "close": float(close),
                            "volume": float(vol or 0),
                        })

                if not rows:
                    raise ValueError("No valid rows after parsing")

                df = pd.DataFrame(rows)
                df["ts_utc"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
                df = df.sort_values("ts_utc").reset_index(drop=True)
                print(f"  OK  {len(df)} candles [{market}] | last: ${df['close'].iloc[-1]:,.2f}")
                return df[["ts_utc", "open", "high", "low", "close", "volume"]]

            except requests.RequestException as e:
                print(f"  Request error [{market}]: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    last_error = str(e)
                    break
            except ValueError as e:
                print(f"  Parse error [{market}]: {e}")
                last_error = str(e)
                break

    raise RuntimeError(f"Failed to fetch {coin_symbol}. Last error: {last_error}")


# ═══════════════════════════════════════════════
#  Feature Engineering (mirrors FeatureEngineering.ipynb)
# ═══════════════════════════════════════════════
def _ema_series(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False).mean()

def build_features(df: pd.DataFrame, extended: bool) -> pd.DataFrame:
    """
    Replicate the create_features() logic from FeatureEngineering.ipynb.
    Input df must have columns: ts_utc, open, high, low, close, volume
    Returns a single-row DataFrame of the latest feature values.
    """
    d = df.copy()
    d = d.rename(columns={
        "ts_utc": "Date",
        "open": "Open", "high": "High", "low": "Low",
        "close": "Close", "volume": "Volume",
    })
    d["Date"] = pd.to_datetime(d["Date"])
    d = d.sort_values("Date").set_index("Date")

    # RSI
    delta  = d["Close"].diff()
    gain   = delta.clip(lower=0).rolling(14).mean()
    loss   = (-delta.clip(upper=0)).rolling(14).mean()
    rs     = gain / loss
    d["RSI_14"] = 100 - 100 / (1 + rs)

    # EMA
    d["EMA_12"] = _ema_series(d["Close"], 12)
    d["EMA_26"] = _ema_series(d["Close"], 26)

    # ATR
    tr = pd.concat([
        d["High"] - d["Low"],
        (d["High"] - d["Close"].shift()).abs(),
        (d["Low"]  - d["Close"].shift()).abs(),
    ], axis=1).max(axis=1)
    d["ATR_14"] = tr.rolling(14).mean()

    # Bollinger Bands
    bb_mid = d["Close"].rolling(20).mean()
    bb_std = d["Close"].rolling(20).std()
    d["BB_Upper"]     = bb_mid + 2 * bb_std
    d["BB_Middle"]    = bb_mid
    d["BB_Lower"]     = bb_mid - 2 * bb_std
    d["Dist_BB_Upper"] = (d["Close"] - d["BB_Upper"]) / d["Close"]
    d["Dist_BB_Lower"] = (d["Close"] - d["BB_Lower"]) / d["Close"]

    # Multi-timeframe: Daily MA50
    daily_close = d["Close"].resample("1D").last()
    daily_ma50  = daily_close.rolling(50).mean()
    d["MA50_1D"]     = daily_ma50.reindex(d.index, method="ffill")
    d["Dist_MA50_1D"] = (d["Close"] - d["MA50_1D"]) / d["MA50_1D"]

    # Lag features
    d["Close_Lag_1"] = d["Close"].shift(1)
    d["Close_Lag_2"] = d["Close"].shift(2)
    d["Close_Lag_3"] = d["Close"].shift(3)

    # Returns
    d["Return_1H"] = d["Close"].pct_change(1)
    d["Return_2H"] = d["Close"].pct_change(2)
    d["Return_3H"] = d["Close"].pct_change(3)

    # Trend
    d["EMA_Spread"] = (d["EMA_12"] - d["EMA_26"]) / d["EMA_26"]

    # Volatility
    d["ATR_Ratio"]     = d["ATR_14"] / d["Close"]
    d["BB_Position"]   = (d["Close"] - d["BB_Lower"]) / (d["BB_Upper"] - d["BB_Lower"])
    d["Volatility_24H"] = d["Return_1H"].rolling(24).std()

    # Volume
    d["Volume_MA20"]  = d["Volume"].rolling(20).mean()
    d["Volume_Ratio"] = d["Volume"] / d["Volume_MA20"]

    # Macro trend
    d["Trend_1D"] = (d["Close"] > d["MA50_1D"]).astype(int)

    if extended:
        d["Dist_EMA12"]      = (d["Close"] - d["EMA_12"]) / d["EMA_12"]
        d["Dist_EMA26"]      = (d["Close"] - d["EMA_26"]) / d["EMA_26"]
        d["Trend_Strength"]  = abs(d["EMA_12"] - d["EMA_26"]) / d["Close"]
        d["RSI_Overbought"]  = (d["RSI_14"] > 70).astype(int)
        d["RSI_Oversold"]    = (d["RSI_14"] < 30).astype(int)
        d["Candle_Range"]    = (d["High"] - d["Low"]) / d["Close"]
        d["Body_Size"]       = abs(d["Close"] - d["Open"]) / d["Close"]
        d["Volume_Change"]   = np.log1p(d["Volume"]).diff()

    # Clean
    d.replace([np.inf, -np.inf], np.nan, inplace=True)
    d.dropna(inplace=True)
    d.reset_index(inplace=True)

    return d


# ═══════════════════════════════════════════════
#  XGBoost Prediction
# ═══════════════════════════════════════════════
def predict_signal(df: pd.DataFrame, ticker: str):
    """
    Run full feature engineering then XGBoost predict_proba.
    Returns (signal, indicators_dict).
    signal: 'UP' (prob >= threshold) | 'DOWN' (prob < threshold)
    """
    extended = (ticker == "ETH-USD")
    feature_cols = ASSET_FEATURES[ticker]

    # Build features on full history
    feat_df = build_features(df, extended=extended)

    if feat_df.empty:
        return "DOWN", {}

    # Take the last row as the live input
    latest = feat_df.iloc[[-1]]

    # Debug: cek NaN di fitur
    nan_cols = [c for c in feature_cols if latest[c].isna().any()]
    if nan_cols:
        print(f'  WARN: NaN di fitur {nan_cols} — akan diisi 0')
        latest = latest.copy()
        latest[nan_cols] = latest[nan_cols].fillna(0)


    # Align columns to what the model was trained on
    X = latest[feature_cols].copy()

    model     = _models[ticker]
    threshold = _thresholds[ticker]

    prob_up = float(model.predict_proba(X)[0, 1])
    signal  = "UP" if prob_up >= threshold else "DOWN"

    # --- Indicator values for display (mirrors old vote_map structure) ---
    row = feat_df.iloc[-1]

    mom_1h  = float(row["Return_1H"]) * 100
    mom_3h  = float(row["Return_3H"]) * 100
    ema12   = float(row["EMA_12"])
    ema26   = float(row["EMA_26"])
    rsi14   = float(row["RSI_14"])
    bb_lo   = float(row["BB_Lower"])
    bb_mid  = float(row["BB_Middle"])
    bb_hi   = float(row["BB_Upper"])
    sma12   = float(latest["Close"].iloc[0])  # close as price proxy
    last    = float(row["Close"])

    # Build a human-readable breakdown of the top drivers
    # (mirrors the old vote_map in structure for frontend compatibility)
    indicators = {
        "last":    last,
        "mom_1h":  mom_1h,
        "mom_3h":  mom_3h,
        "sma12":   sma12,
        "ema12":   ema12,
        "ema26":   ema26,
        "rsi14":   rsi14,
        "bb_lo":   bb_lo,
        "bb_mid":  bb_mid,
        "bb_hi":   bb_hi,
        "prob_up": prob_up,
        "threshold": threshold,
        # Keep vote_map-compatible block for frontend panels
        "votes": round(prob_up * 5),   # scaled 0-5 for pip display
        "vote_map": {
            "mom_1h": mom_1h > 0,
            "mom_3h": mom_3h > 0,
            "ema_x":  ema12 > ema26,
            "rsi":    40 < rsi14 < 70,
            "bb_pos": last > bb_mid,
        },
    }

    return signal, indicators


# ═══════════════════════════════════════════════
#  Groq AI insight
# ═══════════════════════════════════════════════
def get_groq_insight(all_inds, all_signals):
    if not GROQ_API_KEY:
        print("  GROQ_API_KEY not set - skipping")
        return ""

    lines = []
    for ticker, ind in all_inds.items():
        sym  = COIN_META[ticker]["symbol"]
        sig  = all_signals.get(ticker, "DOWN")
        rsi  = ind.get("rsi14")
        prob = ind.get("prob_up", 0)
        thr  = ind.get("threshold", 0.5)
        lines.append(
            f"{sym}: signal={sig}, price=${ind.get('last', 0):,.2f}, "
            f"prob_up={prob:.3f} (threshold={thr:.2f}), "
            f"RSI={f'{rsi:.1f}' if rsi else 'N/A'}, "
            f"mom1H={ind.get('mom_1h', 0):+.2f}%, mom3H={ind.get('mom_3h', 0):+.2f}%, "
            f"EMA12={'>' if ind.get('ema12',0)>ind.get('ema26',0) else '<'}EMA26, "
            f"BB={'above' if ind.get('last',0)>ind.get('bb_mid',0) else 'below'} mid"
        )

    prompt = (
        "Kamu adalah analis teknikal crypto. Berdasarkan output model XGBoost 1 jam ini, "
        "tulis insight singkat 2-3 kalimat dalam Bahasa Indonesia untuk kedua koin. "
        "Sebutkan prob_up dan threshold sebagai dasar sinyal. "
        "Langsung ke poin, tanpa disclaimer, tanpa markdown.\n\n"
        + "\n".join(lines)
    )

    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": "llama-3.1-8b-instant", "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 130, "temperature": 0.4},
            timeout=20,
        )
        r.raise_for_status()
        text = r.json()["choices"][0]["message"]["content"].strip()
        print(f"  OK  insight: {text[:60]}...")
        return text
    except Exception as e:
        print(f"  Groq error: {e}")
        return ""


# ═══════════════════════════════════════════════
#  SVG card
# ═══════════════════════════════════════════════
def _sparkline(prices, x0, y0, w, h):
    mn, mx = min(prices), max(prices)
    rng = mx - mn or 1
    return " ".join(
        f"{x0 + int(i/(len(prices)-1)*w)},{y0 + h - int((p-mn)/rng*h)}"
        for i, p in enumerate(prices)
    )

def _esc(s):
    return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def build_svg_card(all_dfs, all_signals, all_inds, insight, generated_at):
    W        = 900
    PAD      = 36
    COIN_H   = 230
    HEADER_H = 74
    N        = len(all_dfs)

    ins_lines = textwrap.wrap(insight, 95) if insight else []
    INS_H  = (len(ins_lines) * 22 + 36) if ins_lines else 0
    MTH_H  = 30
    FOOT_H = 52
    TOTAL_H = HEADER_H + N * COIN_H + INS_H + MTH_H + FOOT_H + 8

    p = []

    p.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{TOTAL_H}" '
        f'viewBox="0 0 {W} {TOTAL_H}">'
    )
    p.append('<defs>')
    p.append('  <linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">'
             '<stop offset="0%" stop-color="#0d1117"/>'
             '<stop offset="100%" stop-color="#161b22"/></linearGradient>')
    p.append('</defs>')
    p.append(f'<rect width="{W}" height="{TOTAL_H}" fill="url(#bg)"/>')

    p.append(f'<text x="{PAD}" y="42" font-family="monospace" font-size="20" '
             f'font-weight="bold" fill="#e6edf3">Crypto Hourly Update</text>')
    p.append(f'<text x="{PAD}" y="62" font-family="monospace" font-size="11" '
             f'fill="#484f58">{_esc(generated_at)}  |  crypto-hourly by @fbrianzy</text>')
    p.append(f'<line x1="{PAD}" y1="{HEADER_H-2}" x2="{W-PAD}" y2="{HEADER_H-2}" '
             f'stroke="#21262d" stroke-width="1"/>')

    for ci, (ticker, df) in enumerate(all_dfs.items()):
        meta   = COIN_META[ticker]
        signal = all_signals.get(ticker, "DOWN")
        ind    = all_inds.get(ticker, {})
        ss     = SIGNAL_STYLE[signal]
        BY     = HEADER_H + ci * COIN_H + 20

        last    = ind.get("last", df["close"].iloc[-1])
        mom_1h  = ind.get("mom_1h", 0.0)
        mom_24h = (df["close"].iloc[-1] / df["close"].iloc[-25] - 1)*100 if len(df)>=25 else 0.0
        rsi14   = ind.get("rsi14")
        bb_lo   = ind.get("bb_lo", 0.0)
        bb_hi   = ind.get("bb_hi", 0.0)
        bb_mid  = ind.get("bb_mid", 0.0)
        ema12   = ind.get("ema12", 0.0)
        ema26   = ind.get("ema26", 0.0)
        prob_up = ind.get("prob_up", 0.0)
        threshold = ind.get("threshold", 0.5)

        c1h  = "#3fb950" if mom_1h  >= 0 else "#f85149"
        c24h = "#3fb950" if mom_24h >= 0 else "#f85149"
        a1h  = "+" if mom_1h  >= 0 else ""
        a24h = "+" if mom_24h >= 0 else ""

        p.append(f'<text x="{PAD}" y="{BY+16}" font-family="monospace" font-size="13" '
                 f'font-weight="bold" fill="{meta["hex"]}">{meta["icon"]} / USD  —  {meta["name"]}</text>')
        p.append(f'<text x="{PAD}" y="{BY+50}" font-family="monospace" font-size="34" '
                 f'font-weight="bold" fill="#e6edf3">${last:,.2f}</text>')
        p.append(f'<text x="{PAD}" y="{BY+72}" font-family="monospace" font-size="13" '
                 f'fill="{c1h}">{a1h}{mom_1h:.2f}%  1H</text>')
        p.append(f'<text x="{PAD+130}" y="{BY+72}" font-family="monospace" font-size="13" '
                 f'fill="{c24h}">{a24h}{mom_24h:.2f}%  24H</text>')

        SBW, SBH = 140, 40
        SBX = W - PAD - SBW
        SBY = BY + 4
        p.append(f'<rect x="{SBX}" y="{SBY}" width="{SBW}" height="{SBH}" '
                 f'rx="6" fill="{ss["bg"]}"/>')
        p.append(f'<text x="{SBX+SBW//2}" y="{SBY+SBH//2+1}" font-family="monospace" '
                 f'font-size="17" font-weight="bold" fill="{ss["fg"]}" '
                 f'text-anchor="middle" dominant-baseline="central">{ss["label"]}</text>')

        # Probability bar
        bar_w = int(prob_up * 120)
        p.append(f'<rect x="{SBX}" y="{SBY+SBH+10}" width="120" height="6" rx="3" fill="#21262d"/>')
        p.append(f'<rect x="{SBX}" y="{SBY+SBH+10}" width="{bar_w}" height="6" rx="3" fill="{ss["bg"]}"/>')
        p.append(f'<text x="{SBX+124}" y="{SBY+SBH+17}" font-family="monospace" '
                 f'font-size="10" fill="#484f58">P={prob_up:.2f}</text>')

        stats = [
            ("RSI14",  f"{rsi14:.1f}" if rsi14 is not None else "N/A"),
            ("EMA12",  f"{'>' if ema12>ema26 else '<'} EMA26"),
            ("BB",     f"${bb_lo:,.0f} — ${bb_hi:,.0f}"),
            ("THR",    f"{threshold:.2f}"),
        ]
        for si, (lbl, val) in enumerate(stats):
            sx = PAD + si * 210
            p.append(f'<text x="{sx}" y="{BY+100}" font-family="monospace" '
                     f'font-size="11" fill="#484f58">{lbl}</text>')
            p.append(f'<text x="{sx}" y="{BY+118}" font-family="monospace" '
                     f'font-size="13" font-weight="bold" fill="#8b949e">{_esc(val)}</text>')

        spark = df["close"].tail(48).tolist()
        SPX, SPY, SPW, SPH = PAD, BY+132, W-PAD*2, 72
        pts = _sparkline(spark, SPX, SPY, SPW, SPH)
        lx  = pts.split()[-1].split(",")[0]
        p.append(f'<defs><linearGradient id="sg{ci}" x1="0" y1="0" x2="0" y2="1">'
                 f'<stop offset="0%" stop-color="{meta["hex"]}" stop-opacity="0.2"/>'
                 f'<stop offset="100%" stop-color="{meta["hex"]}" stop-opacity="0.01"/>'
                 f'</linearGradient></defs>')
        p.append(f'<polygon points="{SPX},{SPY+SPH} {pts} {lx},{SPY+SPH}" fill="url(#sg{ci})"/>')
        p.append(f'<polyline points="{pts}" fill="none" stroke="{meta["hex"]}" '
                 f'stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>')

        last_pt = pts.split()[-1]
        dot_x, dot_y = last_pt.split(",")
        p.append(f'<circle cx="{dot_x}" cy="{dot_y}" r="4" fill="{meta["hex"]}"/>')
        p.append(f'<circle cx="{dot_x}" cy="{dot_y}" r="7" fill="{meta["hex"]}" opacity="0.25"/>')

        if ci < N - 1:
            dy = HEADER_H + (ci+1)*COIN_H
            p.append(f'<line x1="{PAD}" y1="{dy}" x2="{W-PAD}" y2="{dy}" '
                     f'stroke="#21262d" stroke-width="1"/>')

    INS_Y = HEADER_H + N * COIN_H + 10
    if ins_lines:
        p.append(f'<line x1="{PAD}" y1="{INS_Y}" x2="{W-PAD}" y2="{INS_Y}" '
                 f'stroke="#21262d" stroke-width="1"/>')
        p.append(f'<text x="{PAD}" y="{INS_Y+18}" font-family="monospace" font-size="11" '
                 f'font-weight="bold" fill="#58a6ff">AI Insight  (Groq · llama3-8b)</text>')
        for li, line in enumerate(ins_lines):
            p.append(f'<text x="{PAD}" y="{INS_Y+36+li*22}" font-family="monospace" '
                     f'font-size="13" fill="#8b949e">{_esc(line)}</text>')

    MTH_Y = HEADER_H + N*COIN_H + INS_H + 8
    p.append(f'<text x="{PAD}" y="{MTH_Y+14}" font-family="monospace" font-size="10" '
             f'fill="#30363d">Method: XGBoost · tuned threshold · 29 features (BTC) / 37 features (ETH) · 1H OHLCV  '
             f'UP=prob>=threshold  DOWN=prob&lt;threshold</text>')

    FTY = TOTAL_H - FOOT_H + 10
    p.append(f'<line x1="{PAD}" y1="{FTY}" x2="{W-PAD}" y2="{FTY}" '
             f'stroke="#21262d" stroke-width="1"/>')
    p.append(f'<text x="{PAD}" y="{FTY+18}" font-family="monospace" font-size="11" '
             f'fill="#30363d">fbrianzy.github.io/crypto-hourly  |  github.com/fbrianzy/crypto-hourly'
             f'  |  Source: CoinDesk API</text>')
    p.append(f'<text x="{PAD}" y="{FTY+34}" font-family="monospace" font-size="10" '
             f'fill="#21262d">Crypto Bot by @fbrianzy  |  Auto-update via GitHub Actions</text>')

    p.append("</svg>")
    return "\n".join(p)


def svg_to_png(svg_str):
    return cairosvg.svg2png(bytestring=svg_str.encode("utf-8"), scale=1.5)


# ═══════════════════════════════════════════════
#  Discord
# ═══════════════════════════════════════════════
def build_caption(all_signals, all_inds, now_str):
    parts = []
    for ticker, signal in all_signals.items():
        meta    = COIN_META[ticker]
        price   = all_inds.get(ticker, {}).get("last", 0)
        prob_up = all_inds.get(ticker, {}).get("prob_up", 0)
        parts.append(f"**{meta['symbol']}** `${price:,.2f}` → **{signal}** `p={prob_up:.2f}`")
    return (
        "## Crypto Hourly Update\n"
        + "  ·  ".join(parts)
        + f"\n-# {now_str}"
        + "  ·  [Live Chart](<https://fbrianzy.github.io/crypto-hourly/>)"
        + "  ·  [GitHub](<https://github.com/fbrianzy/crypto-hourly/>)"
    )

def send_discord_image(png_bytes, caption):
    if not WEBHOOK_URL:
        print("  DISCORD_WEBHOOK not set - skipping")
        return False, "DISCORD_WEBHOOK not set"
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(
                WEBHOOK_URL,
                data={"payload_json": json.dumps({
                    "username":   "Crypto Bot by @fbrianzy",
                    "avatar_url": "https://s2.coinmarketcap.com/static/img/coins/64x64/1.png",
                    "content":    caption,
                })},
                files={"file": ("crypto_update.png", io.BytesIO(png_bytes), "image/png")},
                timeout=30,
            )
            if resp.status_code in (200, 204):
                print("  OK  Discord image sent")
                return True, None
            msg = f"HTTP {resp.status_code}: {resp.text[:200]}"
            print(f"  Webhook {msg}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
        except requests.RequestException as e:
            print(f"  Webhook error: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
    return False, "Max retries exceeded"


# ═══════════════════════════════════════════════
#  JSON helpers
# ═══════════════════════════════════════════════
def to_records(df):
    return [{"ts_utc": r["ts_utc"].isoformat(), "close": float(r["close"])}
            for _, r in df.iterrows()]

def write_json(payload, relpath):
    path = os.path.join("data", relpath)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def load_json_safe(relpath):
    path = os.path.join("data", relpath)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ═══════════════════════════════════════════════
#  Pred log helpers  (rolling 30-day window)
# ═══════════════════════════════════════════════
THIRTY_DAYS_SEC = 30 * 24 * 3600

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _ts_age_sec(iso):
    try:
        dt = datetime.fromisoformat(iso)
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return 0

def update_pred_log(now_utc, all_signals, all_inds, all_dfs):
    existing = load_json_safe("pred_log.json") or {"entries": []}
    entries  = existing.get("entries", [])

    # Resolve pending entries
    for entry in entries:
        if entry.get("price_next") is None:
            ticker = entry.get("ticker")
            ind    = all_inds.get(ticker)
            if ind:
                entry["price_next"] = float(ind["last"])

    # Append new entries
    for ticker, signal in all_signals.items():
        ind = all_inds.get(ticker, {})
        entries.append({
            "ts":         now_utc,
            "ticker":     ticker,
            "signal":     signal,
            "price_prev": float(ind.get("last", 0)),
            "price_next": None,
            "prob_up":    float(ind.get("prob_up", 0)),
            "threshold":  float(ind.get("threshold", 0.5)),
        })

    # Prune > 30 days
    entries = [e for e in entries if _ts_age_sec(e.get("ts", "")) < THIRTY_DAYS_SEC]

    write_json({"updated_at": now_utc, "entries": entries}, "pred_log.json")
    print(f"  pred_log.json: {len(entries)} entries")


# ═══════════════════════════════════════════════
#  Run log helpers
# ═══════════════════════════════════════════════
def _append_run_logs(now_utc, gh_entries, dc_entries):
    existing = load_json_safe("run_log.json") or {"gh_runs": [], "discord_runs": []}
    gh_runs  = existing.get("gh_runs", [])
    dc_runs  = existing.get("discord_runs", [])

    gh_runs.extend(gh_entries)
    dc_runs.extend(dc_entries)

    gh_runs = [e for e in gh_runs if _ts_age_sec(e.get("ts","")) < THIRTY_DAYS_SEC]
    dc_runs = [e for e in dc_runs if _ts_age_sec(e.get("ts","")) < THIRTY_DAYS_SEC]

    write_json({"updated_at": now_utc, "gh_runs": gh_runs, "discord_runs": dc_runs}, "run_log.json")
    print(f"  run_log.json: gh={len(gh_runs)} dc={len(dc_runs)}")



# ═══════════════════════════════════════════════
#  24H Forecast
# ═══════════════════════════════════════════════
def forecast_next_24h(df: pd.DataFrame, ticker: str):
    """
    Predict 24H return lalu build garis proyeksi linear 24 titik.
    Hanya dijalankan jika forecast model tersedia.
    Returns dict: { current_price, target_price, pred_return, points: [{ts_utc, price}] }
    atau None jika model tidak ada / error.
    """
    if ticker not in _forecast_models:
        return None

    extended = (ticker == "ETH-USD")
    try:
        feat_df = build_features(df, extended=extended)
        if feat_df.empty:
            return None

        feature_cols = ASSET_FEATURES[ticker]
        latest = feat_df.iloc[[-1]].copy()

        # NaN guard
        nan_cols = [c for c in feature_cols if latest[c].isna().any()]
        if nan_cols:
            latest[nan_cols] = latest[nan_cols].fillna(0)

        X = latest[feature_cols]
        print(X.columns.tolist())
        model = _forecast_models[ticker]
        pred_return = float(model.predict(X)[0])

        current_price = float(df["close"].iloc[-1])
        current_time  = df["ts_utc"].iloc[-1]
        target_price  = current_price * (1 + pred_return)

        forecast_line = np.linspace(current_price, target_price, 24)
        future_times  = pd.date_range(
            start=current_time + pd.Timedelta(hours=1),
            periods=24, freq="1h"
        )

        points = [
            {"ts_utc": t.isoformat(), "price": round(float(p), 2)}
            for t, p in zip(future_times, forecast_line)
        ]

        print(f"  Forecast {ticker}: return={pred_return:.4%}  target=${target_price:,.2f}")
        return {
            "current_price": round(current_price, 2),
            "target_price":  round(target_price, 2),
            "pred_return":   round(pred_return, 6),
            "points":        points,
        }

    except Exception as e:
        print(f"  Forecast error {ticker}: {e}")
        return None


def should_generate_forecast(ticker: str, now_utc: str) -> bool:
    """
    Hanya generate forecast baru jika belum ada untuk hari ini (UTC).
    Reset otomatis setiap ganti hari.
    """
    existing = load_json_safe("forecast.json")
    if not existing:
        return True
    today = now_utc[:10]  # "YYYY-MM-DD"
    generated = existing.get("generated_date", "")
    ticker_data = existing.get("forecasts", {}).get(ticker)
    return generated != today or ticker_data is None

# ═══════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════
def main():
    print(f"\n{'='*60}")
    print(f"Crypto Hourly  |  {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}\n")

    # Load models once
    print("Loading models...")
    load_models()

    now_utc = datetime.now(timezone.utc).isoformat()
    now_str = datetime.fromisoformat(now_utc).strftime("%d %b %Y, %H:%M UTC")

    gh_log_entries = []
    dc_log_entries = []

    all_series   = {}
    all_dfs      = {}
    all_signals  = {}
    all_inds     = {}
    latest_block = {}
    fetch_errors = []

    for idx, (ticker, coin_symbol) in enumerate(COINS.items()):
        if idx:
            time.sleep(2)
        print(f"\n[{idx+1}/{len(COINS)}] {ticker}")
        try:
            df = fetch_cryptocompare_hourly(coin_symbol)
            all_dfs[ticker]      = df
            # Store only last 168 candles for prices.json display
            df_display = df.tail(168).reset_index(drop=True)
            all_series[ticker]   = to_records(df_display)
            latest_block[ticker] = {
                "last_ts_utc": df["ts_utc"].iloc[-1].isoformat(),
                "last_close":  float(df["close"].iloc[-1]),
            }

            signal, ind = predict_signal(df, ticker)
            all_signals[ticker] = signal
            all_inds[ticker]    = ind
            prob_up   = ind.get("prob_up", 0)
            threshold = ind.get("threshold", 0.5)
            print(f"  Signal: {signal}  prob_up={prob_up:.3f}  threshold={threshold:.2f}")

            gh_log_entries.append({
                "ts":      now_utc,
                "level":   "OK",
                "message": (
                    f"fetch {ticker} OK — {len(df)} candles, "
                    f"last=${df['close'].iloc[-1]:,.2f}, "
                    f"signal={signal} (prob={prob_up:.2f}, thr={threshold:.2f})"
                ),
            })

        except Exception as e:
            fetch_errors.append(ticker)
            err_msg = str(e)
            print(f"  FAILED: {err_msg}")
            gh_log_entries.append({
                "ts":       now_utc,
                "level":    "ERROR",
                "message":  f"fetch/predict {ticker} FAILED — {err_msg}",
                "solution": "Periksa COINDESK_API_KEY, koneksi jaringan, atau model .pkl di folder /model/.",
            })

    if fetch_errors:
        gh_log_entries.append({
            "ts":       now_utc,
            "level":    "FAIL",
            "message":  f"Run ABORTED — gagal fetch/predict: {', '.join(fetch_errors)}",
            "solution": "Pastikan COINDESK_API_KEY valid dan file model .pkl tersedia di /model/.",
        })
        _append_run_logs(now_utc, gh_log_entries, dc_log_entries)
        raise SystemExit(1)

    write_json({
        "generated_at_utc": now_utc,
        "interval": "1h",
        "period": "7d",
        "series": all_series,
        "latest": latest_block,
    }, "prices.json")
    gh_log_entries.append({"ts": now_utc, "level": "OK", "message": "prices.json written"})

    # ── Forecast 24H ──────────────────────────────────────────────────
    print("\nGenerating 24H forecast...")
    today_date = now_utc[:10]
    existing_forecast = load_json_safe("forecast.json") or {}
    existing_fc_data  = existing_forecast.get("forecasts", {})
    existing_date     = existing_forecast.get("generated_date", "")

    all_forecasts = {} if existing_date != today_date else dict(existing_fc_data)

    for ticker, df in all_dfs.items():
        if should_generate_forecast(ticker, now_utc):
            fc = forecast_next_24h(df, ticker)
            if fc:
                all_forecasts[ticker] = fc
                gh_log_entries.append({"ts": now_utc, "level": "OK",
                    "message": f"forecast {ticker} OK — target=${fc['target_price']:,.2f} ({fc['pred_return']:+.2%})"})
            else:
                gh_log_entries.append({"ts": now_utc, "level": "WARN",
                    "message": f"forecast {ticker} skipped — no model or error"})
        else:
            print(f"  {ticker}: forecast sudah ada untuk hari ini, skip regenerate")

    if all_forecasts:
        write_json({
            "generated_at_utc": now_utc,
            "generated_date":   today_date,
            "forecasts":        all_forecasts,
        }, "forecast.json")
        gh_log_entries.append({"ts": now_utc, "level": "OK", "message": "forecast.json written"})

    print("\nGroq insight...")
    insight = get_groq_insight(all_inds, all_signals)

    if insight:
        gh_log_entries.append({"ts": now_utc, "level": "OK", "message": f"Groq insight OK — {insight[:80]}..."})
    else:
        gh_log_entries.append({"ts": now_utc, "level": "WARN", "message": "Groq insight kosong atau error"})

    # Serialize indicators
    serialized_inds = {}
    for ticker, ind in all_inds.items():
        serialized_inds[ticker] = {
            "last":      ind.get("last"),
            "mom_1h":    ind.get("mom_1h"),
            "mom_3h":    ind.get("mom_3h"),
            "sma12":     ind.get("sma12"),
            "ema12":     ind.get("ema12"),
            "ema26":     ind.get("ema26"),
            "rsi14":     ind.get("rsi14"),
            "bb_lo":     ind.get("bb_lo"),
            "bb_mid":    ind.get("bb_mid"),
            "bb_hi":     ind.get("bb_hi"),
            "prob_up":   ind.get("prob_up"),
            "threshold": ind.get("threshold"),
            "votes":     ind.get("votes"),       # scaled 0-5 for pip display
            "vote_map":  {k: bool(v) for k, v in (ind.get("vote_map") or {}).items()},
        }

    write_json({
        "generated_at_utc": now_utc,
        "next_1h_prediction": all_signals,
        "method": "xgboost_tuned_threshold_BTC29f_ETH37f",
        "note": "UP=prob_up>=threshold, DOWN=prob_up<threshold. Threshold tuned for best Macro F1.",
        "ai_insight": insight if insight else None,
        "indicators": serialized_inds,
    }, "prediction.json")
    gh_log_entries.append({"ts": now_utc, "level": "OK", "message": "prediction.json written"})

    print("\nJSON written")

    print("\nUpdating pred_log.json...")
    try:
        update_pred_log(now_utc, all_signals, all_inds, all_dfs)
        gh_log_entries.append({"ts": now_utc, "level": "OK", "message": "pred_log.json updated"})
    except Exception as e:
        gh_log_entries.append({"ts": now_utc, "level": "WARN", "message": f"pred_log.json update failed: {e}"})

    print("\nBuilding card...")
    try:
        svg_str   = build_svg_card(all_dfs, all_signals, all_inds, insight, now_str)
        png_bytes = svg_to_png(svg_str)
        print(f"  PNG: {len(png_bytes)/1024:.1f} KB")
        gh_log_entries.append({"ts": now_utc, "level": "OK", "message": f"SVG card built, PNG={len(png_bytes)//1024}KB"})
    except Exception as e:
        print(f"  Card build error: {e}")
        gh_log_entries.append({"ts": now_utc, "level": "ERROR", "message": f"Card build FAILED: {e}",
                               "solution": "Pastikan libcairo2 terinstall dan cairosvg>=2.7.1"})
        png_bytes = None

    print("\nSending to Discord...")
    caption = build_caption(all_signals, all_inds, now_str)
    if png_bytes:
        dc_ok, dc_err = send_discord_image(png_bytes, caption)
        if dc_ok:
            dc_log_entries.append({"ts": now_utc, "level": "OK", "message": "Discord image sent successfully"})
        else:
            dc_log_entries.append({"ts": now_utc, "level": "ERROR", "message": f"Discord send FAILED — {dc_err}",
                                   "solution": "Periksa DISCORD_WEBHOOK URL di repository secrets."})
    else:
        dc_log_entries.append({"ts": now_utc, "level": "WARN", "message": "Discord send skipped — PNG build failed"})

    gh_log_entries.append({"ts": now_utc, "level": "OK", "message": f"Run SUCCESS — {now_str}"})
    _append_run_logs(now_utc, gh_log_entries, dc_log_entries)

    print(f"\n{'='*60}")
    print("SUCCESS")
    for ticker in COINS:
        p   = latest_block[ticker]["last_close"]
        s   = all_signals[ticker]
        prb = all_inds[ticker].get("prob_up", 0)
        thr = all_inds[ticker].get("threshold", 0.5)
        print(f"  {ticker}: {s}  prob_up={prb:.3f}  threshold={thr:.2f}  ${p:,.2f}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        print(f"\nFAILED: {e}")
        raise SystemExit(1)
