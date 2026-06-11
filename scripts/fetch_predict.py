import io
import json
import os
import time
import textwrap
from datetime import datetime, timezone

import cairosvg
import requests
import pandas as pd

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


# ═══════════════════════════════════════════════
#  Data fetching
# ═══════════════════════════════════════════════
def fetch_cryptocompare_hourly(coin_symbol: str) -> pd.DataFrame:
    """
    Fetch 168 candles dari CoinDesk Data API (spot OHLCV hourly).
    Mencoba beberapa market sebagai fallback.
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
            "limit":      168,
            "groups":     "OHLC",
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
                    if ts and close:
                        rows.append({"timestamp": ts, "close": float(close)})

                if not rows:
                    raise ValueError("No valid rows after parsing")

                df = pd.DataFrame(rows)
                df["ts_utc"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
                df = df.sort_values("ts_utc").reset_index(drop=True)
                print(f"  OK  {len(df)} candles [{market}] | last: ${df['close'].iloc[-1]:,.2f}")
                return df[["ts_utc", "close"]]

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
#  Technical indicators
# ═══════════════════════════════════════════════
def _ema(series, n):
    k = 2 / (n + 1)
    e = series[0]
    for v in series[1:]:
        e = v * k + e * (1 - k)
    return e

def _rsi(series, n=14):
    if len(series) < n + 1:
        return None
    deltas = [series[i] - series[i-1] for i in range(1, len(series))]
    gains  = [d for d in deltas if d > 0]
    losses = [-d for d in deltas if d < 0]
    avg_g  = sum(gains[-n:])  / n
    avg_l  = sum(losses[-n:]) / n
    if avg_l == 0:
        return 100.0
    return 100 - 100 / (1 + avg_g / avg_l)

def _bollinger(series, n=20):
    window = series[-n:]
    mid    = sum(window) / n
    std    = (sum((x - mid)**2 for x in window) / n) ** 0.5
    return mid - 2*std, mid, mid + 2*std


def predict_signal(close_series):
    """5-factor voting. Returns (signal, indicators_dict)."""
    if len(close_series) < 26:
        return "HOLD", {}

    last  = close_series[-1]
    prev1 = close_series[-2]
    prev3 = close_series[-4]

    mom_1h = last / prev1 - 1
    mom_3h = last / prev3 - 1
    ema12  = _ema(close_series[-12:], 12)
    ema26  = _ema(close_series[-26:], 26)
    rsi14  = _rsi(close_series[-30:], 14)
    bb_lo, bb_mid, bb_hi = _bollinger(close_series, 20)
    sma12  = sum(close_series[-12:]) / 12

    votes = {
        "mom_1h": mom_1h > 0,
        "mom_3h": mom_3h > 0,
        "ema_x":  ema12 > ema26,
        "rsi":    rsi14 is not None and 40 < rsi14 < 70,
        "bb_pos": last > bb_mid,
    }
    score = sum(votes.values())

    if score >= 4:
        signal = "UP"
    elif score <= 2:
        signal = "DOWN"
    else:
        signal = "HOLD"

    return signal, {
        "last": last, "mom_1h": mom_1h*100, "mom_3h": mom_3h*100,
        "sma12": sma12, "ema12": ema12, "ema26": ema26, "rsi14": rsi14,
        "bb_lo": bb_lo, "bb_mid": bb_mid, "bb_hi": bb_hi,
        "votes": score, "vote_map": votes,
    }


# ═══════════════════════════════════════════════
#  Groq AI insight
# ═══════════════════════════════════════════════
def get_groq_insight(all_inds, all_signals):
    if not GROQ_API_KEY:
        print("  GROQ_API_KEY not set - skipping")
        return ""

    lines = []
    for ticker, ind in all_inds.items():
        sym = COIN_META[ticker]["symbol"]
        sig = all_signals.get(ticker, "HOLD")
        rsi = ind.get("rsi14")
        lines.append(
            f"{sym}: signal={sig}, price=${ind['last']:,.2f}, "
            f"RSI={f'{rsi:.1f}' if rsi else 'N/A'}, "
            f"mom1H={ind['mom_1h']:+.2f}%, mom3H={ind['mom_3h']:+.2f}%, "
            f"EMA12={'>' if ind['ema12']>ind['ema26'] else '<'}EMA26, "
            f"BB={'above' if ind['last']>ind['bb_mid'] else 'below'} mid, "
            f"votes={ind['votes']}/5"
        )

    prompt = (
        "Kamu adalah analis teknikal crypto. Berdasarkan indikator 1 jam ini, "
        "tulis insight singkat 2-3 kalimat dalam Bahasa Indonesia untuk kedua koin. "
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
        signal = all_signals.get(ticker, "HOLD")
        ind    = all_inds.get(ticker, {})
        ss     = SIGNAL_STYLE[signal]
        BY     = HEADER_H + ci * COIN_H + 20

        last    = ind.get("last", df["close"].iloc[-1])
        mom_1h  = ind.get("mom_1h", 0.0)
        mom_24h = (df["close"].iloc[-1] / df["close"].iloc[-25] - 1)*100 if len(df)>=25 else 0.0
        rsi14   = ind.get("rsi14")
        sma12   = ind.get("sma12", 0.0)
        bb_lo   = ind.get("bb_lo", 0.0)
        bb_hi   = ind.get("bb_hi", 0.0)
        bb_mid  = ind.get("bb_mid", 0.0)
        votes   = ind.get("votes", 0)
        ema12   = ind.get("ema12", 0.0)
        ema26   = ind.get("ema26", 0.0)

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

        SBW, SBH = 120, 40
        SBX = W - PAD - SBW
        SBY = BY + 4
        p.append(f'<rect x="{SBX}" y="{SBY}" width="{SBW}" height="{SBH}" '
                 f'rx="6" fill="{ss["bg"]}"/>')
        p.append(f'<text x="{SBX+SBW//2}" y="{SBY+SBH//2+1}" font-family="monospace" '
                 f'font-size="17" font-weight="bold" fill="{ss["fg"]}" '
                 f'text-anchor="middle" dominant-baseline="central">{ss["label"]}</text>')

        for vi in range(5):
            fc = "#3fb950" if vi < votes else "#21262d"
            p.append(f'<circle cx="{SBX + vi*24 + 12}" cy="{SBY+SBH+18}" r="9" fill="{fc}"/>')
        p.append(f'<text x="{SBX+5*24+4}" y="{SBY+SBH+23}" font-family="monospace" '
                 f'font-size="11" fill="#484f58">{votes}/5</text>')

        stats = [
            ("SMA12",  f"${sma12:,.0f}"),
            ("RSI14",  f"{rsi14:.1f}" if rsi14 is not None else "N/A"),
            ("EMA12",  f"{'>' if ema12>ema26 else '<'} EMA26"),
            ("BB",     f"${bb_lo:,.0f} — ${bb_hi:,.0f}"),
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
             f'fill="#30363d">Method: 5-factor vote [mom1H · mom3H · EMA12&gt;EMA26 · RSI(40-70) · BB_mid]  '
             f'UP&gt;=4  HOLD=3  DOWN&lt;=2</text>')

    FTY = TOTAL_H - FOOT_H + 10
    p.append(f'<line x1="{PAD}" y1="{FTY}" x2="{W-PAD}" y2="{FTY}" '
             f'stroke="#21262d" stroke-width="1"/>')
    p.append(f'<text x="{PAD}" y="{FTY+18}" font-family="monospace" font-size="11" '
             f'fill="#30363d">fbrianzy.github.io/crypto-hourly  |  github.com/fbrianzy/crypto-hourly'
             f'  |  Source: CryptoCompare</text>')
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
        meta  = COIN_META[ticker]
        price = all_inds.get(ticker, {}).get("last", 0)
        parts.append(f"**{meta['symbol']}** `${price:,.2f}` → **{signal}**")
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
    """
    Append a new prediction entry for each coin.
    Also resolve the PREVIOUS entry's price_next (close the loop).
    Prune entries older than 30 days.
    """
    existing = load_json_safe("pred_log.json") or {"entries": []}
    entries  = existing.get("entries", [])

    # Resolve pending entries: fill price_next from current price
    for entry in entries:
        if entry.get("price_next") is None:
            ticker  = entry.get("ticker")
            df      = all_dfs.get(ticker)
            if df is not None:
                # match the entry's ts to the df series; use current price as settlement
                entry["price_next"] = float(all_inds[ticker]["last"])

    # Append new entries (one per coin)
    for ticker, signal in all_signals.items():
        ind = all_inds.get(ticker, {})
        entry = {
            "ts":         now_utc,
            "ticker":     ticker,
            "signal":     signal,
            "price_prev": float(ind.get("last", 0)),
            "price_next": None,       # will be filled on next run
            "votes":      ind.get("votes", 0),
        }
        entries.append(entry)

    # Prune entries older than 30 days
    entries = [e for e in entries if _ts_age_sec(e.get("ts", "")) < THIRTY_DAYS_SEC]

    write_json({"updated_at": now_utc, "entries": entries}, "pred_log.json")
    print(f"  pred_log.json: {len(entries)} entries")


# ═══════════════════════════════════════════════
#  Run log helpers  (rolling 30-day window)
# ═══════════════════════════════════════════════
MAX_LOG_ENTRIES = 720   # ~30 days × 24h × ~1 run/h

def _append_run_logs(now_utc, gh_entries, dc_entries):
    existing = load_json_safe("run_log.json") or {"gh_runs": [], "discord_runs": []}
    gh_runs  = existing.get("gh_runs", [])
    dc_runs  = existing.get("discord_runs", [])

    gh_runs.extend(gh_entries)
    dc_runs.extend(dc_entries)

    # Prune to 30-day window
    gh_runs = [e for e in gh_runs if _ts_age_sec(e.get("ts","")) < THIRTY_DAYS_SEC]
    dc_runs = [e for e in dc_runs if _ts_age_sec(e.get("ts","")) < THIRTY_DAYS_SEC]

    write_json({"updated_at": now_utc, "gh_runs": gh_runs, "discord_runs": dc_runs}, "run_log.json")
    print(f"  run_log.json: gh={len(gh_runs)} dc={len(dc_runs)}")


# ═══════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════
def main():
    print(f"\n{'='*60}")
    print(f"Crypto Hourly  |  {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}\n")

    now_utc = datetime.now(timezone.utc).isoformat()
    now_str = datetime.fromisoformat(now_utc).strftime("%d %b %Y, %H:%M UTC")

    gh_log_entries = []   # accumulate for run_log.json
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
        print(f"[{idx+1}/{len(COINS)}] {ticker}")
        try:
            df = fetch_cryptocompare_hourly(coin_symbol)
            all_dfs[ticker]      = df
            all_series[ticker]   = to_records(df)
            latest_block[ticker] = {
                "last_ts_utc": df["ts_utc"].iloc[-1].isoformat(),
                "last_close":  float(df["close"].iloc[-1]),
            }
            signal, ind = predict_signal(df["close"].tolist())
            all_signals[ticker] = signal
            all_inds[ticker]    = ind
            print(f"  Signal: {signal}  votes={ind.get('votes',0)}/5")

            gh_log_entries.append({
                "ts":      now_utc,
                "level":   "OK",
                "message": f"fetch {ticker} OK — {len(df)} candles, last=${df['close'].iloc[-1]:,.2f}, signal={signal} ({ind.get('votes',0)}/5)",
            })

        except Exception as e:
            fetch_errors.append(ticker)
            err_msg = str(e)
            print(f"  FAILED: {err_msg}")
            gh_log_entries.append({
                "ts":       now_utc,
                "level":    "ERROR",
                "message":  f"fetch {ticker} FAILED — {err_msg}",
                "solution": "Periksa COINDESK_API_KEY, koneksi jaringan, atau coba lagi. Semua market fallback (coinbase/kraken/bitstamp/gemini) gagal.",
            })

    if fetch_errors:
        # abort early if no data
        gh_log_entries.append({
            "ts":       now_utc,
            "level":    "FAIL",
            "message":  f"Run ABORTED — gagal fetch: {', '.join(fetch_errors)}",
            "solution": "Pastikan COINDESK_API_KEY valid dan quota API tidak habis.",
        })
        _append_run_logs(now_utc, gh_log_entries, dc_log_entries)
        raise SystemExit(1)

    write_json({"generated_at_utc": now_utc, "interval": "1h", "period": "7d",
                "series": all_series, "latest": latest_block}, "prices.json")
    gh_log_entries.append({"ts": now_utc, "level": "OK", "message": "prices.json written"})

    print("\nGroq insight...")
    insight = get_groq_insight(all_inds, all_signals)

    if insight:
        gh_log_entries.append({"ts": now_utc, "level": "OK", "message": f"Groq insight OK — {insight[:80]}..."})
    else:
        gh_log_entries.append({"ts": now_utc, "level": "WARN", "message": "Groq insight kosong atau error (GROQ_API_KEY mungkin tidak di-set)"})

    # Serialize indicators
    serialized_inds = {}
    for ticker, ind in all_inds.items():
        serialized_inds[ticker] = {
            "last":    ind.get("last"),
            "mom_1h":  ind.get("mom_1h"),
            "mom_3h":  ind.get("mom_3h"),
            "sma12":   ind.get("sma12"),
            "ema12":   ind.get("ema12"),
            "ema26":   ind.get("ema26"),
            "rsi14":   ind.get("rsi14"),
            "bb_lo":   ind.get("bb_lo"),
            "bb_mid":  ind.get("bb_mid"),
            "bb_hi":   ind.get("bb_hi"),
            "votes":   ind.get("votes"),
            "vote_map": {k: bool(v) for k, v in (ind.get("vote_map") or {}).items()},
        }

    write_json({"generated_at_utc": now_utc, "next_1h_prediction": all_signals,
                "method": "5factor_vote_mom1H_mom3H_EMA_RSI_BB",
                "note": "UP>=4/5 votes bullish, DOWN<=2/5, else HOLD.",
                "ai_insight": insight if insight else None,
                "indicators": serialized_inds}, "prediction.json")
    gh_log_entries.append({"ts": now_utc, "level": "OK", "message": "prediction.json written"})

    print("\nJSON written")

    # ── Update pred_log.json ──────────────────
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
                               "solution": "Pastikan libcairo2 terinstall (apt-get install libcairo2) dan cairosvg>=2.7.1"})
        png_bytes = None

    print("\nSending to Discord...")
    caption = build_caption(all_signals, all_inds, now_str)
    if png_bytes:
        dc_ok, dc_err = send_discord_image(png_bytes, caption)
        if dc_ok:
            dc_log_entries.append({"ts": now_utc, "level": "OK",    "message": "Discord image sent successfully"})
        else:
            dc_log_entries.append({"ts": now_utc, "level": "ERROR", "message": f"Discord send FAILED — {dc_err}",
                                   "solution": "Periksa DISCORD_WEBHOOK URL di repository secrets. Pastikan webhook masih aktif di channel Discord."})
    else:
        dc_log_entries.append({"ts": now_utc, "level": "WARN", "message": "Discord send skipped — PNG build failed"})

    gh_log_entries.append({"ts": now_utc, "level": "OK", "message": f"Run SUCCESS — {now_str}"})

    # ── Write run_log.json ────────────────────
    _append_run_logs(now_utc, gh_log_entries, dc_log_entries)

    print(f"\n{'='*60}")
    print("SUCCESS")
    for ticker in COINS:
        p = latest_block[ticker]["last_close"]
        s = all_signals[ticker]
        v = all_inds[ticker].get("votes", 0)
        print(f"  {ticker}: {s}  votes={v}/5  ${p:,.2f}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        print(f"\nFAILED: {e}")
        raise SystemExit(1)
