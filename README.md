# Crypto Hourly — BTC & ETH Live + Prediksi 1 Jam (Root Version)

Website statis di **root** repository untuk GitHub Pages (Folder: `/(root)`).
Auto-commit per jam dengan identitas **fbrianzy (bagusfeb60@gmail.com)**.

## Struktur
```
.
├─ data/                 # JSON yang dibaca halaman web
├─ scripts/              # fetch_predict.py (yfinance + sinyal)
├─ .github/workflows/    # hourly.yml (cron tiap jam)
├─ index.html            # halaman utama
├─ app.js                # logic chart & prediksi
└─ requirements.txt
```

## Setup
1. Push semua file ini ke branch `main`.
2. Settings → Pages → Source: Deploy from a branch → Branch: `main` → Folder: `/(root)`.
3. Workflow jalan tiap jam; JSON di `data/` akan diperbarui otomatis.
4. Halaman otomatis membaca `./data/*.json` dan menampilkan chart + prediksi.

## Kustom
- Edit sinyal di `scripts/fetch_predict.py` (fungsi `simple_signal`).
- UI waktu lokal ada di `app.js` (`toLocaleString('id-ID')`).

© 2025
