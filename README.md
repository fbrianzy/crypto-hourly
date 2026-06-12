<p align="center">
  <img src="assets/banner.svg" width="100%">
</p>

<p align="center">

<img src="https://img.shields.io/badge/GitHub_Actions-automated-2ea44f?style=flat-square&logo=githubactions&logoColor=white">

<img src="https://img.shields.io/badge/Python-3.11-3776ab?style=flat-square&logo=python&logoColor=white">

<img src="https://img.shields.io/badge/XGBoost-classifier-e76f00?style=flat-square">

<img src="https://img.shields.io/badge/Groq-llama--3.1--8b-f55036?style=flat-square">

<img src="https://img.shields.io/badge/license-MIT-blue?style=flat-square">

</p>

---

## What it does

CryptoHourly is a self-updating cryptocurrency dashboard that runs every 15 minutes using GitHub Actions.

The system fetches live OHLCV data for Bitcoin and Ethereum, generates engineered technical features, runs each asset through a dedicated XGBoost classifier, and publishes the resulting market signal to a static GitHub Pages dashboard.

Features include:

- Hourly BTC & ETH directional prediction
- Asset-specific XGBoost models
- Tuned probability thresholds
- Automated Discord notifications
- AI-generated market commentary via Groq
- Historical prediction logging
- Fully static frontend powered by GitHub Pages

No server, database, or cloud infrastructure is required.

---

## Preview

![CryptoHourly Dashboard](assets/dashboard-preview.png)

---

## Architecture

```text
CoinDesk API
      │
      ▼
fetch_predict.py
      │
      ├── XGBoost Classifier
      ├── Groq Insight
      ├── Discord Notification
      ▼
 data/*.json
      ▼
 GitHub Pages
```

---

## Tech Stack

<p align="center">
  <img src="assets/tech-stack.svg" width="100%">
</p>
