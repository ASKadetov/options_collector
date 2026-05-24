# OKX Options & Open Interest Data Collector

A production-ready data ingestion pipeline that periodically snapshots the entire options order board and tracks Open Interest (OI) metrics for specified cryptocurrencies directly from the OKX exchange.

## 💡 The Problem & The Solution

* **The Problem:** Historical cryptocurrency options data and granular Open Interest timelines are notoriously difficult to find. Most institutional data providers lock this historical depth behind expensive premium paywalls.
* **The Solution:** This project resolves the barrier to entry by establishing a self-hosted, lightweight, and continuous data scraper. By running this pipeline, you create your own comprehensive, free database of historical options market depth and OI trends.

## 🚀 Key Features

* **Full Option Board Snapshots:** Captures essential Greeks ($\Delta$, $\Gamma$, $\mathcal{V}$, $\Theta$), implied volatilities, leverage, and forward prices for all active option instruments.
* **Open Interest Ingestion:** Tracks real-time open interest volumes, currency values, and equivalent USD metrics.
* **Isolated Transactions:** Failures during an Open Interest insertion do not corrupt or roll back the captured Options snapshot (and vice-versa).
* **Asynchronous Execution:** Leverages `asyncio` and `aiohttp` to aggregate API data rapidly without blocking the runtime loop.
* **Containerized Deployment:** Fully configured Docker architecture designed to spin up and scale seamlessly on a production server.

## 🛠️ Tech Stack

* **Language:** Python 3.11+
* **Concurrency:** `asyncio` & `aiohttp`
* **Database:** PostgreSQL
* **DB Drivers / Toolkit:** `psycopg2`, `SQLAlchemy`
* **Deployment:** Docker & Docker Compose

## 📦 Database Schema

The pipeline automatically provisions and maintains two primary tables:

### 1. `option_snapshots`
Stores full option matrix records, including pricing matrices and options Greeks.
* `snapshot_ts` (Timestamp, Primary Key)
* `instrument_name` (Text, Primary Key)
* `mark_price` & `forward_price`
* Volatility metrics: `askVol`, `bidVol`, `markVol`, `volLv`, `realVol`
* Options Greeks: `delta`, `deltaBS`, `gamma`, `gammaBS`, `vega`, `vegaBS`, `theta`, `thetaBS`
* Risk indicators: `distance`, `leverage`, `buyApr`, `sellApr`

### 2. `open_interest`
Tracks perpetual/futures market liquidity markers.
* `snapshot_ts` (Timestamp, Primary Key)
* `instrument_name` (Text, Primary Key)
* `oi` (Open Interest Contract Volume)
* `oiCcy` (Open Interest in Margin Currency)
* `oiUsd` (Open Interest in USD value)

---

## ⚙️ Configuration & Deployment

### 1. Environment Configuration
Create a `.env` file in the root directory of your project with your PostgreSQL server credentials:

```env
DB_HOST=your_db_host
DB_PORT=5432
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_secure_password
```

### 2. Run via Docker Compose
To build and start the tracker service in the background, run:
```bash
docker compose up -d --build
```

### 3. Monitoring Application Logs
Track the state of your data collection loop and verify successful database transactions:
```bash
docker compose logs -f
```
