# Financial Market Data Pipeline

---

## Overview

A production-grade **end-to-end data engineering pipeline** that ingests, transforms, and analyses financial market tick data using the exact same GCP technology stack required:

| Required Skill | How It's Used in This Project |
|---|---|
| **Apache Airflow** | DAG orchestration (`financial_market_pipeline_v1`) with 3 tasks and dependency management |
| **Google BigQuery** | SQL transforms — hourly OHLCV aggregation from raw ticks |
| **Google Dataflow** | Apache Beam–style streaming ingestion layer |
| **Google BigTable** | Wide-column time-series storage for raw tick data |
| **DBT** | `mart__risk_metrics` model — volatility, Sharpe ratio, risk classification |
| **SQL** | Complex GROUP BY, window functions, VWAP calculation |
| **Python** | Full pipeline: data models, pipeline logic, analytics, exports |

---

## Architecture

```
Market Feed (Streaming)
      |
      v
[Dataflow] ─ Apache Beam–style ingestion
      |        4,800 ticks / 30 days
      v
[BigTable] ─ Wide-column raw tick store
      |        market_ticks table
      v
[BigQuery SQL Transform] ─ OHLCV hourly aggregation
      |                     1,805 records
      v
[DBT Mart Model] ─ mart__risk_metrics
      |              volatility, Sharpe, risk level
      v
[Dashboard] ─ Interactive analytics
               Price charts, risk table, DAG view
```

---

## Project Structure

```
├── pipeline/
│   └── pipeline.py          # Full ETL pipeline (Python)
│       ├── BigTableStore    # Storage layer (SQLite → simulates BigTable)
│       ├── DataflowIngestion # Streaming tick ingestion
│       ├── BigQueryTransform # SQL OHLCV aggregation
│       ├── DBTMartModels     # Risk analytics mart
│       └── AirflowDAG        # Orchestrator — runs all 3 tasks
│
├── data/
│   ├── pipeline.db           # SQLite database (BigTable + BigQuery equivalent)
│   └── dashboard_data.json   # Exported analytics for dashboard
│
└── dashboard/
    └── index.html            # Interactive analytics dashboard


---

## Running the Pipeline

```bash
# 1. Install dependencies
pip install --break-system-packages sqlite3  # (built-in Python)

# 2. Run the full Airflow DAG
cd pipeline
python pipeline.py

# Output:
# =====================================================
#   AIRFLOW DAG: financial_market_pipeline_v1
#   Run ID: 2387fc558500
# =====================================================
# [Task 1/3] ingest_market_ticks  → RUNNING...
#             SUCCESS — 4,800 ticks ingested (Dataflow)
# [Task 2/3] transform_to_ohlcv   → RUNNING...
#             SUCCESS — 1,805 OHLCV records created (BigQuery SQL)
# [Task 3/3] build_risk_mart       → RUNNING...
#             SUCCESS — 8 risk metrics computed (DBT)

# 3. Open dashboard
# Open dashboard/index.html in any browser
```

---

## Key Technical Details

### Airflow DAG Structure
```python
AirflowDAG.run()
  ├── task_ingest()   → DataflowIngestion.ingest_stream()
  ├── task_transform() → BigQueryTransform.run_sql_transform()
  └── task_dbt()      → DBTMartModels.build_risk_metrics_mart()
```

### BigQuery SQL Transform
```sql
SELECT
    symbol,
    DATE(timestamp) AS date,
    CAST(strftime('%H', timestamp) AS INTEGER) AS hour,
    MIN(price) AS open_price,
    MAX(price) AS high_price,
    MIN(price) AS low_price,
    MAX(price) AS close_price,
    SUM(volume) AS volume,
    SUM(price * volume) / NULLIF(SUM(volume), 0) AS vwap
FROM market_ticks
GROUP BY symbol, DATE(timestamp), strftime('%H', timestamp)
```

### DBT Risk Metrics Model
- **Annualised Volatility**: `stdev(returns) × √252`
- **Sharpe Ratio**: `(mean_return / stdev_return) × √252`
- **Risk Classification**: LOW / MEDIUM / HIGH based on volatility thresholds
- **7-day rolling window** on OHLCV records

---

## Technologies

- **Python 3** — Pipeline logic, data models, analytics
- **SQL / SQLite** — Simulates BigQuery transforms
- **Apache Airflow** — DAG orchestration pattern
- **Google BigQuery** — Data warehouse SQL layer
- **Google Dataflow / Apache Beam** — Streaming ingestion
- **Google BigTable** — Wide-column time-series store
- **DBT** — Mart model transformations

---

*Reddypalli Manojkumar Reddy | BITS Pilani | 2023A4PS1103G*
