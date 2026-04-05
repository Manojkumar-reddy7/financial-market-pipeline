"""
Financial Market Data Pipeline
================================
Simulates a production-grade ETL pipeline using:
- Apache Airflow DAG structure
- BigQuery-style data warehouse operations
- DBT-style transformations
- BigTable-style time-series storage
- Dataflow-style streaming processing

Author: Reddypalli Manojkumar Reddy
"""

import sqlite3
import json
import random
import hashlib
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
import statistics


# ─────────────────────────────────────────────
# DATA MODELS (BigTable-style schema)
# ─────────────────────────────────────────────

@dataclass
class MarketTick:
    """Raw tick data — equivalent to BigTable row"""
    tick_id: str
    symbol: str
    price: float
    volume: int
    timestamp: str
    source: str
    ingested_at: str

@dataclass
class OHLCVRecord:
    """OHLCV aggregated record — BigQuery fact table"""
    record_id: str
    symbol: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    vwap: float
    date: str
    hour: int
    created_at: str

@dataclass
class RiskMetric:
    """DBT-style mart model — derived analytics"""
    metric_id: str
    symbol: str
    volatility_7d: float
    avg_volume_7d: float
    price_change_pct: float
    sharpe_ratio: float
    risk_level: str
    computed_at: str


# ─────────────────────────────────────────────
# BIGTABLE-STYLE STORAGE LAYER
# ─────────────────────────────────────────────

class BigTableStore:
    """Simulates Google Cloud BigTable — wide-column, time-series optimized"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._create_tables()

    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS market_ticks (
                tick_id     TEXT PRIMARY KEY,
                symbol      TEXT NOT NULL,
                price       REAL NOT NULL,
                volume      INTEGER NOT NULL,
                timestamp   TEXT NOT NULL,
                source      TEXT,
                ingested_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ohlcv_records (
                record_id   TEXT PRIMARY KEY,
                symbol      TEXT NOT NULL,
                open_price  REAL,
                high_price  REAL,
                low_price   REAL,
                close_price REAL,
                volume      INTEGER,
                vwap        REAL,
                date        TEXT,
                hour        INTEGER,
                created_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS risk_metrics (
                metric_id       TEXT PRIMARY KEY,
                symbol          TEXT NOT NULL,
                volatility_7d   REAL,
                avg_volume_7d   REAL,
                price_change_pct REAL,
                sharpe_ratio    REAL,
                risk_level      TEXT,
                computed_at     TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id      TEXT PRIMARY KEY,
                dag_id      TEXT NOT NULL,
                task_id     TEXT NOT NULL,
                status      TEXT NOT NULL,
                started_at  TEXT,
                ended_at    TEXT,
                rows_in     INTEGER DEFAULT 0,
                rows_out    INTEGER DEFAULT 0,
                error_msg   TEXT
            );
        """)
        self.conn.commit()

    def insert_ticks(self, ticks: List[MarketTick]):
        self.conn.executemany(
            "INSERT OR REPLACE INTO market_ticks VALUES (?,?,?,?,?,?,?)",
            [tuple(asdict(t).values()) for t in ticks]
        )
        self.conn.commit()

    def insert_ohlcv(self, records: List[OHLCVRecord]):
        self.conn.executemany(
            "INSERT OR REPLACE INTO ohlcv_records VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [tuple(asdict(r).values()) for r in records]
        )
        self.conn.commit()

    def insert_risk_metrics(self, metrics: List[RiskMetric]):
        self.conn.executemany(
            "INSERT OR REPLACE INTO risk_metrics VALUES (?,?,?,?,?,?,?,?)",
            [tuple(asdict(m).values()) for m in metrics]
        )
        self.conn.commit()

    def log_pipeline_run(self, run_id, dag_id, task_id, status,
                          started_at, ended_at=None, rows_in=0, rows_out=0, error_msg=None):
        self.conn.execute(
            "INSERT OR REPLACE INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?,?)",
            (run_id, dag_id, task_id, status, started_at, ended_at, rows_in, rows_out, error_msg)
        )
        self.conn.commit()


# ─────────────────────────────────────────────
# DATAFLOW-STYLE STREAMING INGESTION
# ─────────────────────────────────────────────

class DataflowIngestion:
    """
    Simulates Google Cloud Dataflow — Apache Beam style
    streaming ingestion of market tick data
    """

    SYMBOLS = ["HSBC", "AAPL", "GOOGL", "MSFT", "AMZN", "JPM", "GS", "MS"]
    BASE_PRICES = {
        "HSBC": 42.50, "AAPL": 185.20, "GOOGL": 142.80,
        "MSFT": 378.50, "AMZN": 182.10, "JPM": 198.70,
        "GS": 452.30, "MS": 96.40
    }

    def __init__(self, store: BigTableStore):
        self.store = store
        self._prices = dict(self.BASE_PRICES)

    def _simulate_price(self, symbol: str) -> float:
        """Random walk price simulation"""
        change = random.gauss(0, 0.005)
        self._prices[symbol] *= (1 + change)
        return round(self._prices[symbol], 4)

    def ingest_stream(self, days: int = 30, ticks_per_hour: int = 20) -> int:
        """
        Dataflow pipeline: source → transform → sink
        Ingests N days of simulated tick data
        """
        ticks = []
        now = datetime.now()

        for day_offset in range(days):
            base_date = now - timedelta(days=days - day_offset)
            for hour in range(9, 17):  # market hours
                for _ in range(ticks_per_hour):
                    minute = random.randint(0, 59)
                    second = random.randint(0, 59)
                    ts = base_date.replace(hour=hour, minute=minute, second=second)

                    symbol = random.choice(self.SYMBOLS)
                    price = self._simulate_price(symbol)
                    volume = random.randint(100, 50000)

                    tick_id = hashlib.md5(
                        f"{symbol}{ts.isoformat()}{price}".encode()
                    ).hexdigest()[:16]

                    ticks.append(MarketTick(
                        tick_id=tick_id,
                        symbol=symbol,
                        price=price,
                        volume=volume,
                        timestamp=ts.isoformat(),
                        source="market_feed_v2",
                        ingested_at=datetime.now().isoformat()
                    ))

        self.store.insert_ticks(ticks)
        return len(ticks)


# ─────────────────────────────────────────────
# BIGQUERY-STYLE TRANSFORMATION LAYER
# ─────────────────────────────────────────────

class BigQueryTransform:
    """
    Simulates BigQuery SQL transformations
    Aggregates raw ticks into OHLCV hourly candles
    """

    def __init__(self, store: BigTableStore):
        self.store = store

    def run_sql_transform(self) -> int:
        """
        BigQuery-style SQL: aggregate ticks → OHLCV
        Equivalent to running a scheduled query in BigQuery
        """
        query = """
            SELECT
                symbol,
                DATE(timestamp)   AS date,
                CAST(strftime('%H', timestamp) AS INTEGER) AS hour,
                MIN(price)        AS open_price,
                MAX(price)        AS high_price,
                MIN(price)        AS low_price,
                MAX(price)        AS close_price,
                SUM(volume)       AS volume,
                SUM(price * volume) / NULLIF(SUM(volume), 0) AS vwap
            FROM market_ticks
            GROUP BY symbol, DATE(timestamp), strftime('%H', timestamp)
        """
        rows = self.store.conn.execute(query).fetchall()
        records = []
        for row in rows:
            symbol, date, hour, open_p, high_p, low_p, close_p, vol, vwap = row
            rid = hashlib.md5(f"{symbol}{date}{hour}".encode()).hexdigest()[:16]
            records.append(OHLCVRecord(
                record_id=rid,
                symbol=symbol,
                open_price=round(open_p, 4),
                high_price=round(high_p, 4),
                low_price=round(low_p, 4),
                close_price=round(close_p, 4),
                volume=vol,
                vwap=round(vwap or 0, 4),
                date=date,
                hour=hour,
                created_at=datetime.now().isoformat()
            ))

        self.store.insert_ohlcv(records)
        return len(records)


# ─────────────────────────────────────────────
# DBT-STYLE MART MODELS
# ─────────────────────────────────────────────

class DBTMartModels:
    """
    Simulates DBT (Data Build Tool) transformations
    Builds analytics mart models from warehouse data
    """

    def __init__(self, store: BigTableStore):
        self.store = store

    def build_risk_metrics_mart(self) -> int:
        """
        DBT model: mart__risk_metrics
        Computes 7-day rolling volatility, Sharpe ratio, risk classification
        """
        symbols_query = "SELECT DISTINCT symbol FROM ohlcv_records"
        symbols = [r[0] for r in self.store.conn.execute(symbols_query).fetchall()]

        metrics = []
        for symbol in symbols:
            rows = self.store.conn.execute("""
                SELECT close_price, volume, date
                FROM ohlcv_records
                WHERE symbol = ?
                ORDER BY date DESC, hour DESC
                LIMIT 168
            """, (symbol,)).fetchall()

            if len(rows) < 5:
                continue

            prices = [r[0] for r in rows]
            volumes = [r[1] for r in rows]

            returns = [(prices[i] - prices[i+1]) / prices[i+1]
                       for i in range(len(prices)-1)]

            volatility = statistics.stdev(returns) * (252 ** 0.5) if len(returns) > 1 else 0
            avg_volume = statistics.mean(volumes)
            price_change = ((prices[0] - prices[-1]) / prices[-1]) * 100 if prices[-1] else 0
            avg_return = statistics.mean(returns) if returns else 0
            std_return = statistics.stdev(returns) if len(returns) > 1 else 1
            sharpe = (avg_return / std_return) * (252 ** 0.5) if std_return else 0

            risk_level = (
                "HIGH" if volatility > 0.4 else
                "MEDIUM" if volatility > 0.2 else
                "LOW"
            )

            mid = hashlib.md5(f"{symbol}risk".encode()).hexdigest()[:16]
            metrics.append(RiskMetric(
                metric_id=mid,
                symbol=symbol,
                volatility_7d=round(volatility, 6),
                avg_volume_7d=round(avg_volume, 2),
                price_change_pct=round(price_change, 4),
                sharpe_ratio=round(sharpe, 4),
                risk_level=risk_level,
                computed_at=datetime.now().isoformat()
            ))

        self.store.insert_risk_metrics(metrics)
        return len(metrics)


# ─────────────────────────────────────────────
# AIRFLOW DAG ORCHESTRATOR
# ─────────────────────────────────────────────

class AirflowDAG:
    """
    Simulates Apache Airflow DAG execution
    Orchestrates the full ETL pipeline with task dependencies
    """

    DAG_ID = "financial_market_pipeline_v1"

    def __init__(self, store: BigTableStore):
        self.store = store
        self.ingestion = DataflowIngestion(store)
        self.transform = BigQueryTransform(store)
        self.dbt = DBTMartModels(store)
        self.run_id = hashlib.md5(datetime.now().isoformat().encode()).hexdigest()[:12]
        self.results = {}

    def _log(self, task_id, status, started, ended=None, rows_in=0, rows_out=0, error=None):
        self.store.log_pipeline_run(
            run_id=f"{self.run_id}_{task_id}",
            dag_id=self.DAG_ID,
            task_id=task_id,
            status=status,
            started_at=started,
            ended_at=ended or datetime.now().isoformat(),
            rows_in=rows_in,
            rows_out=rows_out,
            error_msg=error
        )

    def task_ingest(self):
        """Task 1: ingest_market_ticks (Dataflow)"""
        t0 = datetime.now().isoformat()
        try:
            count = self.ingestion.ingest_stream(days=30, ticks_per_hour=20)
            self._log("ingest_market_ticks", "SUCCESS", t0, rows_out=count)
            self.results["ingest"] = count
            return count
        except Exception as e:
            self._log("ingest_market_ticks", "FAILED", t0, error=str(e))
            raise

    def task_transform(self):
        """Task 2: transform_to_ohlcv (BigQuery)"""
        t0 = datetime.now().isoformat()
        try:
            count = self.transform.run_sql_transform()
            self._log("transform_to_ohlcv", "SUCCESS", t0,
                      rows_in=self.results.get("ingest", 0), rows_out=count)
            self.results["transform"] = count
            return count
        except Exception as e:
            self._log("transform_to_ohlcv", "FAILED", t0, error=str(e))
            raise

    def task_dbt(self):
        """Task 3: build_risk_mart (DBT)"""
        t0 = datetime.now().isoformat()
        try:
            count = self.dbt.build_risk_metrics_mart()
            self._log("build_risk_mart", "SUCCESS", t0,
                      rows_in=self.results.get("transform", 0), rows_out=count)
            self.results["dbt"] = count
            return count
        except Exception as e:
            self._log("build_risk_mart", "FAILED", t0, error=str(e))
            raise

    def run(self) -> Dict:
        """Execute full DAG: ingest → transform → dbt"""
        print(f"\n{'='*55}")
        print(f"  AIRFLOW DAG: {self.DAG_ID}")
        print(f"  Run ID: {self.run_id}")
        print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*55}")

        print("\n[Task 1/3] ingest_market_ticks  →  RUNNING...")
        n1 = self.task_ingest()
        print(f"           ✓ SUCCESS — {n1:,} ticks ingested (Dataflow)")

        print("\n[Task 2/3] transform_to_ohlcv   →  RUNNING...")
        n2 = self.task_transform()
        print(f"           ✓ SUCCESS — {n2:,} OHLCV records created (BigQuery SQL)")

        print("\n[Task 3/3] build_risk_mart       →  RUNNING...")
        n3 = self.task_dbt()
        print(f"           ✓ SUCCESS — {n3:,} risk metrics computed (DBT)")

        print(f"\n{'='*55}")
        print(f"  DAG RUN COMPLETE ✓")
        print(f"  Pipeline: {n1:,} → {n2:,} → {n3:,}")
        print(f"{'='*55}\n")

        return {
            "run_id": self.run_id,
            "dag_id": self.DAG_ID,
            "ticks_ingested": n1,
            "ohlcv_records": n2,
            "risk_metrics": n3,
            "status": "SUCCESS"
        }


# ─────────────────────────────────────────────
# EXPORT TO JSON (for dashboard)
# ─────────────────────────────────────────────

def export_dashboard_data(store: BigTableStore, output_path: str):
    """Export pipeline results to JSON for the dashboard"""

    ohlcv = store.conn.execute("""
        SELECT symbol, date, AVG(close_price) as price, SUM(volume) as volume
        FROM ohlcv_records
        GROUP BY symbol, date
        ORDER BY date DESC
        LIMIT 240
    """).fetchall()

    risk = store.conn.execute("""
        SELECT symbol, volatility_7d, avg_volume_7d,
               price_change_pct, sharpe_ratio, risk_level
        FROM risk_metrics
    """).fetchall()

    pipeline_runs = store.conn.execute("""
        SELECT dag_id, task_id, status, started_at, ended_at, rows_in, rows_out
        FROM pipeline_runs
        ORDER BY started_at DESC
    """).fetchall()

    data = {
        "generated_at": datetime.now().isoformat(),
        "ohlcv": [
            {"symbol": r[0], "date": r[1], "price": round(r[2], 4), "volume": r[3]}
            for r in ohlcv
        ],
        "risk_metrics": [
            {
                "symbol": r[0], "volatility_7d": r[1],
                "avg_volume_7d": r[2], "price_change_pct": r[3],
                "sharpe_ratio": r[4], "risk_level": r[5]
            }
            for r in risk
        ],
        "pipeline_runs": [
            {
                "dag_id": r[0], "task_id": r[1], "status": r[2],
                "started_at": r[3], "ended_at": r[4],
                "rows_in": r[5], "rows_out": r[6]
            }
            for r in pipeline_runs
        ]
    }

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Dashboard data exported → {output_path}")
    return data


# ─────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    DB_PATH = "data/pipeline.db"
    JSON_PATH = "data/dashboard_data.json"

    import os
    os.makedirs("data", exist_ok=True)

    store = BigTableStore(DB_PATH)
    dag = AirflowDAG(store)
    results = dag.run()

    data = export_dashboard_data(store, JSON_PATH)
    print(f"\nSummary:")
    print(f"  Symbols tracked : {len(set(r['symbol'] for r in data['ohlcv']))}")
    print(f"  OHLCV records   : {len(data['ohlcv'])}")
    print(f"  Risk metrics    : {len(data['risk_metrics'])}")
    print(f"  Pipeline tasks  : {len(data['pipeline_runs'])}")
