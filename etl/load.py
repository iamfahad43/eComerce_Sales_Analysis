# etl/load.py
import logging
from pathlib import Path

import yaml
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus

# ─── LOGGING SETUP ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ─── CONFIG LOADING ─────────────────────────────────────────────────────────────
def load_config(path: str = "config.yaml") -> dict:
    """
    Read database connection settings from config.yaml in project root.
    Expected structure:
    db:
      dialect: postgresql
      host: localhost
      port: 5432
      database: ecommerce
      user: postgres
      password: your_password
    """
    project_root = Path(__file__).resolve().parent.parent
    cfg_path = project_root / path
    if not cfg_path.exists():
        raise FileNotFoundError(f"Could not find config file at {cfg_path}")
    return yaml.safe_load(cfg_path.read_text())

# ─── ENGINE CREATION ────────────────────────────────────────────────────────────
def get_engine(cfg: dict) -> Engine:
    db = cfg["db"]
    # URL-encode any special characters in the password
    pw = quote_plus(db["password"])
    uri = (
        f"{db['dialect']}://{db['user']}:{pw}"
        f"@{db['host']}:{db['port']}/{db['database']}"
    )
    logging.info(f"Connecting to {db['dialect']}://{db['host']}:{db['port']}/***")
    return create_engine(uri, echo=False)


# ─── DATA LOADING ───────────────────────────────────────────────────────────────
def load_parquet_to_sql(
    engine: Engine,
    parquet_path: Path,
    table_name: str,
    if_exists: str = "replace"
):
    """
    Read a Parquet file into pandas, then write to the target SQL table.
    Drops & recreates by default (if_exists='replace').
    """
    logging.info(f"Reading {parquet_path.name}")
    df = pd.read_parquet(parquet_path)

    logging.info(
        f"Writing {len(df):,} rows to table `{table_name}` (if_exists={if_exists})"
    )
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
        method="multi",      # faster batch inserts
        chunksize=1000       # adjust to your liking
    )
    logging.info(f"✔ Finished loading `{table_name}`")

# ─── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    # 1) Load DB config & create engine
    cfg    = load_config()
    engine = get_engine(cfg)

    # 2) Define paths to transformed data
    project_root = Path(__file__).resolve().parent.parent
    tx_dir       = project_root / "data" / "processed" / "transformed"

    # 3) Map parquet files to their SQL target names
    tables = {
        "dim_customers.parquet": "dim_customers",
        "dim_products.parquet":  "dim_products",
        "dim_date.parquet":      "dim_date",
        "fact_orders.parquet":   "fact_orders",
    }

    # 4) Load each into the database
    for fq, table in tables.items():
        pq = tx_dir / fq
        if not pq.exists():
            logging.warning(f"Skipping missing file: {pq}")
            continue
        load_parquet_to_sql(engine, pq, table_name=table)

    logging.info("🎉 All tables loaded into the database!")

if __name__ == "__main__":
    main()
