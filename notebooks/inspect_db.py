# notebooks/inspect_db.py

import yaml
from pathlib import Path
from urllib.parse import quote_plus
from sqlalchemy import create_engine, inspect
import pandas as pd

# ─── CONFIG & CONNECTION ────────────────────────────────────────────────────────

# Resolve project root (two levels up from this file)
project_root = Path(__file__).resolve().parent.parent

cfg = yaml.safe_load((project_root / "config.yaml").read_text())["db"]
pw  = quote_plus(cfg["password"])
uri = (
    f"{cfg['dialect']}://{cfg['user']}:{pw}"
    f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
)

engine = create_engine(uri)

# ─── TABLE LISTING ──────────────────────────────────────────────────────────────

inspector = inspect(engine)
tables = inspector.get_table_names()
print("Tables in ecommerce:", tables)

# ─── DATA PREVIEW ───────────────────────────────────────────────────────────────

# Define a helper to load & show a few rows
def preview(table_name: str, n: int = 5) -> pd.DataFrame:
    df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT {n}", engine)
    display(df)  # in Interactive Window this pops up the Data Viewer
    return df

# Preview each of the four tables
dim_customers = preview("dim_customers")
dim_products  = preview("dim_products")
dim_date      = preview("dim_date")
fact_orders   = preview("fact_orders")
