# scripts/analysis/analytics.py

import yaml
from pathlib import Path
from urllib.parse import quote_plus
from sqlalchemy import create_engine
import pandas as pd

def get_engine():
    cfg = yaml.safe_load(Path("config.yaml").read_text())["db"]
    pw  = quote_plus(cfg["password"])
    uri = f"{cfg['dialect']}://{cfg['user']}:{pw}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    return create_engine(uri)

def monthly_revenue(engine):
    """
    Returns a DataFrame with year, month, and total revenue.
    """
    sql = """
    SELECT d.year,
           d.month,
           SUM(f.line_total) AS revenue
    FROM fact_orders f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month;
    """
    return pd.read_sql(sql, engine)

def top_products(engine, top_n=10):
    """
    Returns top N products by total sales revenue.
    """
    sql = f"""
    SELECT p."Description"   AS description,
           SUM(f.line_total) AS revenue
    FROM fact_orders f
    JOIN dim_products p
        ON f.product_key = p.product_key
    GROUP BY p."Description"
    ORDER BY revenue DESC
    LIMIT {top_n};
    """
    return pd.read_sql(sql, engine)

if __name__ == "__main__":
    eng = get_engine()
    print("Monthly Revenue:\n", monthly_revenue(eng))
    print("\nTop 10 Products:\n", top_products(eng))
