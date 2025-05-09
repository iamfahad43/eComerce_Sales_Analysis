# etl/transform.py
import logging
from pathlib import Path

import pandas as pd

# ─── LOGGING ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ─── HELPERS ───────────────────────────────────────────────────────────────────

def load_staging(path: Path) -> pd.DataFrame:
    """
    Load the staging Parquet file into a DataFrame.
    This has all raw orders with cleaned dtypes.
    """
    logging.info(f"Loading staging data from {path}")
    df = pd.read_parquet(path)
    logging.info(f"Staging contains {len(df):,} rows")
    return df

def create_dim_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build customer dimension:
    - Unique CustomerID
    - Country (and any other customer attributes)
    """
    logging.info("Building dim_customers")
    dim = (
        df[["CustomerID", "Country"]]
        .dropna(subset=["CustomerID"])         # drop guest orders
        .drop_duplicates("CustomerID")
        .reset_index(drop=True)
    )
    # Optionally add an auto-increment surrogate key:
    dim["customer_key"] = dim.index + 1
    logging.info(f"dim_customers has {len(dim):,} rows")
    return dim

def create_dim_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build product dimension:
    - Unique StockCode
    - Description
    """
    logging.info("Building dim_products")
    dim = (
        df[["StockCode", "Description"]]
        .drop_duplicates("StockCode")
        .reset_index(drop=True)
    )
    dim["product_key"] = dim.index + 1
    logging.info(f"dim_products has {len(dim):,} rows")
    return dim

def create_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a date dimension from InvoiceDate:
    - Extract year, month, day, weekday, etc.
    - Unique on the date portion only
    """
    logging.info("Building dim_date")
    # Ensure InvoiceDate is datetime
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    # Extract date portion
    dates = df["InvoiceDate"].dt.normalize().drop_duplicates().sort_values()
    dim = pd.DataFrame({"invoice_date": dates})
    # Derive attributes
    dim["date_key"]   = dim.index + 1
    dim["year"]       = dim["invoice_date"].dt.year
    dim["month"]      = dim["invoice_date"].dt.month
    dim["day"]        = dim["invoice_date"].dt.day
    dim["weekday"]    = dim["invoice_date"].dt.weekday  # Monday=0
    dim["month_name"] = dim["invoice_date"].dt.month_name()
    logging.info(f"dim_date has {len(dim):,} rows")
    return dim

def create_fact_orders(
    df: pd.DataFrame,
    dim_cust: pd.DataFrame,
    dim_prod: pd.DataFrame,
    dim_date: pd.DataFrame
) -> pd.DataFrame:
    """
    Build the fact_orders table, joining in surrogate keys:
    - customer_key, product_key, date_key
    - preserve measures: Quantity, UnitPrice, compute line_total
    """
    logging.info("Building fact_orders")
    fact = df.copy()

    # Join on natural keys to get surrogate keys
    fact = fact.merge(
        dim_cust[["CustomerID", "customer_key"]],
        how="left", on="CustomerID"
    )
    fact = fact.merge(
        dim_prod[["StockCode", "product_key"]],
        how="left", on="StockCode"
    )
    # For date, match on the normalized date
    fact["invoice_date"] = pd.to_datetime(fact["InvoiceDate"]).dt.normalize()
    fact = fact.merge(
        dim_date[["invoice_date", "date_key"]],
        how="left", on="invoice_date"
    )

    # Compute measures
    fact["line_total"] = fact["Quantity"] * fact["UnitPrice"]

    # Select/finalize columns
    fact_final = fact[[
        "invoice_date",  # optional if you want
        "date_key",
        "customer_key",
        "product_key",
        "InvoiceNo",
        "Quantity",
        "UnitPrice",
        "line_total"
    ]].reset_index(drop=True)

    logging.info(f"fact_orders has {len(fact_final):,} rows")
    return fact_final

def save_table(df: pd.DataFrame, path: Path):
    """
    Persist a DataFrame to Parquet at the given path.
    """
    logging.info(f"Saving {path.name} ({len(df):,} rows)")
    df.to_parquet(path, index=False)

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    project_root = Path(__file__).resolve().parent.parent
    staging_path = project_root / "data" / "processed" / "orders_staging.parquet"
    out_dir      = project_root / "data" / "processed" / "transformed"

    df_staging = load_staging(staging_path)

    # Build dimensions
    dim_customers = create_dim_customers(df_staging)
    dim_products  = create_dim_products(df_staging)
    dim_date      = create_dim_date(df_staging)

    # Build fact
    fact_orders   = create_fact_orders(df_staging, dim_customers, dim_products, dim_date)

    # Save all tables
    save_table(dim_customers, out_dir / "dim_customers.parquet")
    save_table(dim_products,  out_dir / "dim_products.parquet")
    save_table(dim_date,      out_dir / "dim_date.parquet")
    save_table(fact_orders,   out_dir / "fact_orders.parquet")

    logging.info("🎉 Transform complete!")

if __name__ == "__main__":
    main()
