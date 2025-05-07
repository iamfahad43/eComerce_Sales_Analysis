# etl/extract.py
import logging
from pathlib import Path

import yaml
import pandas as pd

# ─── CONFIG & LOGGING ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def load_config(filename: str = "config.yaml") -> dict:
    """
    Locate and load the YAML config from the project root.
    We use this for DB credentials or file‐path settings down the line.
    """
    project_root = Path(__file__).resolve().parent.parent
    config_path = project_root / filename
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at {config_path}")
    return yaml.safe_load(config_path.read_text())

def extract(excel_path: Path, sheet_name=0) -> pd.DataFrame:
    """
    Read the raw Excel file into a pandas DataFrame.
    We’ll clean up dtypes here so the next step (to_parquet) doesn’t choke.
    """
    logging.info(f"▶ Reading raw data from {excel_path}")
    df = pd.read_excel(excel_path, sheet_name=sheet_name, engine="openpyxl")
    logging.info(f"✔ Loaded {len(df):,} rows × {len(df.columns)} columns")
    
    # ————————————————————————————————————————————————————————————
    # FORCE ALL “object” COLUMNS TO STRINGS TO AVOID PYARROW ERRORS
    # (This covers InvoiceNo, StockCode, Description, etc.)
    obj_cols = df.select_dtypes(include=["object"]).columns
    for col in obj_cols:
        df[col] = df[col].astype(str)

    # Fix mixed‐type columns:
    # InvoiceNo has letter prefixes (e.g. 'C536379'), so force it to string.
    df["InvoiceNo"] = df["InvoiceNo"].astype(str)
    df["StockCode"]   = df["StockCode"].astype(str)

    # CustomerID is numeric but may have NaNs: use pandas' nullable integer type
    if "CustomerID" in df.columns:
        df["CustomerID"] = df["CustomerID"].astype("Int64")

    return df

def save_staging(df: pd.DataFrame, out_path: Path):
    """
    Write the cleaned DataFrame out as a Parquet file for the transform step.
    Parquet is fast, columnar, and preserves nulls & types nicely.
    """
    logging.info(f"▶ Writing staging file to {out_path}")
    df.to_parquet(out_path, index=False)
    logging.info("✔ Staging file saved.")

def main():
    # 1) Load config (not yet used here, but this is where DB creds would come in)
    cfg = load_config()

    # 2) Build our paths off the project root (nice and portable)
    project_root = Path(__file__).resolve().parent.parent
    raw_excel     = project_root / "data" / "raw" / "Online Retail.xlsx"
    staging_file  = project_root / "data" / "processed" / "orders_staging.parquet"

    # 3) Extract + clean dtypes
    df = extract(raw_excel)

    # 4) Save to Parquet for the next step
    save_staging(df, staging_file)

if __name__ == "__main__":
    main()
