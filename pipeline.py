#!/usr/bin/env python3
"""ETL Pipeline: kc_house_data.csv.zip → PostgreSQL kc_housing.house_sales"""

import os
import sys
import json
import zipfile
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# ─── Config ───────────────────────────────────────────────────────
DEFAULT_SOURCE = "/home/sindhuh/Downloads/kc_house_data.csv.zip"
DEFAULT_DB_URL = "postgresql://postgres:password@localhost:5432/kc_housing"
DEFAULT_TABLE = "house_sales"
CHUNK_SIZE = 5000
REPORTS_DIR = Path("/home/sindhuh/.openclaw/workspace/reports")


def extract(source_path: str) -> pd.DataFrame:
    """Extract data from zipped CSV."""
    print("📥 Extracting data...")
    if source_path.endswith(".zip"):
        with zipfile.ZipFile(source_path, "r") as z:
            csv_name = [f for f in z.namelist() if f.endswith(".csv")][0]
            with z.open(csv_name) as f:
                df = pd.read_csv(f)
    else:
        df = pd.read_csv(source_path)
    print(f"   Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform the data."""
    print("🔄 Transforming data...")

    # Normalize column names: lowercase, strip, replace spaces with underscores
    df.columns = [c.strip().lower().replace(" ", "_").replace('"', '') for c in df.columns]
    print(f"   Normalized {len(df.columns)} column names")

    # Parse date column
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%dT%H%M%S", errors="coerce")
        parsed = df["date"].notna().sum()
        print(f"   Parsed {parsed}/{len(df)} dates to timestamp")

    # Strip whitespace on string columns
    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        df[col] = df[col].str.strip()
    if len(str_cols):
        print(f"   Stripped whitespace on {len(str_cols)} string columns")

    # Optimize numeric types
    for col in df.select_dtypes(include=["int64"]).columns:
        if df[col].min() >= 0 and df[col].max() < 2**31:
            df[col] = df[col].astype("int32")
    for col in df.select_dtypes(include=["float64"]).columns:
        df[col] = df[col].astype("float32")
    print("   Optimized numeric types")

    # Drop exact duplicates
    before = len(df)
    df = df.drop_duplicates()
    dupes = before - len(df)
    if dupes:
        print(f"   Removed {dupes} duplicate rows")
    else:
        print("   No duplicate rows found")

    return df


def quality_check(df: pd.DataFrame) -> dict:
    """Run data quality checks and return report."""
    print("🔍 Running quality checks...")
    report = {
        "timestamp": datetime.now().isoformat(),
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "checks": []
    }

    # Null analysis
    nulls = df.isnull().sum()
    null_cols = nulls[nulls > 0]
    null_check = {
        "name": "Null Analysis",
        "status": "PASS" if len(null_cols) == 0 else "WARN",
        "details": {col: int(count) for col, count in null_cols.items()} if len(null_cols) > 0 else "No nulls found"
    }
    report["checks"].append(null_check)
    print(f"   Nulls: {len(null_cols)} columns with nulls" if len(null_cols) else "   Nulls: ✅ None")

    # Duplicate check on ID
    if "id" in df.columns:
        dup_ids = df["id"].duplicated().sum()
        id_check = {
            "name": "ID Uniqueness",
            "status": "WARN" if dup_ids > 0 else "PASS",
            "details": f"{dup_ids} duplicate IDs" if dup_ids else "All IDs unique"
        }
        report["checks"].append(id_check)
        print(f"   ID uniqueness: {'⚠️ ' + str(dup_ids) + ' duplicates' if dup_ids else '✅ All unique'}")

    # Price sanity
    if "price" in df.columns:
        neg_prices = (df["price"] <= 0).sum()
        price_check = {
            "name": "Price Validation",
            "status": "WARN" if neg_prices > 0 else "PASS",
            "details": {
                "min": float(df["price"].min()),
                "max": float(df["price"].max()),
                "mean": float(df["price"].mean()),
                "zero_or_negative": int(neg_prices)
            }
        }
        report["checks"].append(price_check)
        print(f"   Prices: ${df['price'].min():,.0f} – ${df['price'].max():,.0f} (avg ${df['price'].mean():,.0f})")

    # Date range
    if "date" in df.columns and pd.api.types.is_datetime64_any_dtype(df["date"]):
        date_check = {
            "name": "Date Range",
            "status": "PASS",
            "details": {
                "min": str(df["date"].min()),
                "max": str(df["date"].max()),
                "null_dates": int(df["date"].isnull().sum())
            }
        }
        report["checks"].append(date_check)
        print(f"   Date range: {df['date'].min().date()} → {df['date'].max().date()}")

    # Summary
    passed = sum(1 for c in report["checks"] if c["status"] == "PASS")
    warned = sum(1 for c in report["checks"] if c["status"] == "WARN")
    report["summary"] = {"passed": passed, "warnings": warned}
    print(f"   Summary: {passed} passed, {warned} warnings")

    return report


def generate_html_report(report: dict, output_path: Path):
    """Generate a styled HTML quality report."""
    checks_html = ""
    for check in report["checks"]:
        color = "#27ae60" if check["status"] == "PASS" else "#f39c12"
        icon = "✅" if check["status"] == "PASS" else "⚠️"
        details = check["details"] if isinstance(check["details"], str) else json.dumps(check["details"], indent=2)
        checks_html += f"""
        <div style="background:#f8f9fa;border-left:4px solid {color};padding:12px 16px;margin:8px 0;border-radius:4px;">
            <strong>{icon} {check['name']}</strong> <span style="color:{color};font-weight:bold;">[{check['status']}]</span>
            <pre style="margin:4px 0 0;font-size:13px;color:#555;">{details}</pre>
        </div>"""

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Data Quality Report</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; color: #333; }}
h1 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
.summary {{ display: flex; gap: 20px; margin: 20px 0; }}
.card {{ background: #fff; border: 1px solid #ddd; border-radius: 8px; padding: 20px; flex: 1; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }}
.card h2 {{ margin: 0; font-size: 28px; }}
.card p {{ margin: 4px 0 0; color: #777; }}
</style></head><body>
<h1>📊 Data Quality Report</h1>
<p>Generated: {report['timestamp']}</p>
<div class="summary">
    <div class="card"><h2>{report['row_count']:,}</h2><p>Rows</p></div>
    <div class="card"><h2>{report['column_count']}</h2><p>Columns</p></div>
    <div class="card"><h2>{report['summary']['passed']}</h2><p>Checks Passed</p></div>
    <div class="card"><h2>{report['summary']['warnings']}</h2><p>Warnings</p></div>
</div>
<h2>Checks</h2>
{checks_html}
</body></html>"""
    output_path.write_text(html)


def load(df: pd.DataFrame, db_url: str, table: str):
    """Load data into PostgreSQL."""
    print(f"📤 Loading {len(df)} rows into {table}...")
    engine = create_engine(db_url)

    # Test connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("   Connected to PostgreSQL")

    # Load in chunks
    df.to_sql(table, engine, if_exists="replace", index=False, chunksize=CHUNK_SIZE)
    print(f"   Loaded {len(df)} rows")

    # Verify
    with engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        sample = pd.read_sql(f"SELECT * FROM {table} LIMIT 3", conn)
    print(f"   Verified: {count} rows in {table}")
    print(f"\n   Sample rows:\n{sample.to_string(index=False)}")

    return count


def main():
    parser = argparse.ArgumentParser(description="ETL Pipeline: KC House Data → PostgreSQL")
    parser.add_argument("--source", default=DEFAULT_SOURCE, help="Source file path")
    parser.add_argument("--db-url", default=DEFAULT_DB_URL, help="PostgreSQL connection URL")
    parser.add_argument("--table", default=DEFAULT_TABLE, help="Target table name")
    args = parser.parse_args()

    print("=" * 60)
    print("🚀 ETL Pipeline: KC House Data → PostgreSQL")
    print("=" * 60)

    # Extract
    df = extract(args.source)

    # Transform
    df = transform(df)

    # Quality checks
    report = quality_check(df)

    # Save reports
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_json = REPORTS_DIR / "kc_housing_quality.json"
    report_html = REPORTS_DIR / "kc_housing_quality.html"
    with open(report_json, "w") as f:
        json.dump(report, f, indent=2, default=str)
    generate_html_report(report, report_html)
    print(f"\n📄 Reports saved:")
    print(f"   JSON: {report_json}")
    print(f"   HTML: {report_html}")

    # Load
    print()
    row_count = load(df, args.db_url, args.table)

    print("\n" + "=" * 60)
    print(f"✅ Pipeline complete! {row_count} rows → {args.table}")
    print("=" * 60)


if __name__ == "__main__":
    main()
