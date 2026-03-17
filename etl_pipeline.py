"""
ETL Pipeline — KC Housing Dataset
Source: Public KC House Data (21,613 records)
Task: Ingest → Transform → Output clean CSV
"""

import pandas as pd
import numpy as np
import os
import re
from datetime import datetime

# ─────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────
def extract(filepath):
    print(f"[EXTRACT] Loading dataset from: {filepath}")
    df = pd.read_csv(filepath)
    print(f"[EXTRACT] Raw shape: {df.shape}")
    print(f"[EXTRACT] Columns: {list(df.columns)}")
    return df

# ─────────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────────
def normalize_column_names(df):
    """Lowercase, strip spaces, replace special chars with underscores"""
    df.columns = [
        re.sub(r'[^a-z0-9_]', '_', col.strip().lower())
        for col in df.columns
    ]
    print(f"[TRANSFORM] Normalized column names: {list(df.columns)}")
    return df

def parse_dates(df):
    """Parse date columns to datetime"""
    date_cols = [col for col in df.columns if 'date' in col]
    for col in date_cols:
        try:
            # KC Housing date format: 20141013T000000
            df[col] = pd.to_datetime(df[col], format='%Y%m%dT%H%M%S', errors='coerce')
            print(f"[TRANSFORM] Parsed date column: '{col}'")
        except Exception as e:
            print(f"[TRANSFORM] Could not parse '{col}': {e}")
    return df

def handle_nulls(df):
    """Report and fill/drop nulls"""
    null_counts = df.isnull().sum()
    total_nulls = null_counts.sum()
    print(f"[TRANSFORM] Total null values: {total_nulls}")
    if total_nulls > 0:
        print(null_counts[null_counts > 0])
        # Fill numeric nulls with median
        num_cols = df.select_dtypes(include=[np.number]).columns
        for col in num_cols:
            if df[col].isnull().sum() > 0:
                median_val = df[col].median()
                df[col].fillna(median_val, inplace=True)
                print(f"[TRANSFORM] Filled nulls in '{col}' with median ({median_val})")
        # Fill object nulls with 'Unknown'
        obj_cols = df.select_dtypes(include=['object']).columns
        for col in obj_cols:
            if df[col].isnull().sum() > 0:
                df[col].fillna('Unknown', inplace=True)
                print(f"[TRANSFORM] Filled nulls in '{col}' with 'Unknown'")
    else:
        print("[TRANSFORM] No nulls found — clean dataset!")
    return df

def remove_duplicates(df):
    """Drop exact duplicate rows"""
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    removed = before - after
    print(f"[TRANSFORM] Removed {removed} duplicate rows ({before} → {after})")
    return df

def optimize_types(df):
    """Downcast numeric types to reduce memory"""
    before_mem = df.memory_usage(deep=True).sum() / 1024 / 1024

    # Downcast integers
    int_cols = df.select_dtypes(include=['int64', 'int32']).columns
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], downcast='integer')

    # Downcast floats
    float_cols = df.select_dtypes(include=['float64']).columns
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], downcast='float')

    after_mem = df.memory_usage(deep=True).sum() / 1024 / 1024
    print(f"[TRANSFORM] Memory optimized: {before_mem:.2f} MB → {after_mem:.2f} MB")
    return df

def add_derived_features(df):
    """Add useful derived columns"""
    # House age at time of sale
    if 'yr_built' in df.columns and 'date' in df.columns:
        df['house_age_at_sale'] = df['date'].dt.year - df['yr_built']
        print("[TRANSFORM] Added 'house_age_at_sale'")

    # Was renovated flag
    if 'yr_renovated' in df.columns:
        df['was_renovated'] = (df['yr_renovated'] > 0).astype('int8')
        print("[TRANSFORM] Added 'was_renovated'")

    # Price per sqft
    if 'price' in df.columns and 'sqft_living' in df.columns:
        df['price_per_sqft'] = (df['price'] / df['sqft_living']).round(2)
        print("[TRANSFORM] Added 'price_per_sqft'")

    return df

def transform(df):
    print("\n[TRANSFORM] Starting transformation pipeline...")
    df = normalize_column_names(df)
    df = parse_dates(df)
    df = handle_nulls(df)
    df = remove_duplicates(df)
    df = optimize_types(df)
    df = add_derived_features(df)
    print(f"[TRANSFORM] Final shape: {df.shape}")
    print(f"[TRANSFORM] Dtypes:\n{df.dtypes}")
    return df

# ─────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────
def load(df, output_path):
    df.to_csv(output_path, index=False)
    size_kb = os.path.getsize(output_path) / 1024
    print(f"\n[LOAD] Saved cleaned data to: {output_path}")
    print(f"[LOAD] Output size: {size_kb:.1f} KB")
    print(f"[LOAD] Rows: {len(df):,} | Columns: {len(df.columns)}")
    return output_path

# ─────────────────────────────────────────────
# PIPELINE SUMMARY
# ─────────────────────────────────────────────
def print_summary(df):
    print("\n" + "="*60)
    print("ETL PIPELINE SUMMARY")
    print("="*60)
    print(f"Total records       : {len(df):,}")
    print(f"Total features      : {len(df.columns)}")
    print(f"Price range         : ${df['price'].min():,.0f} — ${df['price'].max():,.0f}")
    print(f"Avg price           : ${df['price'].mean():,.0f}")
    print(f"Avg price/sqft      : ${df['price_per_sqft'].mean():,.2f}" if 'price_per_sqft' in df.columns else "")
    print(f"Date range          : {df['date'].min().date()} to {df['date'].max().date()}" if 'date' in df.columns else "")
    print(f"% Renovated         : {df['was_renovated'].mean()*100:.1f}%" if 'was_renovated' in df.columns else "")
    print("="*60)

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    base = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(base, "kc_house_data_raw.csv")
    output_path = os.path.join(base, "kc_house_data_cleaned.csv")

    start = datetime.now()
    print("="*60)
    print("KC HOUSING ETL PIPELINE")
    print(f"Started: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    df_raw = extract(input_path)
    df_clean = transform(df_raw)
    load(df_clean, output_path)
    print_summary(df_clean)

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n✅ Pipeline completed in {elapsed:.2f} seconds")
