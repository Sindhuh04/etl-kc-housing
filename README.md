# ETL Pipeline: KC House Data → PostgreSQL

Auto-generated data pipeline for King County house sales data.

## Quick Start
```bash
pip install -r requirements.txt
python3 pipeline.py --help
```

## Pipeline Details
- **Source:** `kc_house_data.csv.zip` (21,613 rows, 21 columns)
- **Destination:** PostgreSQL `kc_housing.house_sales`
- **Transformations:** Date parsing, column normalization, type optimization
- **Quality Checks:** Null analysis, ID uniqueness, price validation, date range

## Usage
```bash
# Run with defaults
python3 pipeline.py

# Custom source/destination
python3 pipeline.py --source /path/to/data.csv --db-url postgresql://user:pass@host:5432/dbname --table my_table
```
