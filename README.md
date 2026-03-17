# ETL Pipeline — Kaggle Dataset → CSV

## Prompt

Build an ETL pipeline that:
1. **Ingests** a dataset from Kaggle (user provides the dataset slug, e.g. `zillow/zecon`)
2. **Transforms** the data: normalize column names, parse dates, handle nulls, remove duplicates, optimize types
3. **Outputs** the cleaned data as a CSV file

## Quick Start
```bash
pip install -r requirements.txt
```

## Requirements
- Python 3.10+
- Kaggle API credentials (`~/.kaggle/kaggle.json`)
- Dependencies listed in `requirements.txt`
