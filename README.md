# DAS7002 Chicago Crime Analysis

This project contains a reproducible PySpark pipeline for Task 1 of the DAS7002 practical assignment. It reads the Chicago Crime CSV, normalises the supported Chicago header variants, cleans and validates key fields, derives time columns, and writes partitioned Parquet data.

## Project layout

```text
data/raw/                 Input CSV files (not committed)
data/processed/           Generated Parquet output (not committed)
src/chicago_crime/        Reusable pipeline code
tests/                    Unit tests for pure transformation logic
```

## Run the pipeline

Install the project dependency in your notebook or virtual environment, then run:

```bash
python3 -m pip install -r requirements.txt
```

Then run:

```bash
PYTHONPATH=src python3 -m chicago_crime.run_pipeline \
  --input data/raw/chicago_crime.csv \
  --output data/processed/chicago_crime_parquet
```

The pipeline rejects records with an invalid incident timestamp or crime ID, reporting both counts before removal. Geographic defects are retained with transparent flags, so the report can explain what was found and how it was handled.

## Evidence to retain for the report

- Raw and cleaned row counts
- Data-quality summary printed by the pipeline
- Cleaning rules in `src/chicago_crime/transforms.py`
- A screenshot of the Parquet partition output by `year` and `district`
