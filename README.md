# DAS7002 Chicago Crime Analysis

This project contains a reproducible PySpark workflow for all four DAS7002 practical-assignment tasks. It cleans Chicago Crime and socioeconomic data, performs weather and spatial EDA, clusters spatial-temporal activity zones, and evaluates a Random Forest arrest-prediction model.

## Project layout

```text
data/raw/                 Input CSV files (not committed)
data/processed/           Generated Parquet output (not committed)
outputs/                  Analytical tables, charts, and trained model
reports/evidence/         Compact CSV evidence tables
src/chicago_crime/        Reusable pipeline code
tests/                    Unit and integration tests
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
  --output data/processed/chicago_crime_parquet \
  --quality-output data/processed/task1_crime_quality \
  --quarantine-output data/processed/task1_quarantine \
  --dictionary-output data/processed/task1_data_dictionary
```

The pipeline rejects records with an invalid incident timestamp or crime ID, reporting both counts before removal. Geographic defects are retained with transparent flags, so the report can explain what was found and how it was handled.

## Evidence to retain for the report

- Raw and cleaned row counts
- Data-quality summary printed by the pipeline
- Cleaning rules in `src/chicago_crime/transforms.py`
- A screenshot of the Parquet partition output by `year` and `district`

## Task commands

Clean the census lookup and NOAA weather data as Task 1 secondary ETL outputs:

```bash
PYTHONPATH=src python3 -m chicago_crime.task1 \
  --census-input data/raw/chicago_census.csv \
  --census-output data/processed/chicago_census_parquet \
  --weather-input data/raw/chicago_weather.csv \
  --weather-output data/processed/chicago_weather_parquet \
  --weather-quality-output data/processed/task1_weather_quality
```

Run spatial-temporal K-Means clustering for Task 3:

```bash
PYTHONPATH=src python3 -m chicago_crime.run_tasks cluster \
  --crime-input data/processed/chicago_crime_parquet \
  --output-dir outputs
```

Run Task 4 arrest prediction with Random Forest:

```bash
PYTHONPATH=src python3 -m chicago_crime.run_tasks model \
  --crime-input data/processed/chicago_crime_parquet \
  --output-dir outputs
```

Task outputs are saved as Parquet for use in charts and tables. The model uses `Arrest` as its binary target and compares a majority-class baseline, unweighted Random Forest, class-weighted Random Forest, alternative probability thresholds, and a time-based holdout. ROC-AUC, PR-AUC, precision, recall, F1, balanced accuracy, feature importance, and class-skew evidence are retained.

Task 3 requires a crime timestamp with a real time-of-day component. It selects K from a reproducible 0.2% training sample, records silhouette and training-cost evidence for every candidate, then assigns clusters and calculates concentration and district-alignment summaries across all valid records.

Run Task 2 with the cleaned NOAA Chicago Midway Airport output from Task 1. The station is a documented city-wide weather proxy; crime latitude and longitude remain separate inputs for Task 3 spatial analysis.

```bash
PYTHONPATH=src python3 -m chicago_crime.run_tasks eda \
  --crime-input data/processed/chicago_crime_parquet \
  --weather-input data/processed/chicago_weather_parquet \
  --census-input data/processed/chicago_census_parquet \
  --output-dir outputs
```

Task 2 writes explicit PySpark SQL temporal patterns, daily crime-weather counts, seasonal weather comparisons, effect sizes, weekday/weekend patterns, correlations, 7-day community-area crime averages, and census-enriched spatial summaries to `outputs/task2`.

Local Spark defaults to four workers and a 2 GB driver heap. Override these settings when needed with `DAS7002_SPARK_MASTER` and `DAS7002_SPARK_DRIVER_MEMORY`.

Export the compact tables for all completed tasks as ordinary CSV files for Excel and the report:

```bash
PYTHONPATH=src python3 -m chicago_crime.export_evidence --tasks task1 task2 task3 task4
```
