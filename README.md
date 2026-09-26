# Chicago Crime Analysis

A reproducible PySpark project for the DAS7002 practical assignment. It prepares Chicago crime, census, and weather data, then performs exploratory analysis, spatial-temporal clustering, and arrest-prediction modelling.

## What the project covers

- **Task 1 — Data preparation:** validates and cleans the source data, records quality metrics, quarantines invalid crime records, and writes partitioned Parquet files.
- **Task 2 — Exploratory analysis:** analyses temporal patterns, weather relationships, community-area trends, and socioeconomic indicators.
- **Task 3 — Clustering:** applies K-Means to identify spatial-temporal crime activity zones and evaluates cluster quality and stability.
- **Task 4 — Prediction:** compares classification models for arrest prediction and reports threshold, class-imbalance, and feature-importance metrics.

## Project structure

```text
.
├── data/
│   ├── raw/                 # Local source CSV files (not committed)
│   └── processed/           # Generated Parquet data (not committed)
├── reports/
│   ├── evidence/            # Report-ready CSV tables
│   └── figures/             # Selected charts
├── scripts/                 # Reproducible data-download utilities
├── src/chicago_crime/       # ETL and analysis modules
├── tests/                   # Unit and integration tests
├── requirements.txt
└── README.md
```

Generated task outputs are written to `outputs/` and are excluded from Git.

## Prerequisites

- Python 3.12
- Java 8, 11, or 17 available through `JAVA_HOME`
- Enough local memory and disk space for the selected crime-data extract

## Setup

Create and activate a virtual environment, then install the dependencies.

### PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
$env:PYTHONPATH = "src"
```

### macOS or Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
export PYTHONPATH=src
```

Set `PYTHONPATH` again when starting a new terminal session.

## Data

Download the following public datasets and save them with these names:

| Dataset | Local path | Source |
|---|---|---|
| Chicago Crimes, 2001 to present | `data/raw/chicago_crime.csv` | [Chicago Data Portal](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2) |
| Selected socioeconomic indicators | `data/raw/chicago_census.csv` | [Chicago Data Portal](https://data.cityofchicago.org/Health-Human-Services/Selected-socioeconomic-indicators-in-Chicago-2008/kn9c-c2s2) |
| ERA5 hourly weather data | `data/raw/chicago_weather.csv` | [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api) |

The analysis uses 43,848 hourly ERA5 records from one reanalysis grid cell for 2008–2012. The grid cell is treated as a city-wide weather proxy rather than a measurement for each crime location. ERA5 is documented in the [Copernicus Climate Data Store](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=overview).

Generate the weather file with the reproducible downloader:

```powershell
python scripts/download_open_meteo_weather.py --output data/raw/chicago_weather.csv
```

The request fixes the Chicago city-centre coordinate, ERA5 model, date range, `America/Chicago` time zone, and measurement units. Raw datasets are intentionally not committed.

## Run the analysis

Run the commands from the repository root after completing the setup.

### 1. Prepare the crime data

```powershell
python -m chicago_crime.run_pipeline `
  --input data/raw/chicago_crime.csv `
  --output data/processed/chicago_crime_parquet `
  --quality-output data/processed/task1_crime_quality `
  --quarantine-output data/processed/task1_quarantine `
  --dictionary-output data/processed/task1_data_dictionary
```

The cleaned crime data is partitioned by `year` and `district`. Records with an invalid crime ID or timestamp are excluded and written to the quarantine output; geographic issues are retained with quality flags.

### 2. Prepare census and weather data

```powershell
python -m chicago_crime.task1 `
  --census-input data/raw/chicago_census.csv `
  --census-output data/processed/chicago_census_parquet `
  --weather-input data/raw/chicago_weather.csv `
  --weather-output data/processed/chicago_weather_parquet `
  --weather-quality-output data/processed/task1_weather_quality
```

### 3. Run exploratory analysis

```powershell
python -m chicago_crime.run_tasks eda `
  --crime-input data/processed/chicago_crime_parquet `
  --weather-input data/processed/chicago_weather_parquet `
  --census-input data/processed/chicago_census_parquet `
  --output-dir outputs
```

### 4. Run clustering

```powershell
python -m chicago_crime.run_tasks cluster `
  --crime-input data/processed/chicago_crime_parquet `
  --output-dir outputs
```

### 5. Run arrest-prediction modelling

```powershell
python -m chicago_crime.run_tasks model `
  --crime-input data/processed/chicago_crime_parquet `
  --output-dir outputs
```

On macOS or Linux, replace PowerShell's backtick line continuations with `\`.

## Export report evidence

After running the required tasks, export their compact Parquet results as CSV files:

```powershell
python -m chicago_crime.export_evidence --tasks task1 task2 task3 task4
```

You may list only completed tasks, for example `--tasks task1 task2`. Files are written to `reports/evidence/` by default.

## Outputs

| Path | Contents |
|---|---|
| `data/processed/` | Cleaned and quality-control Parquet datasets |
| `outputs/task2/` | EDA result tables |
| `outputs/task3/` | Cluster scores, summaries, and sampled assignments |
| `outputs/task4/` | Predictions, evaluation tables, and trained models |
| `outputs/charts/` | Charts produced by Tasks 2–4 |
| `reports/evidence/` | Small CSV tables for reporting |
| `reports/figures/` | Selected figures retained in Git |

## Testing

Run the automated test suite with:

```powershell
python -m pytest -q
```

The tests cover schema validation, transformations, the fixed ERA5 request, weather parsing, Task 2 outputs, clustering, modelling, and the partitioned-Parquet pipeline.

## Spark configuration

The application uses `local[4]`, 2 GB of driver memory, and the `America/Chicago` time zone by default. Override the local Spark settings when needed:

```powershell
$env:DAS7002_SPARK_MASTER = "local[2]"
$env:DAS7002_SPARK_DRIVER_MEMORY = "4g"
```

## Notes

- Pipeline outputs are overwritten by default. Pass `--fail-if-exists` to the crime preparation command to prevent replacement of an existing output.
- Keep raw data, full predictions, Spark metadata, and trained models out of source control; the repository's `.gitignore` already excludes them.
- The committed evidence tables and figures allow the main findings to be reviewed without rerunning the full workflow.
