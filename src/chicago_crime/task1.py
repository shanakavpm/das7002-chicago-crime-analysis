"""Task 1 command entry point for the secondary census and weather ETL outputs."""

import argparse
import logging
from pathlib import Path

from .census import clean_census_records
from .logging_utils import configure_logging
from .pipeline import create_spark_session
from .weather import aggregate_hourly_weather, clean_weather_records, weather_quality_summary


LOGGER = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """Parse Task 1 census, weather, quality, and output paths."""
    parser = argparse.ArgumentParser(
        description="Clean census and weather datasets for DAS7002 Task 1."
    )
    parser.add_argument(
        "--census-input",
        required=True,
        type=Path,
        help="Path to the raw census CSV.",
    )
    parser.add_argument(
        "--census-output",
        required=True,
        type=Path,
        help="Directory for cleaned census Parquet output.",
    )
    parser.add_argument(
        "--weather-input",
        required=True,
        type=Path,
        help="Path to the raw ERA5 weather CSV.",
    )
    parser.add_argument(
        "--weather-output",
        required=True,
        type=Path,
        help="Directory for cleaned hourly weather Parquet.",
    )
    parser.add_argument(
        "--weather-quality-output",
        required=True,
        type=Path,
        help="Directory for the Task 1 weather data-quality summary.",
    )
    args = parser.parse_args()
    return args


def run() -> None:
    """Clean the census lookup and ERA5 weather data used by Task 2."""
    configure_logging()
    args = parse_arguments()
    spark = create_spark_session("DAS7002-Chicago-Census-ETL")
    try:
        raw_census = spark.read.option("header", True).option("mode", "PERMISSIVE").csv(
            str(args.census_input)
        )
        clean_census = clean_census_records(raw_census)
        LOGGER.info("raw_census_record_count count=%d", raw_census.count())
        LOGGER.info("clean_community_area_count count=%d", clean_census.count())
        clean_census.write.mode("overwrite").parquet(str(args.census_output))
        LOGGER.info("cleaned_census_parquet_saved path=%s", args.census_output)

        raw_weather = spark.read.option("header", True).option("mode", "PERMISSIVE").csv(
            str(args.weather_input)
        )
        hourly_weather = aggregate_hourly_weather(clean_weather_records(raw_weather)).cache()
        weather_quality = weather_quality_summary(raw_weather, hourly_weather)
        hourly_weather.write.mode("overwrite").parquet(str(args.weather_output))
        weather_quality.write.mode("overwrite").parquet(str(args.weather_quality_output))
        LOGGER.info("task1_weather_quality_summary")
        weather_quality.show(truncate=False)
        hourly_weather.unpersist()
        LOGGER.info("cleaned_weather_parquet_saved path=%s", args.weather_output)
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
