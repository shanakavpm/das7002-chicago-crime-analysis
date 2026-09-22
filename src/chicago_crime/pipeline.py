"""Orchestration layer: read, transform, report quality, and write output."""

import os
import sys

from pyspark.sql import SparkSession

from .config import PipelineConfig
from .quality import quality_summary, source_quality_summary
from .schema import validate_required_source_columns
from .transforms import clean_prepared_records, normalize_source_columns, prepare_crime_records


def create_spark_session(app_name: str) -> SparkSession:
    """Create the one Spark dependency required by the application boundary."""
    # Prevent a virtual-environment driver from launching workers with the
    # system Python, which PySpark rejects when their minor versions differ.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    return SparkSession.builder.appName(app_name).getOrCreate()


def run_pipeline(config: PipelineConfig) -> None:
    """Run Task 1 ETL and write data partitioned for analytical queries."""
    spark = create_spark_session(config.app_name)
    try:
        raw_frame = (
            spark.read.option("header", True)
            .option("mode", "PERMISSIVE")
            .csv(str(config.input_path))
            .transform(normalize_source_columns)
        )
        validate_required_source_columns(raw_frame)
        prepared_frame = prepare_crime_records(raw_frame)
        clean_frame = clean_prepared_records(prepared_frame)

        print("Source data-quality summary:")
        source_quality_summary(prepared_frame).show(truncate=False)
        print("Cleaned data-quality summary:")
        quality_summary(clean_frame).show(truncate=False)

        write_mode = "overwrite" if config.overwrite_output else "errorifexists"
        (
            clean_frame.write.mode(write_mode)
            .partitionBy("year", "district")
            .parquet(str(config.output_path))
        )
        print(f"Partitioned Parquet saved to: {config.output_path}")
    finally:
        spark.stop()
