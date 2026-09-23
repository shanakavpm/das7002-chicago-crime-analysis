"""Orchestration layer: read, transform, report quality, and write output."""

import os
import sys
import logging

from pyspark.sql import SparkSession, functions as F

from .config import PipelineConfig
from .logging_utils import configure_logging
from .quality import quality_summary, source_quality_summary
from .schema import create_crime_data_dictionary, validate_required_source_columns
from .transforms import clean_prepared_records, normalize_source_columns, prepare_crime_records


LOGGER = logging.getLogger(__name__)


def create_spark_session(app_name: str) -> SparkSession:
    """Create the one Spark dependency required by the application boundary."""
    # Prevent a virtual-environment driver from launching workers with the
    # system Python, which PySpark rejects when their minor versions differ.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark_master = os.environ.get("DAS7002_SPARK_MASTER", "local[4]")
    driver_memory = os.environ.get("DAS7002_SPARK_DRIVER_MEMORY", "2g")
    return (
        SparkSession.builder.master(spark_master)
        .appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.session.timeZone", "America/Chicago")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def run_pipeline(config: PipelineConfig) -> None:
    """Run Task 1 ETL and write data partitioned for analytical queries."""
    configure_logging()
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

        source_summary = source_quality_summary(prepared_frame)
        clean_summary = quality_summary(clean_frame)
        combined_quality_summary = source_summary.crossJoin(clean_summary).cache()

        LOGGER.info("task1_crime_quality_summary")
        combined_quality_summary.show(truncate=False)

        write_mode = "overwrite" if config.overwrite_output else "errorifexists"
        (
            clean_frame.write.mode(write_mode)
            .partitionBy("year", "district")
            .parquet(str(config.output_path))
        )
        if config.quality_output_path:
            combined_quality_summary.write.mode(write_mode).parquet(
                str(config.quality_output_path)
            )
        if config.quarantine_output_path:
            rejected_frame = prepared_frame.filter(
                ~F.col("has_valid_crime_id") | ~F.col("has_valid_timestamp")
            ).withColumn(
                "rejection_reason",
                F.concat_ws(
                    ",",
                    F.when(~F.col("has_valid_crime_id"), F.lit("invalid_crime_id")),
                    F.when(~F.col("has_valid_timestamp"), F.lit("invalid_timestamp")),
                ),
            )
            rejected_frame.write.mode(write_mode).parquet(str(config.quarantine_output_path))
        if config.dictionary_output_path:
            create_crime_data_dictionary(spark).write.mode(write_mode).parquet(
                str(config.dictionary_output_path)
            )
        combined_quality_summary.unpersist()
        LOGGER.info("partitioned_crime_parquet_saved path=%s", config.output_path)
    finally:
        spark.stop()
