"""Data-quality metrics for report evidence and operational logging."""

from pyspark.sql import DataFrame, functions as F


def source_quality_summary(frame: DataFrame) -> DataFrame:
    """Summarise rejected and flagged records before cleaning removes them."""
    return frame.agg(
        F.count("*").alias("raw_record_count"),
        F.sum((~F.col("has_valid_crime_id")).cast("long")).alias("invalid_crime_id_count"),
        F.sum((~F.col("has_valid_timestamp")).cast("long")).alias("invalid_timestamp_count"),
        F.sum((~F.col("has_valid_coordinates")).cast("long")).alias("invalid_coordinate_count"),
        F.sum((~F.col("has_valid_district")).cast("long")).alias("invalid_district_count"),
        F.sum((~F.col("has_valid_community_area")).cast("long")).alias("invalid_community_area_count"),
    )


def quality_summary(frame: DataFrame) -> DataFrame:
    """Return one row of auditable quality metrics for cleaned crime data."""
    return frame.agg(
        F.count("*").alias("clean_record_count"),
        F.sum((~F.col("has_valid_crime_id")).cast("long")).alias("invalid_crime_id_count"),
        F.sum((~F.col("has_valid_coordinates")).cast("long")).alias("invalid_coordinate_count"),
        F.sum((~F.col("has_valid_district")).cast("long")).alias("invalid_district_count"),
        F.sum((~F.col("has_valid_community_area")).cast("long")).alias("invalid_community_area_count"),
        F.countDistinct("crime_id").alias("distinct_crime_id_count"),
    )
