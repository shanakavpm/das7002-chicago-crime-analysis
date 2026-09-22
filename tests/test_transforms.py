"""Tests for the core business rules of the Chicago Crime ETL pipeline."""

from pyspark.sql.types import StringType, StructField, StructType

from chicago_crime.transforms import clean_crime_records, prepare_crime_records


RAW_TEST_COLUMNS = [
    "ID",
    "Date",
    "District",
    "Community Area",
    "Latitude",
    "Longitude",
    "Arrest",
    "Domestic",
    "Primary Type",
]
RAW_TEST_SCHEMA = StructType([StructField(column, StringType(), True) for column in RAW_TEST_COLUMNS])


def create_raw_frame(spark, rows):
    """Build a raw CSV-like frame, including columns that contain only nulls."""
    return spark.createDataFrame(rows, RAW_TEST_SCHEMA)


def test_cleaning_removes_bad_timestamp_and_duplicate_id(spark):
    rows = [
        ("1", "01/02/2012 01:30:00 PM", "12", "7", "41.88", "-87.63", "true", "false", "THEFT"),
        ("1", "01/02/2012 01:30:00 PM", "12", "7", "41.88", "-87.63", "true", "false", "THEFT"),
        ("2", "not-a-date", "12", "7", "41.88", "-87.63", "false", "false", "BATTERY"),
    ]
    cleaned = clean_crime_records(create_raw_frame(spark, rows))

    assert cleaned.count() == 1
    assert cleaned.first().year == 2012
    assert cleaned.first().arrest is True


def test_missing_geographic_fields_are_marked_invalid(spark):
    rows = [("1", "01/02/2012 01:30:00 PM", None, None, None, None, "true", "false", "THEFT")]
    prepared = prepare_crime_records(create_raw_frame(spark, rows)).first()

    assert prepared.has_valid_coordinates is False
    assert prepared.has_valid_district is False
    assert prepared.has_valid_community_area is False


def test_records_without_valid_ids_are_rejected_before_deduplication(spark):
    rows = [
        (None, "01/02/2012 01:30:00 PM", "12", "7", "41.88", "-87.63", "true", "false", "THEFT"),
        (None, "01/03/2012 01:30:00 PM", "12", "7", "41.88", "-87.63", "true", "false", "BATTERY"),
    ]
    cleaned = clean_crime_records(create_raw_frame(spark, rows))

    assert cleaned.count() == 0
