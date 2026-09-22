"""Pure, composable DataFrame transformations for Chicago Crime records."""

from pyspark.sql import DataFrame, functions as F


INCIDENT_TIMESTAMP_FORMATS = (
    "MM/dd/yyyy hh:mm:ss a",
    "MM/dd/yyyy HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd",
)


# The course download uses upper-snake-case headers, whereas other Chicago
# exports use title-cased headers.  Transform both into one internal contract.
SOURCE_COLUMN_ALIASES = {
    "CASE_NUMBER": "Case Number",
    "DATE": "Date",
    "BLOCK": "Block",
    "PRIMARY_TYPE": "Primary Type",
    "DESCRIPTION": "Description",
    "LOCATION_DESCRIPTION": "Location Description",
    "COMMUNITY_AREA_NUMBER": "Community Area",
    "LATITUDE": "Latitude",
    "LONGITUDE": "Longitude",
    "ARREST": "Arrest",
    "DOMESTIC": "Domestic",
    "DISTRICT": "District",
}


def normalize_source_columns(frame: DataFrame) -> DataFrame:
    """Normalize supported Chicago Crime CSV header variants."""
    for source_column, canonical_column in SOURCE_COLUMN_ALIASES.items():
        if source_column in frame.columns:
            frame = frame.withColumnRenamed(source_column, canonical_column)
    return frame


def parse_incident_timestamp(frame: DataFrame) -> DataFrame:
    """Parse multiple observed timestamp formats into one incident timestamp."""
    parsed_candidates = [F.to_timestamp(F.col("Date"), fmt) for fmt in INCIDENT_TIMESTAMP_FORMATS]
    return frame.withColumn("incident_timestamp", F.coalesce(*parsed_candidates))


def cast_core_columns(frame: DataFrame) -> DataFrame:
    """Cast analysis fields explicitly; malformed values become null and are flagged later."""
    return (
        frame.withColumn("crime_id", F.col("ID").cast("long"))
        .withColumn("district", F.col("District").cast("int"))
        .withColumn("community_area", F.col("Community Area").cast("int"))
        .withColumn("latitude", F.col("Latitude").cast("double"))
        .withColumn("longitude", F.col("Longitude").cast("double"))
        .withColumn("arrest", F.lower(F.trim(F.col("Arrest"))).cast("boolean"))
        .withColumn("domestic", F.lower(F.trim(F.col("Domestic"))).cast("boolean"))
    )


def add_time_columns(frame: DataFrame) -> DataFrame:
    """Create standard temporal dimensions from the valid incident timestamp."""
    return (
        frame.withColumn("year", F.year("incident_timestamp"))
        .withColumn("month", F.month("incident_timestamp"))
        .withColumn("hour", F.hour("incident_timestamp"))
        .withColumn("day_of_week", F.dayofweek("incident_timestamp"))
    )


def add_quality_flags(frame: DataFrame) -> DataFrame:
    """Keep data-quality decisions visible instead of silently discarding records."""
    valid_coordinates = (
        F.col("latitude").between(41.0, 43.0)
        & F.col("longitude").between(-88.5, -87.0)
    )
    return (
        frame.withColumn("has_valid_crime_id", F.col("crime_id").isNotNull() & (F.col("crime_id") > 0))
        .withColumn("has_valid_timestamp", F.col("incident_timestamp").isNotNull())
        .withColumn("has_valid_coordinates", F.coalesce(valid_coordinates, F.lit(False)))
        .withColumn("has_valid_district", F.coalesce(F.col("district").between(1, 31), F.lit(False)))
        .withColumn(
            "has_valid_community_area",
            F.coalesce(F.col("community_area").between(1, 77), F.lit(False)),
        )
    )


def select_clean_columns(frame: DataFrame) -> DataFrame:
    """Return the stable, documented output contract for downstream tasks."""
    def optional_text(source_column: str, output_column: str):
        """Select a trimmed source column, or a null when it is absent.

        Small test fixtures and some Chicago Crime extracts omit descriptive
        columns.  Retain the output contract without making those columns a
        requirement for the core cleaning rules.
        """
        if source_column in frame.columns:
            return F.trim(F.col(source_column)).alias(output_column)
        return F.lit(None).cast("string").alias(output_column)

    return frame.select(
        "crime_id",
        optional_text("Case Number", "case_number"),
        "incident_timestamp",
        "year",
        "month",
        "hour",
        "day_of_week",
        optional_text("Block", "block"),
        optional_text("Primary Type", "primary_type"),
        optional_text("Description", "description"),
        optional_text("Location Description", "location_description"),
        "arrest",
        "domestic",
        "district",
        "community_area",
        "latitude",
        "longitude",
        "has_valid_crime_id",
        "has_valid_coordinates",
        "has_valid_district",
        "has_valid_community_area",
    )


def prepare_crime_records(raw_frame: DataFrame) -> DataFrame:
    """Parse, cast, derive, and flag records before any rows are rejected."""
    transformed = raw_frame.transform(parse_incident_timestamp).transform(cast_core_columns)
    return transformed.transform(add_time_columns).transform(add_quality_flags)


def clean_prepared_records(prepared_frame: DataFrame) -> DataFrame:
    """Apply the documented rejection and deduplication policy.

    Records with no parseable incident time cannot support temporal analysis and are
    rejected. Records with missing or invalid crime IDs are also rejected before
    deduplication, so distinct malformed rows are never silently collapsed. Coordinate
    and geographic defects are retained with flags for transparent analysis.
    """
    return (
        prepared_frame.filter(F.col("has_valid_timestamp") & F.col("has_valid_crime_id"))
        .dropDuplicates(["crime_id"])
        .transform(select_clean_columns)
    )


def clean_crime_records(raw_frame: DataFrame) -> DataFrame:
    """Convenience function for callers that do not need pre-filter metrics."""
    return clean_prepared_records(prepare_crime_records(raw_frame))
