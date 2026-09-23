"""Schema contract and validation rules for Chicago Crime source files."""

from pyspark.sql import DataFrame, SparkSession


# Raw CSV values are intentionally read as strings. This lets the pipeline
# detect malformed values during explicit casts rather than losing them during
# CSV parsing. The canonical header contract is validated immediately after
# normalisation of supported source-header variants.
REQUIRED_SOURCE_COLUMNS = frozenset(
    {
        "ID",
        "Date",
        "District",
        "Community Area",
        "Latitude",
        "Longitude",
        "Arrest",
        "Domestic",
        "Primary Type",
    }
)

CRIME_DATA_DICTIONARY = (
    ("ID", "crime_id", "long", "Cast to long", "Reject null or non-positive values"),
    ("Date", "incident_timestamp", "timestamp", "Parse supported timestamp formats", "Reject unparseable values"),
    ("District", "district", "integer", "Cast to integer", "Retain and flag values outside 1-31"),
    ("Community Area", "community_area", "integer", "Cast to integer", "Retain and flag values outside 1-77"),
    ("Latitude", "latitude", "double", "Cast to double", "Retain and flag values outside Chicago bounds"),
    ("Longitude", "longitude", "double", "Cast to double", "Retain and flag values outside Chicago bounds"),
    ("Arrest", "arrest", "boolean", "Trim, lowercase, and cast", "Retain null for transparent downstream filtering"),
    ("Domestic", "domestic", "boolean", "Trim, lowercase, and cast", "Retain null for transparent downstream filtering"),
    ("Primary Type", "primary_type", "string", "Trim text", "Retain null for non-modelling analysis"),
)


def validate_required_source_columns(frame: DataFrame) -> None:
    """Fail early when a CSV cannot satisfy the Task 1 data contract."""
    missing_columns = sorted(REQUIRED_SOURCE_COLUMNS.difference(frame.columns))
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"Chicago Crime input is missing required columns: {missing}")


def create_crime_data_dictionary(spark: SparkSession) -> DataFrame:
    """Create the auditable source-to-target schema and invalid-value policy table."""
    return spark.createDataFrame(
        CRIME_DATA_DICTIONARY,
        ["source_field", "target_field", "spark_type", "cleaning_rule", "invalid_value_policy"],
    )
