"""Schema contract and validation rules for Chicago Crime source files."""

from pyspark.sql import DataFrame


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


def validate_required_source_columns(frame: DataFrame) -> None:
    """Fail early when a CSV cannot satisfy the Task 1 data contract."""
    missing_columns = sorted(REQUIRED_SOURCE_COLUMNS.difference(frame.columns))
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"Chicago Crime input is missing required columns: {missing}")
