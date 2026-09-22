import pytest

from chicago_crime.schema import validate_required_source_columns


def test_schema_validation_reports_missing_required_columns(spark):
    frame = spark.createDataFrame([("1",)], ["ID"])

    with pytest.raises(ValueError, match="missing required columns"):
        validate_required_source_columns(frame)
