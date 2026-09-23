"""Focused tests for Task 3 input preparation."""

from chicago_crime.clustering import prepare_clustering_input


def test_clustering_input_removes_invalid_coordinates(spark):
    crime = spark.createDataFrame(
        [
            (1, 41.8, -87.7, 12, 1, True),
            (2, None, None, 13, 2, False),
        ],
        ["crime_id", "latitude", "longitude", "hour", "district", "has_valid_coordinates"],
    )

    rows = prepare_clustering_input(crime).collect()

    assert len(rows) == 1
    assert rows[0].crime_id == 1
