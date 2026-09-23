"""End-to-end integration coverage for the partitioned crime ETL output."""

from chicago_crime.transforms import clean_crime_records
from test_transforms import create_raw_frame


def test_cleaned_crime_round_trip_uses_year_and_district_partitions(spark, tmp_path):
    raw = create_raw_frame(
        spark,
        [
            (
                "1", "01/02/2012 01:30:00 PM", "12", "7", "41.88", "-87.63",
                "true", "false", "THEFT",
            ),
            (
                "2", "02/03/2013 02:15:00 AM", "7", "68", "41.77", "-87.65",
                "false", "true", "BATTERY",
            ),
        ],
    )
    output_path = tmp_path / "partitioned_crime"

    clean_crime_records(raw).write.partitionBy("year", "district").parquet(str(output_path))
    loaded = spark.read.parquet(str(output_path))

    assert loaded.count() == 2
    assert {row.year for row in loaded.select("year").distinct().collect()} == {2012, 2013}
    assert (output_path / "year=2012" / "district=12").is_dir()
    assert (output_path / "year=2013" / "district=7").is_dir()
