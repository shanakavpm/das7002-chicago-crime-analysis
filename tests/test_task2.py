"""Tests for ERA5 weather cleaning and the Task 2 daily crime-weather join."""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from chicago_crime.task2 import (
    create_daily_crime_weather,
    create_temporal_patterns,
    summarise_weather_impact,
)
from chicago_crime.weather import (
    aggregate_hourly_weather,
    clean_weather_records,
    weather_quality_summary,
)


WEATHER_SCHEMA = StructType(
    [
        StructField("weather_location", StringType(), True),
        StructField("weather_timestamp", StringType(), True),
        StructField("temperature_2m", StringType(), True),
        StructField("precipitation", StringType(), True),
        StructField("relative_humidity_2m", StringType(), True),
        StructField("wind_speed_10m", StringType(), True),
        StructField("weather_code", StringType(), True),
    ]
)


def test_era5_measurements_and_events_are_cleaned(spark):
    weather = spark.createDataFrame(
        [
            (
                "Chicago city centre ERA5 grid cell",
                "2012-01-01T12:10",
                "32",
                "0.00",
                "80",
                "12",
                "71",
            ),
            (
                "Chicago city centre ERA5 grid cell",
                "2012-01-01T12:50",
                "34",
                "0.10",
                "82",
                "14",
                "61",
            ),
        ],
        WEATHER_SCHEMA,
    )

    hourly = aggregate_hourly_weather(clean_weather_records(weather)).first()

    assert hourly.weather_date.isoformat() == "2012-01-01"
    assert hourly.weather_hour == 12
    assert hourly.hourly_precipitation_inches == 0.1
    assert hourly.has_precipitation == 1
    assert hourly.has_snow == 1
    assert hourly.has_rain == 1

    quality = weather_quality_summary(
        weather,
        aggregate_hourly_weather(clean_weather_records(weather)),
    ).first()
    assert quality.raw_weather_record_count == 2
    assert quality.invalid_weather_timestamp_count == 0
    assert quality.clean_hourly_weather_count == 1


def test_daily_weather_join_fills_hours_without_crime_as_zero(spark):
    weather = spark.createDataFrame(
        [
            (
                "Chicago city centre ERA5 grid cell",
                "2012-01-01T12:10",
                "32",
                "0.00",
                "80",
                "12",
                "0",
            ),
            (
                "Chicago city centre ERA5 grid cell",
                "2012-01-01T13:10",
                "33",
                "0.20",
                "82",
                "14",
                "61",
            ),
        ],
        WEATHER_SCHEMA,
    )
    crime = spark.createDataFrame(
        [("2012-01-01 12:30:00", 1, True), ("2012-01-01 12:45:00", 1, True)],
        ["incident_timestamp_text", "community_area", "has_valid_community_area"],
    ).withColumn(
        "incident_timestamp",
        F.col("incident_timestamp_text").cast("timestamp"),
    ).drop("incident_timestamp_text")

    daily = create_daily_crime_weather(
        crime,
        aggregate_hourly_weather(clean_weather_records(weather)),
    )
    row = daily.first()

    assert row.crime_count == 2
    assert row.has_rain == 1
    assert row.has_precipitation == 1
    summary = summarise_weather_impact(daily).first()
    assert summary.weather_condition == "rain"
    assert summary.day_count == 1


def test_temporal_patterns_are_created_with_spark_sql(spark):
    crime = spark.createDataFrame(
        [
            ("2012-01-01 12:30:00", 2012, 1, 1, 12),
            ("2012-01-01 12:45:00", 2012, 1, 1, 12),
        ],
        ["incident_timestamp_text", "year", "month", "day_of_week", "hour"],
    ).withColumn("incident_timestamp", F.col("incident_timestamp_text").cast("timestamp"))

    row = create_temporal_patterns(crime).first()

    assert row.crime_count == 2
