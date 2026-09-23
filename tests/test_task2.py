"""Tests for NOAA weather cleaning and the Task 2 daily crime-weather join."""

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType, TimestampType

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
        StructField("STATION", StringType(), True),
        StructField("DATE", StringType(), True),
        StructField("HourlyDryBulbTemperature", StringType(), True),
        StructField("HourlyPrecipitation", StringType(), True),
        StructField("HourlyPresentWeatherType", StringType(), True),
        StructField("HourlyRelativeHumidity", StringType(), True),
        StructField("HourlyWindSpeed", StringType(), True),
    ]
)

CRIME_SCHEMA = StructType(
    [
        StructField("incident_timestamp", TimestampType(), True),
        StructField("community_area", IntegerType(), True),
        StructField("has_valid_community_area", BooleanType(), True),
    ]
)


def test_noaa_trace_precipitation_and_events_are_cleaned(spark):
    weather = spark.createDataFrame(
        [
            ("72534014819", "2012-01-01T12:10:00", "32", "T", "-SN:03 BR:1", "80", "12"),
            ("72534014819", "2012-01-01T12:50:00", "34", "0.10", "RA:02", "82", "14"),
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

    quality = weather_quality_summary(weather, aggregate_hourly_weather(clean_weather_records(weather))).first()
    assert quality.raw_weather_record_count == 2
    assert quality.invalid_weather_timestamp_count == 0
    assert quality.clean_hourly_weather_count == 1


def test_daily_weather_join_fills_hours_without_crime_as_zero(spark):
    weather = spark.createDataFrame(
        [
            ("72534014819", "2012-01-01T12:10:00", "32", "0.00", "", "80", "12"),
            ("72534014819", "2012-01-01T13:10:00", "33", "0.20", "RA:02", "82", "14"),
        ],
        WEATHER_SCHEMA,
    )
    crime = spark.createDataFrame(
        [("2012-01-01 12:30:00", 1, True), ("2012-01-01 12:45:00", 1, True)],
        ["incident_timestamp_text", "community_area", "has_valid_community_area"],
    ).withColumn("incident_timestamp", F.col("incident_timestamp_text").cast("timestamp")).drop("incident_timestamp_text")

    daily = create_daily_crime_weather(crime, aggregate_hourly_weather(clean_weather_records(weather)))
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
