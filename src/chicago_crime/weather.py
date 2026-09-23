"""Cleaning and aggregation rules for NOAA Local Climatological Data."""

from pyspark.sql import DataFrame, functions as F


REQUIRED_WEATHER_COLUMNS = frozenset(
    {
        "STATION",
        "DATE",
        "HourlyDryBulbTemperature",
        "HourlyPrecipitation",
        "HourlyPresentWeatherType",
        "HourlyRelativeHumidity",
        "HourlyWindSpeed",
    }
)

WEATHER_TIMESTAMP_FORMATS = (
    "yyyy-MM-dd'T'HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss",
)


def validate_weather_columns(frame: DataFrame) -> None:
    """Fail early when the NOAA extract cannot support the weather analysis."""
    missing_columns = sorted(REQUIRED_WEATHER_COLUMNS.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"Chicago weather input is missing required columns: {', '.join(missing_columns)}")


def parse_noaa_measurement(column_name: str):
    """Convert NOAA numeric strings, preserving trace precipitation as 0.001 inch."""
    cleaned = F.upper(F.trim(F.col(column_name)))
    return (
        F.when(cleaned.isNull() | (cleaned == "") | cleaned.isin("M", "S"), F.lit(None).cast("double"))
        .when(cleaned == "T", F.lit(0.001))
        .otherwise(cleaned.cast("double"))
    )


def classify_weather_event(weather_text_column: str):
    """Map NOAA present-weather codes into report-friendly event categories."""
    weather_text = F.upper(F.coalesce(F.col(weather_text_column), F.lit("")))
    return (
        F.when(weather_text.contains("TS"), F.lit("thunderstorm"))
        .when(weather_text.contains("SN"), F.lit("snow"))
        .when(weather_text.rlike("RA|DZ"), F.lit("rain"))
        .when(weather_text.rlike("FG|BR"), F.lit("fog_or_mist"))
        .when(weather_text != "", F.lit("other_reported_weather"))
        .otherwise(F.lit("no_reported_weather"))
    )


def clean_weather_records(raw_frame: DataFrame) -> DataFrame:
    """Create a typed, documented NOAA weather contract at observation level."""
    validate_weather_columns(raw_frame)
    parsed_timestamp = F.coalesce(
        *(F.to_timestamp(F.col("DATE"), timestamp_format) for timestamp_format in WEATHER_TIMESTAMP_FORMATS)
    )
    weather_frame = (
        raw_frame.withColumn("weather_timestamp", parsed_timestamp)
        .withColumn("temperature_f", parse_noaa_measurement("HourlyDryBulbTemperature"))
        .withColumn("precipitation_inches", parse_noaa_measurement("HourlyPrecipitation"))
        .withColumn("relative_humidity", parse_noaa_measurement("HourlyRelativeHumidity"))
        .withColumn("wind_speed_mph", parse_noaa_measurement("HourlyWindSpeed"))
        .withColumn("weather_event", classify_weather_event("HourlyPresentWeatherType"))
        .filter(F.col("weather_timestamp").isNotNull())
        .select(
            F.trim(F.col("STATION")).alias("weather_station"),
            "weather_timestamp",
            F.to_date("weather_timestamp").alias("weather_date"),
            F.hour("weather_timestamp").alias("weather_hour"),
            "temperature_f",
            "precipitation_inches",
            "relative_humidity",
            "wind_speed_mph",
            "weather_event",
        )
    )
    return weather_frame


def aggregate_hourly_weather(weather_frame: DataFrame) -> DataFrame:
    """Collapse multiple station reports in an hour into one joinable observation."""
    return (
        weather_frame.groupBy("weather_date", "weather_hour")
        .agg(
            F.first("weather_station", ignorenulls=True).alias("weather_station"),
            F.avg("temperature_f").alias("average_temperature_f"),
            F.max("precipitation_inches").alias("hourly_precipitation_inches"),
            F.avg("relative_humidity").alias("average_relative_humidity"),
            F.avg("wind_speed_mph").alias("average_wind_speed_mph"),
            F.max(F.when(F.col("weather_event") != "no_reported_weather", F.lit(1)).otherwise(F.lit(0))).alias(
                "has_reported_weather_event"
            ),
            F.max(F.when(F.col("weather_event") == "rain", F.lit(1)).otherwise(F.lit(0))).alias("has_rain"),
            F.max(F.when(F.col("weather_event") == "snow", F.lit(1)).otherwise(F.lit(0))).alias("has_snow"),
            F.max(F.when(F.col("weather_event") == "thunderstorm", F.lit(1)).otherwise(F.lit(0))).alias(
                "has_thunderstorm"
            ),
        )
        .withColumn(
            "has_precipitation",
            F.coalesce(F.col("hourly_precipitation_inches") > 0, F.lit(False)).cast("int"),
        )
    )


def weather_quality_summary(raw_frame: DataFrame, hourly_frame: DataFrame) -> DataFrame:
    """Return compact Task 1 evidence for weather coverage and rejected timestamps."""
    validate_weather_columns(raw_frame)
    parsed_timestamp = F.coalesce(
        *(F.to_timestamp(F.col("DATE"), timestamp_format) for timestamp_format in WEATHER_TIMESTAMP_FORMATS)
    )
    raw_summary = raw_frame.agg(
        F.count("*").alias("raw_weather_record_count"),
        F.sum(parsed_timestamp.isNull().cast("long")).alias("invalid_weather_timestamp_count"),
    )
    hourly_summary = hourly_frame.agg(
        F.count("*").alias("clean_hourly_weather_count"),
        F.countDistinct("weather_station").alias("weather_station_count"),
        F.min("weather_date").alias("weather_start_date"),
        F.max("weather_date").alias("weather_end_date"),
    )
    return raw_summary.crossJoin(hourly_summary)
