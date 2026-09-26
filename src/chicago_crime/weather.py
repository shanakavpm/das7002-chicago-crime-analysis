"""Cleaning and aggregation rules for Open-Meteo ERA5 weather data."""

from pyspark.sql import DataFrame, functions as F


REQUIRED_WEATHER_COLUMNS = frozenset(
    {
        "weather_location",
        "weather_timestamp",
        "temperature_2m",
        "precipitation",
        "relative_humidity_2m",
        "wind_speed_10m",
        "weather_code",
    }
)

WEATHER_TIMESTAMP_FORMATS = (
    "yyyy-MM-dd'T'HH:mm",
    "yyyy-MM-dd'T'HH:mm:ss",
)


def validate_weather_columns(frame: DataFrame) -> None:
    """Fail early when the ERA5 extract cannot support the weather analysis."""
    missing_columns = sorted(REQUIRED_WEATHER_COLUMNS.difference(frame.columns))
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"Chicago ERA5 input is missing required columns: {missing}")


def parse_numeric_measurement(column_name: str):
    """Convert an Open-Meteo numeric field while preserving missing values."""
    cleaned = F.trim(F.col(column_name))
    return F.when(
        cleaned.isNull() | (cleaned == ""),
        F.lit(None).cast("double"),
    ).otherwise(cleaned.cast("double"))


def classify_weather_event(weather_code_column: str):
    """Map WMO weather interpretation codes into report-friendly categories."""
    weather_code = F.col(weather_code_column).cast("int")
    return (
        F.when(weather_code.isin(95, 96, 99), F.lit("thunderstorm"))
        .when(weather_code.isin(71, 73, 75, 77, 85, 86), F.lit("snow"))
        .when(
            weather_code.isin(51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82),
            F.lit("rain"),
        )
        .when(weather_code.isin(45, 48), F.lit("fog_or_mist"))
        .when(
            weather_code.isNull() | weather_code.isin(0, 1, 2, 3),
            F.lit("no_reported_weather"),
        )
        .when(weather_code > 3, F.lit("other_reported_weather"))
        .otherwise(F.lit("no_reported_weather"))
    )


def clean_weather_records(raw_frame: DataFrame) -> DataFrame:
    """Create a typed ERA5 weather contract at observation level."""
    validate_weather_columns(raw_frame)
    parsed_timestamp = F.coalesce(
        *(
            F.to_timestamp(F.col("weather_timestamp"), timestamp_format)
            for timestamp_format in WEATHER_TIMESTAMP_FORMATS
        )
    )
    return (
        raw_frame.withColumn("weather_timestamp", parsed_timestamp)
        .withColumn("temperature_f", parse_numeric_measurement("temperature_2m"))
        .withColumn("precipitation_inches", parse_numeric_measurement("precipitation"))
        .withColumn("relative_humidity", parse_numeric_measurement("relative_humidity_2m"))
        .withColumn("wind_speed_mph", parse_numeric_measurement("wind_speed_10m"))
        .withColumn("weather_event", classify_weather_event("weather_code"))
        .filter(F.col("weather_timestamp").isNotNull())
        .select(
            F.trim(F.col("weather_location")).alias("weather_station"),
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


def aggregate_hourly_weather(weather_frame: DataFrame) -> DataFrame:
    """Collapse repeated local clock hours into one joinable observation."""
    return (
        weather_frame.groupBy("weather_date", "weather_hour")
        .agg(
            F.first("weather_station", ignorenulls=True).alias("weather_station"),
            F.avg("temperature_f").alias("average_temperature_f"),
            F.max("precipitation_inches").alias("hourly_precipitation_inches"),
            F.avg("relative_humidity").alias("average_relative_humidity"),
            F.avg("wind_speed_mph").alias("average_wind_speed_mph"),
            F.max(
                F.when(
                    F.col("weather_event") != "no_reported_weather",
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("has_reported_weather_event"),
            F.max(
                F.when(F.col("weather_event") == "rain", F.lit(1)).otherwise(F.lit(0))
            ).alias("has_rain"),
            F.max(
                F.when(F.col("weather_event") == "snow", F.lit(1)).otherwise(F.lit(0))
            ).alias("has_snow"),
            F.max(
                F.when(F.col("weather_event") == "thunderstorm", F.lit(1)).otherwise(
                    F.lit(0)
                )
            ).alias("has_thunderstorm"),
        )
        .withColumn(
            "has_precipitation",
            F.coalesce(
                F.col("hourly_precipitation_inches") > 0,
                F.lit(False),
            ).cast("int"),
        )
    )


def weather_quality_summary(raw_frame: DataFrame, hourly_frame: DataFrame) -> DataFrame:
    """Return compact Task 1 evidence for weather coverage and rejected timestamps."""
    validate_weather_columns(raw_frame)
    parsed_timestamp = F.coalesce(
        *(
            F.to_timestamp(F.col("weather_timestamp"), timestamp_format)
            for timestamp_format in WEATHER_TIMESTAMP_FORMATS
        )
    )
    raw_summary = raw_frame.agg(
        F.count("*").alias("raw_weather_record_count"),
        F.sum(parsed_timestamp.isNull().cast("long")).alias(
            "invalid_weather_timestamp_count"
        ),
    )
    hourly_summary = hourly_frame.agg(
        F.count("*").alias("clean_hourly_weather_count"),
        F.countDistinct("weather_station").alias("weather_station_count"),
        F.min("weather_date").alias("weather_start_date"),
        F.max("weather_date").alias("weather_end_date"),
    )
    return raw_summary.crossJoin(hourly_summary)
