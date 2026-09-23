"""Task 2 PySpark SQL temporal, weather, spatial, and census analysis."""

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window, functions as F


@dataclass(frozen=True)
class Task2Result:
    """Reproducible tables used to answer the Task 2 requirements."""

    daily_crime_weather: DataFrame
    weather_impact: DataFrame
    seasonal_weather_impact: DataFrame
    weather_effect_sizes: DataFrame
    temporal_patterns: DataFrame
    weekday_weekend_patterns: DataFrame
    weather_correlations: DataFrame
    community_area_rolling: DataFrame
    community_socioeconomic: DataFrame
    community_spatial_patterns: DataFrame


def weather_condition_expression():
    """Return the shared precedence rule used to label daily weather conditions."""
    return (
        F.when(F.col("has_thunderstorm") == 1, F.lit("thunderstorm"))
        .when(F.col("has_snow") == 1, F.lit("snow"))
        .when(F.col("has_rain") == 1, F.lit("rain"))
        .when(F.col("has_precipitation") == 1, F.lit("other precipitation"))
        .when(F.col("has_reported_weather_event") == 1, F.lit("other event"))
        .otherwise(F.lit("no event"))
    )


def prepare_crime_hourly_counts(crime_frame: DataFrame) -> DataFrame:
    """Count valid cleaned crime records at the same hourly grain as NOAA data."""
    return (
        crime_frame.filter(F.col("incident_timestamp").isNotNull())
        .withColumn("crime_date", F.to_date("incident_timestamp"))
        .withColumn("crime_hour", F.hour("incident_timestamp"))
        .groupBy("crime_date", "crime_hour")
        .agg(F.count("*").alias("crime_count"))
    )


def create_daily_crime_weather(
    crime_frame: DataFrame,
    weather_hourly: DataFrame,
) -> DataFrame:
    """Join city-wide Midway observations to hourly crime frequency, then summarise days."""
    crime_hourly = prepare_crime_hourly_counts(crime_frame)
    hourly_join = weather_hourly.join(
        crime_hourly,
        (weather_hourly.weather_date == crime_hourly.crime_date)
        & (weather_hourly.weather_hour == crime_hourly.crime_hour),
        "left",
    ).drop("crime_date", "crime_hour")

    return (
        hourly_join.groupBy("weather_date")
        .agg(
            F.sum(F.coalesce(F.col("crime_count"), F.lit(0))).cast("long").alias("crime_count"),
            F.avg("average_temperature_f").alias("average_temperature_f"),
            F.sum(F.coalesce(F.col("hourly_precipitation_inches"), F.lit(0.0))).alias(
                "reported_precipitation_inches"
            ),
            F.avg("average_relative_humidity").alias("average_relative_humidity"),
            F.avg("average_wind_speed_mph").alias("average_wind_speed_mph"),
            F.max("has_reported_weather_event").cast("int").alias("has_reported_weather_event"),
            F.max("has_precipitation").cast("int").alias("has_precipitation"),
            F.max("has_rain").cast("int").alias("has_rain"),
            F.max("has_snow").cast("int").alias("has_snow"),
            F.max("has_thunderstorm").cast("int").alias("has_thunderstorm"),
        )
        .orderBy("weather_date")
    )


def summarise_weather_impact(daily_crime_weather: DataFrame) -> DataFrame:
    """Compare daily crime frequency across observed weather conditions."""
    return (
        daily_crime_weather.withColumn("weather_condition", weather_condition_expression())
        .groupBy("weather_condition")
        .agg(
            F.count("*").alias("day_count"),
            F.avg("crime_count").alias("average_daily_crime_count"),
            F.expr("percentile_approx(crime_count, 0.5)").alias("median_daily_crime_count"),
        )
        .orderBy("weather_condition")
    )


def create_seasonal_weather_impact(daily_crime_weather: DataFrame) -> DataFrame:
    """Compare weather groups within seasons to reduce seasonal confounding."""
    month = F.month("weather_date")
    season = (
        F.when(month.isin(12, 1, 2), F.lit("winter"))
        .when(month.isin(3, 4, 5), F.lit("spring"))
        .when(month.isin(6, 7, 8), F.lit("summer"))
        .otherwise(F.lit("autumn"))
    )
    return (
        daily_crime_weather.withColumn("season", season)
        .withColumn("weather_condition", weather_condition_expression())
        .groupBy("season", "weather_condition")
        .agg(
            F.count("*").alias("day_count"),
            F.avg("crime_count").alias("average_daily_crime_count"),
            F.stddev_samp("crime_count").alias("crime_count_standard_deviation"),
        )
        .orderBy("season", "weather_condition")
    )


def create_weather_effect_sizes(weather_impact: DataFrame) -> DataFrame:
    """Compare each weather condition with the no-event daily-crime baseline."""
    baseline = weather_impact.filter(F.col("weather_condition") == "no event").select(
        F.col("average_daily_crime_count").alias("no_event_average_daily_crime_count")
    )
    return (
        weather_impact.crossJoin(baseline)
        .withColumn(
            "absolute_difference_from_no_event",
            F.col("average_daily_crime_count") - F.col("no_event_average_daily_crime_count"),
        )
        .withColumn(
            "percent_difference_from_no_event",
            F.when(
                F.col("no_event_average_daily_crime_count") != 0,
                F.col("absolute_difference_from_no_event")
                / F.col("no_event_average_daily_crime_count")
                * 100,
            ),
        )
        .orderBy("weather_condition")
    )


def create_temporal_patterns(crime_frame: DataFrame) -> DataFrame:
    """Use an explicit PySpark SQL query to count crimes by core time dimensions."""
    view_name = "task2_crime_records"
    crime_frame.createOrReplaceTempView(view_name)
    return crime_frame.sparkSession.sql(
        f"""
        SELECT year, month, day_of_week, hour, COUNT(*) AS crime_count
        FROM {view_name}
        WHERE incident_timestamp IS NOT NULL
        GROUP BY year, month, day_of_week, hour
        ORDER BY year, month, day_of_week, hour
        """
    )


def create_weekday_weekend_patterns(crime_frame: DataFrame) -> DataFrame:
    """Compare hourly crime distributions between weekdays and weekends."""
    return (
        crime_frame.filter(F.col("incident_timestamp").isNotNull())
        .withColumn(
            "day_type",
            F.when(F.col("day_of_week").isin(1, 7), F.lit("weekend")).otherwise(
                F.lit("weekday")
            ),
        )
        .groupBy("day_type", "hour")
        .agg(F.count("*").alias("crime_count"))
        .orderBy("day_type", "hour")
    )


def create_weather_correlations(daily_crime_weather: DataFrame) -> DataFrame:
    """Calculate linear weather correlations for cautious, non-causal interpretation."""
    return daily_crime_weather.agg(
        F.corr("crime_count", "average_temperature_f").alias("temperature_crime_correlation"),
        F.corr("crime_count", "reported_precipitation_inches").alias(
            "precipitation_crime_correlation"
        ),
        F.corr("crime_count", "average_relative_humidity").alias("humidity_crime_correlation"),
        F.corr("crime_count", "average_wind_speed_mph").alias("wind_crime_correlation"),
    )


def create_community_rolling_average(
    crime_frame: DataFrame,
    census_frame: DataFrame,
    daily_crime_weather: DataFrame,
) -> DataFrame:
    """Use a broadcast community lookup and a complete calendar for 7-day rolling crime trends."""
    calendar = census_frame.select("community_area").crossJoin(
        daily_crime_weather.select("weather_date")
    )
    daily_area_crime = (
        crime_frame.filter(F.col("has_valid_community_area"))
        .withColumn("weather_date", F.to_date("incident_timestamp"))
        .groupBy("community_area", "weather_date")
        .agg(F.count("*").alias("crime_count"))
    )
    area_calendar = calendar.join(daily_area_crime, ["community_area", "weather_date"], "left").fillna(
        {"crime_count": 0}
    )
    rolling_window = (
        Window.partitionBy("community_area")
        .orderBy("weather_date")
        .rowsBetween(-6, 0)
    )

    return (
        area_calendar.join(F.broadcast(census_frame), "community_area", "inner")
        .withColumn("rolling_7_day_crime_average", F.avg("crime_count").over(rolling_window))
        .select(
            "weather_date",
            "community_area",
            "community_name",
            "hardship_index",
            "crime_count",
            "rolling_7_day_crime_average",
        )
        .orderBy("community_area", "weather_date")
    )


def create_community_socioeconomic_summary(
    crime_frame: DataFrame, census_frame: DataFrame, analysis_start, analysis_end
) -> DataFrame:
    """Broadcast the census lookup while aggregating crimes within the weather/census period."""
    return (
        crime_frame.filter(F.col("has_valid_community_area") & F.col("year").between(2008, 2012))
        .filter(F.to_date("incident_timestamp").between(F.lit(analysis_start), F.lit(analysis_end)))
        .join(F.broadcast(census_frame), "community_area", "inner")
        .groupBy("community_area", "community_name", "hardship_index")
        .agg(
            F.count("*").alias("crime_count"),
            F.avg(F.col("arrest").cast("double")).alias("arrest_rate"),
        )
        .orderBy("community_area")
    )


def create_community_spatial_patterns(
    crime_frame: DataFrame,
    census_frame: DataFrame,
    analysis_start,
    analysis_end,
) -> DataFrame:
    """Profile community-level crime concentration and geographic centroids."""
    filtered_crime = (
        crime_frame.filter(F.col("has_valid_community_area") & F.col("has_valid_coordinates"))
        .filter(F.to_date("incident_timestamp").between(F.lit(analysis_start), F.lit(analysis_end)))
    )
    total_count = filtered_crime.agg(F.count("*").alias("total_count"))
    return (
        filtered_crime.groupBy("community_area")
        .agg(
            F.count("*").alias("crime_count"),
            F.avg("latitude").alias("centroid_latitude"),
            F.avg("longitude").alias("centroid_longitude"),
        )
        .crossJoin(total_count)
        .withColumn("crime_share", F.col("crime_count") / F.col("total_count"))
        .drop("total_count")
        .join(F.broadcast(census_frame), "community_area", "inner")
        .select(
            "community_area",
            "community_name",
            "crime_count",
            "crime_share",
            "centroid_latitude",
            "centroid_longitude",
            "hardship_index",
        )
        .orderBy(F.desc("crime_count"))
    )


def run_task2(
    crime_frame: DataFrame,
    weather_hourly: DataFrame,
    census_frame: DataFrame,
) -> Task2Result:
    """Analyze Task 1 outputs using NOAA Midway as the city-wide weather proxy."""
    daily_crime_weather = create_daily_crime_weather(crime_frame, weather_hourly).cache()
    temporal_patterns = create_temporal_patterns(crime_frame).cache()
    analysis_period = daily_crime_weather.agg(
        F.min("weather_date").alias("analysis_start"), F.max("weather_date").alias("analysis_end")
    ).first()
    weather_impact = summarise_weather_impact(daily_crime_weather)
    return Task2Result(
        daily_crime_weather=daily_crime_weather,
        weather_impact=weather_impact,
        seasonal_weather_impact=create_seasonal_weather_impact(daily_crime_weather),
        weather_effect_sizes=create_weather_effect_sizes(weather_impact),
        temporal_patterns=temporal_patterns,
        weekday_weekend_patterns=create_weekday_weekend_patterns(crime_frame),
        weather_correlations=create_weather_correlations(daily_crime_weather),
        community_area_rolling=create_community_rolling_average(
            crime_frame,
            census_frame,
            daily_crime_weather,
        ),
        community_socioeconomic=create_community_socioeconomic_summary(
            crime_frame, census_frame, analysis_period.analysis_start, analysis_period.analysis_end
        ),
        community_spatial_patterns=create_community_spatial_patterns(
            crime_frame,
            census_frame,
            analysis_period.analysis_start,
            analysis_period.analysis_end,
        ),
    )
