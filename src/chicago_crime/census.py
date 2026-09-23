"""Cleaning rules for the Chicago socio-economic indicators dataset."""

from pyspark.sql import DataFrame, functions as F


CENSUS_COLUMN_ALIASES = {
    "Community Area Number": "community_area",
    "COMMUNITY_AREA_NUMBER": "community_area",
    "COMMUNITY AREA NAME": "community_name",
    "COMMUNITY_AREA_NAME": "community_name",
    "PERCENT OF HOUSING CROWDED": "percent_housing_crowded",
    "PERCENT_OF_HOUSING_CROWDED": "percent_housing_crowded",
    "PERCENT HOUSEHOLDS BELOW POVERTY": "percent_households_below_poverty",
    "PERCENT_HOUSEHOLDS_BELOW_POVERTY": "percent_households_below_poverty",
    "PERCENT AGED 16+ UNEMPLOYED": "percent_aged_16_unemployed",
    "PERCENT_AGED_16__UNEMPLOYED": "percent_aged_16_unemployed",
    "PERCENT AGED 25+ WITHOUT HIGH SCHOOL DIPLOMA": "percent_aged_25_without_high_school_diploma",
    "PERCENT_AGED_25__WITHOUT_HIGH_SCHOOL_DIPLOMA": "percent_aged_25_without_high_school_diploma",
    "PERCENT AGED UNDER 18 OR OVER 64": "percent_aged_under_18_or_over_64",
    "PERCENT_AGED_UNDER_18_OR_OVER_64": "percent_aged_under_18_or_over_64",
    "PER CAPITA INCOME ": "per_capita_income",
    "PER_CAPITA_INCOME": "per_capita_income",
    "HARDSHIP INDEX": "hardship_index",
    "HARDSHIP_INDEX": "hardship_index",
}

REQUIRED_CENSUS_COLUMNS = frozenset(
    {
        "community_area",
        "community_name",
        "percent_housing_crowded",
        "percent_households_below_poverty",
        "percent_aged_16_unemployed",
        "percent_aged_25_without_high_school_diploma",
        "percent_aged_under_18_or_over_64",
        "per_capita_income",
        "hardship_index",
    }
)


def normalize_census_columns(frame: DataFrame) -> DataFrame:
    """Map known census header variants to one stable output contract."""
    for source_column, canonical_column in CENSUS_COLUMN_ALIASES.items():
        if source_column in frame.columns:
            frame = frame.withColumnRenamed(source_column, canonical_column)
    return frame


def validate_census_columns(frame: DataFrame) -> None:
    """Fail early when essential join and analysis fields are absent."""
    missing_columns = sorted(REQUIRED_CENSUS_COLUMNS.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"Chicago census input is missing required columns: {', '.join(missing_columns)}")


def clean_census_records(raw_frame: DataFrame) -> DataFrame:
    """Create a typed 77-community-area lookup and remove the city aggregate row."""
    normalized = normalize_census_columns(raw_frame)
    validate_census_columns(normalized)

    numeric_columns = [
        "percent_housing_crowded",
        "percent_households_below_poverty",
        "percent_aged_16_unemployed",
        "percent_aged_25_without_high_school_diploma",
        "percent_aged_under_18_or_over_64",
        "per_capita_income",
        "hardship_index",
    ]
    typed = normalized.withColumn("community_area", F.col("community_area").cast("int"))
    for column_name in numeric_columns:
        typed = typed.withColumn(column_name, F.col(column_name).cast("double"))

    return (
        typed.filter(F.col("community_area").between(1, 77))
        .dropDuplicates(["community_area"])
        .select("community_area", F.trim(F.col("community_name")).alias("community_name"), *numeric_columns)
    )
