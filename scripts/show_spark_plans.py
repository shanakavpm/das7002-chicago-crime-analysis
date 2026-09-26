"""Print physical-plan evidence for partition pruning and a broadcast join."""

from pyspark.sql import functions as F

from chicago_crime.pipeline import create_spark_session


def run() -> None:
    """Explain the two Spark optimisations cited in the assignment report."""
    spark = create_spark_session("DAS7002-Execution-Plans")
    try:
        crime = spark.read.parquet("data/processed/chicago_crime_parquet")
        census = spark.read.parquet("data/processed/chicago_census_parquet")

        print("\nPARTITION PRUNING")
        (
            crime.filter((F.col("year") == 2012) & (F.col("district") == 11))
            .select("crime_id", "year", "district")
            .explain(mode="formatted")
        )

        print("\nBROADCAST JOIN")
        (
            crime.filter(
                F.col("has_valid_community_area")
                & F.col("year").between(2008, 2012)
            )
            .join(F.broadcast(census), "community_area", "inner")
            .groupBy("community_area", "community_name")
            .agg(F.count("*").alias("crime_count"))
            .explain(mode="formatted")
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
