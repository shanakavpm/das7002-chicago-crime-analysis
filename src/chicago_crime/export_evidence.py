"""Export small, report-ready DAS7002 tables as ordinary CSV files."""

import argparse
import csv
import logging
from pathlib import Path

from pyspark.sql import DataFrame

from .logging_utils import configure_logging
from .pipeline import create_spark_session


LOGGER = logging.getLogger(__name__)


def export_small_frame(frame: DataFrame, output_path: Path) -> None:
    """Write a small Spark result table as one human-readable CSV file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=frame.columns,
            lineterminator="\n",
        )
        writer.writeheader()
        for row in frame.toLocalIterator():
            writer.writerow(row.asDict(recursive=True))


def parse_arguments() -> argparse.Namespace:
    """Parse source directories and the task evidence sets to export."""
    parser = argparse.ArgumentParser(description="Export report-ready DAS7002 evidence tables.")
    parser.add_argument("--processed-dir", type=Path, default=Path("data/processed"))
    parser.add_argument("--task-output-dir", type=Path, default=Path("outputs/task4"))
    parser.add_argument("--task2-output-dir", type=Path, default=Path("outputs/task2"))
    parser.add_argument("--task3-output-dir", type=Path, default=Path("outputs/task3"))
    parser.add_argument("--evidence-dir", type=Path, default=Path("reports/evidence"))
    parser.add_argument(
        "--tasks",
        choices=("task1", "task2", "task3", "task4"),
        nargs="+",
        default=("task1", "task2", "task3", "task4"),
        help="Tasks whose compact evidence tables should be exported.",
    )
    return parser.parse_args()


def run() -> None:
    """Create visible CSV copies of the small tables needed in the report."""
    configure_logging()
    args = parse_arguments()
    spark = create_spark_session("DAS7002-Export-Evidence")
    try:
        if "task1" in args.tasks:
            task1_tables = {
                "cleaned_census": spark.read.parquet(
                    str(args.processed_dir / "chicago_census_parquet")
                ).orderBy("community_area"),
                "crime_quality": spark.read.parquet(
                    str(args.processed_dir / "task1_crime_quality")
                ),
                "weather_quality": spark.read.parquet(
                    str(args.processed_dir / "task1_weather_quality")
                ),
                "data_dictionary": spark.read.parquet(
                    str(args.processed_dir / "task1_data_dictionary")
                ).orderBy("source_field"),
            }
            for table_name, frame in task1_tables.items():
                export_small_frame(
                    frame,
                    args.evidence_dir / f"task1_{table_name}.csv",
                )
        if "task2" in args.tasks:
            for table_name in (
                "weather_impact",
                "weather_correlations",
                "temporal_patterns",
                "daily_crime_weather",
                "community_socioeconomic",
                "seasonal_weather_impact",
                "weather_effect_sizes",
                "weekday_weekend_patterns",
                "community_spatial_patterns",
            ):
                export_small_frame(
                    spark.read.parquet(str(args.task2_output_dir / table_name)),
                    args.evidence_dir / f"task2_{table_name}.csv",
                )
        if "task3" in args.tasks:
            for table_name in ("silhouette_scores", "cluster_summary", "district_alignment"):
                export_small_frame(
                    spark.read.parquet(str(args.task3_output_dir / table_name)),
                    args.evidence_dir / f"task3_{table_name}.csv",
                )
        if "task4" in args.tasks:
            for table_name in (
                "class_distribution",
                "crime_type_distribution",
                "confusion_matrix",
                "metrics",
                "model_comparison",
                "threshold_metrics",
                "feature_importance",
            ):
                export_small_frame(
                    spark.read.parquet(str(args.task_output_dir / table_name)),
                    args.evidence_dir / f"task4_{table_name}.csv",
                )
        LOGGER.info("human_readable_evidence_saved path=%s", args.evidence_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
