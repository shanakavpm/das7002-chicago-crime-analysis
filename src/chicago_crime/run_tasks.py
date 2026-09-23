"""Command entry point for DAS7002 Tasks 2, 3, and 4."""

import argparse
import logging
from pathlib import Path

from .clustering import ClusteringResult, fit_spatial_temporal_clusters
from .logging_utils import configure_logging
from .modeling import ModelingResult, fit_arrest_model
from .pipeline import create_spark_session
from .task2 import Task2Result, run_task2
from .visualizations import (
    save_cluster_map,
    save_crime_type_distribution_chart,
    save_elbow_chart,
    save_feature_importance_chart,
    save_hourly_crime_chart,
    save_model_comparison_chart,
    save_roc_curve,
    save_silhouette_chart,
    save_threshold_metrics_chart,
    save_weather_impact_chart,
)


CLUSTER_OUTPUT_SAMPLE_SIZE = 50_000
LOGGER = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """Parse the selected assignment task and its required input/output paths."""
    parser = argparse.ArgumentParser(description="Run DAS7002 Task 2, 3, or 4 analysis.")
    parser.add_argument("task", choices=("eda", "cluster", "model"), help="Assignment task to run.")
    parser.add_argument("--crime-input", required=True, type=Path, help="Path to cleaned crime Parquet data.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for task outputs.")
    parser.add_argument(
        "--weather-input",
        type=Path,
        help="Path to cleaned hourly weather Parquet from Task 1; required for Task 2.",
    )
    parser.add_argument("--census-input", type=Path, help="Path to cleaned census Parquet; required for Task 2.")
    return parser.parse_args()


def write_task3_outputs(result: ClusteringResult, output_dir: Path) -> None:
    """Persist reproducible Task 3 evidence for charts and report tables."""
    task_dir = str(output_dir / "task3")
    cluster_sample = result.clustered_records.sample(
        withReplacement=False,
        fraction=0.02,
        seed=42,
    ).limit(CLUSTER_OUTPUT_SAMPLE_SIZE)
    result.scores.write.mode("overwrite").parquet(f"{task_dir}/silhouette_scores")
    result.stability_scores.write.mode("overwrite").parquet(f"{task_dir}/stability_scores")
    cluster_sample.write.mode("overwrite").parquet(f"{task_dir}/clustered_records")
    result.cluster_summary.write.mode("overwrite").parquet(f"{task_dir}/cluster_summary")
    result.district_alignment.write.mode("overwrite").parquet(f"{task_dir}/district_alignment")
    save_silhouette_chart(result.scores, output_dir / "charts" / "task3_silhouette.png")
    save_elbow_chart(result.scores, output_dir / "charts" / "task3_elbow.png")
    save_cluster_map(cluster_sample, output_dir / "charts" / "task3_cluster_map.png")
    LOGGER.info("best_k_selected value=%d", result.best_k)
    result.scores.show(truncate=False)
    result.cluster_summary.show(truncate=False)


def write_task4_outputs(result: ModelingResult, output_dir: Path) -> None:
    """Persist Task 4 predictions, metrics, matrix, imbalance evidence, and model."""
    task_dir = str(output_dir / "task4")
    result.predictions.write.mode("overwrite").parquet(f"{task_dir}/predictions")
    result.confusion_matrix.write.mode("overwrite").parquet(f"{task_dir}/confusion_matrix")
    result.metrics.write.mode("overwrite").parquet(f"{task_dir}/metrics")
    result.class_distribution.write.mode("overwrite").parquet(f"{task_dir}/class_distribution")
    result.crime_type_distribution.write.mode("overwrite").parquet(
        f"{task_dir}/crime_type_distribution"
    )
    result.model_comparison.write.mode("overwrite").parquet(f"{task_dir}/model_comparison")
    result.threshold_metrics.write.mode("overwrite").parquet(f"{task_dir}/threshold_metrics")
    result.feature_importance.write.mode("overwrite").parquet(f"{task_dir}/feature_importance")
    result.roc_curve.write.mode("overwrite").parquet(f"{task_dir}/roc_curve")
    result.model.write().overwrite().save(f"{task_dir}/random_forest_model")
    for model_name, model in result.comparison_models.items():
        model.write().overwrite().save(f"{task_dir}/{model_name}_model")
    save_roc_curve(result.roc_curve, output_dir / "charts" / "task4_roc_curve.png")
    save_crime_type_distribution_chart(
        result.crime_type_distribution,
        output_dir / "charts" / "task4_crime_type_distribution.png",
    )
    save_feature_importance_chart(
        result.feature_importance,
        output_dir / "charts" / "task4_feature_importance.png",
    )
    save_threshold_metrics_chart(
        result.threshold_metrics,
        output_dir / "charts" / "task4_threshold_metrics.png",
    )
    save_model_comparison_chart(
        result.model_comparison,
        output_dir / "charts" / "task4_model_comparison.png",
    )
    LOGGER.info("task4_class_distribution")
    result.class_distribution.show(truncate=False)
    LOGGER.info("task4_confusion_matrix")
    result.confusion_matrix.show(truncate=False)
    LOGGER.info("task4_model_comparison")
    result.model_comparison.show(truncate=False)
    LOGGER.info("task4_selected_model_metrics")
    result.metrics.show(truncate=False)
    LOGGER.info("task4_most_frequent_crime_types")
    result.crime_type_distribution.show(10, truncate=False)


def write_task2_outputs(result: Task2Result, output_dir: Path) -> None:
    """Persist Task 2 EDA tables for report charts and analysis."""
    task_dir = str(output_dir / "task2")
    result.daily_crime_weather.write.mode("overwrite").parquet(f"{task_dir}/daily_crime_weather")
    result.weather_impact.write.mode("overwrite").parquet(f"{task_dir}/weather_impact")
    result.temporal_patterns.write.mode("overwrite").parquet(f"{task_dir}/temporal_patterns")
    result.weather_correlations.write.mode("overwrite").parquet(
        f"{task_dir}/weather_correlations"
    )
    result.community_area_rolling.write.mode("overwrite").parquet(f"{task_dir}/community_area_rolling")
    result.community_socioeconomic.write.mode("overwrite").parquet(f"{task_dir}/community_socioeconomic")
    result.seasonal_weather_impact.write.mode("overwrite").parquet(
        f"{task_dir}/seasonal_weather_impact"
    )
    result.weather_effect_sizes.write.mode("overwrite").parquet(
        f"{task_dir}/weather_effect_sizes"
    )
    result.weekday_weekend_patterns.write.mode("overwrite").parquet(
        f"{task_dir}/weekday_weekend_patterns"
    )
    result.community_spatial_patterns.write.mode("overwrite").parquet(
        f"{task_dir}/community_spatial_patterns"
    )
    save_weather_impact_chart(result.weather_impact, output_dir / "charts" / "task2_weather_impact.png")
    save_hourly_crime_chart(result.temporal_patterns, output_dir / "charts" / "task2_hourly_crime.png")
    LOGGER.info("task2_weather_impact_summary")
    result.weather_impact.show(truncate=False)
    result.daily_crime_weather.unpersist()
    result.temporal_patterns.unpersist()


def run() -> None:
    """Execute one supported analysis task against the cleaned crime dataset."""
    configure_logging()
    args = parse_arguments()
    spark = create_spark_session(f"DAS7002-{args.task.title()}")
    try:
        crime_frame = spark.read.parquet(str(args.crime_input))
        if args.task == "eda":
            if not args.weather_input or not args.census_input:
                raise ValueError("Task 2 requires --weather-input and --census-input.")
            weather_hourly = spark.read.parquet(str(args.weather_input))
            census_frame = spark.read.parquet(str(args.census_input))
            write_task2_outputs(
                run_task2(crime_frame, weather_hourly, census_frame),
                args.output_dir,
            )
        elif args.task == "cluster":
            write_task3_outputs(fit_spatial_temporal_clusters(crime_frame), args.output_dir)
        else:
            write_task4_outputs(fit_arrest_model(crime_frame), args.output_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
