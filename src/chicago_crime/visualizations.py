"""Small chart functions that only receive already-aggregated Spark outputs."""

from pathlib import Path

import matplotlib.pyplot as plt
from pyspark.sql import DataFrame


def _prepare_output(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def save_roc_curve(roc_points: DataFrame, output_path: Path) -> None:
    """Save a report-ready ROC curve from a small evaluation table."""
    points = roc_points.orderBy("false_positive_rate").toPandas()
    _prepare_output(output_path)
    figure, axis = plt.subplots(figsize=(7, 5))
    axis.plot(points["false_positive_rate"], points["true_positive_rate"], label="Random Forest")
    axis.plot([0, 1], [0, 1], linestyle="--", color="grey", label="No-skill baseline")
    axis.set(xlabel="False positive rate", ylabel="True positive rate", title="ROC Curve for Arrest Prediction")
    axis.legend()
    axis.grid(alpha=0.25)
    figure.tight_layout()
    figure.savefig(output_path, dpi=180)
    plt.close(figure)


def save_silhouette_chart(scores: DataFrame, output_path: Path) -> None:
    """Save mean silhouette with seed-to-seed variability for Task 3."""
    values = scores.orderBy("k").toPandas()
    _prepare_output(output_path)
    figure, axis = plt.subplots(figsize=(7, 5))
    axis.errorbar(
        values["k"],
        values["silhouette_score"],
        yerr=values["silhouette_stddev"],
        marker="o",
        capsize=4,
    )
    axis.set(
        xlabel="Number of clusters K",
        ylabel="Mean silhouette score",
        title="K-Means Cluster Selection Across Three Seeds",
    )
    axis.grid(alpha=0.25)
    figure.tight_layout()
    figure.savefig(output_path, dpi=180)
    plt.close(figure)


def save_elbow_chart(scores: DataFrame, output_path: Path) -> None:
    """Save K versus within-cluster training cost for elbow analysis."""
    values = scores.orderBy("k").toPandas()
    _prepare_output(output_path)
    figure, axis = plt.subplots(figsize=(7, 5))
    axis.plot(values["k"], values["training_cost"], marker="o")
    axis.set(
        xlabel="Number of clusters K",
        ylabel="Within-cluster training cost",
        title="K-Means Elbow Analysis",
    )
    axis.grid(alpha=0.25)
    figure.tight_layout()
    figure.savefig(output_path, dpi=180)
    plt.close(figure)


def save_weather_impact_chart(weather_impact: DataFrame, output_path: Path) -> None:
    """Save the required weather-event versus average-crime comparison chart."""
    values = weather_impact.orderBy("weather_condition").toPandas()
    _prepare_output(output_path)
    figure, axis = plt.subplots(figsize=(8, 5))
    axis.bar(values["weather_condition"], values["average_daily_crime_count"])
    axis.set(
        xlabel="ERA5 weather condition",
        ylabel="Average daily crime count",
        title="Crime Frequency by ERA5 Weather Condition",
    )
    axis.tick_params(axis="x", rotation=20)
    figure.tight_layout()
    figure.savefig(output_path, dpi=180)
    plt.close(figure)


def save_hourly_crime_chart(temporal_patterns: DataFrame, output_path: Path) -> None:
    """Save total crime frequency by hour from the PySpark SQL temporal table."""
    values = (
        temporal_patterns.groupBy("hour")
        .sum("crime_count")
        .withColumnRenamed("sum(crime_count)", "crime_count")
        .orderBy("hour")
        .toPandas()
    )
    _prepare_output(output_path)
    figure, axis = plt.subplots(figsize=(8, 5))
    axis.plot(values["hour"], values["crime_count"], marker="o")
    axis.set(
        xlabel="Hour of day",
        ylabel="Crime count",
        title="Crime Frequency by Hour of Day",
    )
    axis.set_xticks(range(0, 24, 2))
    axis.grid(alpha=0.25)
    figure.tight_layout()
    figure.savefig(output_path, dpi=180)
    plt.close(figure)


def save_crime_type_distribution_chart(distribution: DataFrame, output_path: Path) -> None:
    """Save the most frequent crime types used to discuss modelling data skew."""
    values = distribution.limit(10).orderBy("count").toPandas()
    _prepare_output(output_path)
    figure, axis = plt.subplots(figsize=(9, 6))
    axis.barh(values["primary_type"], values["count"])
    axis.set(
        xlabel="Crime records",
        ylabel="Primary crime type",
        title="Ten Most Frequent Crime Types",
    )
    figure.tight_layout()
    figure.savefig(output_path, dpi=180)
    plt.close(figure)


def save_feature_importance_chart(feature_importance: DataFrame, output_path: Path) -> None:
    """Save Random Forest feature importance values."""
    values = feature_importance.orderBy("importance").toPandas()
    _prepare_output(output_path)
    figure, axis = plt.subplots(figsize=(8, 5))
    axis.barh(values["feature"], values["importance"])
    axis.set(
        xlabel="Importance",
        ylabel="Feature",
        title="Weighted Random Forest Feature Importance",
    )
    figure.tight_layout()
    figure.savefig(output_path, dpi=180)
    plt.close(figure)


def save_threshold_metrics_chart(threshold_metrics: DataFrame, output_path: Path) -> None:
    """Save precision, recall, and F1 across alternative probability thresholds."""
    values = threshold_metrics.orderBy("threshold").toPandas()
    _prepare_output(output_path)
    figure, axis = plt.subplots(figsize=(8, 5))
    for metric in ("precision", "recall", "f1_score"):
        axis.plot(values["threshold"], values[metric], marker="o", label=metric)
    axis.set(
        xlabel="Arrest probability threshold",
        ylabel="Metric value",
        title="Classification Threshold Trade-offs",
    )
    axis.set_ylim(0, 1)
    axis.grid(alpha=0.25)
    axis.legend()
    figure.tight_layout()
    figure.savefig(output_path, dpi=180)
    plt.close(figure)


def save_model_comparison_chart(model_comparison: DataFrame, output_path: Path) -> None:
    """Compare precision, recall, and F1 for models on the seeded random holdout."""
    values = (
        model_comparison.filter("validation_strategy = 'seeded_random_holdout'")
        .select("model_name", "precision", "recall", "f1_score")
        .toPandas()
    )
    _prepare_output(output_path)
    figure, axis = plt.subplots(figsize=(10, 6))
    positions = range(len(values))
    width = 0.24
    for offset, metric in zip((-width, 0, width), ("precision", "recall", "f1_score")):
        axis.bar([position + offset for position in positions], values[metric], width, label=metric)
    labels = values["model_name"].str.replace("_", " ")
    axis.set_xticks(list(positions), labels, rotation=18, ha="right")
    axis.set(
        ylabel="Metric value",
        title="Arrest Prediction Model Comparison",
        ylim=(0, 1),
    )
    axis.legend()
    axis.grid(axis="y", alpha=0.25)
    figure.tight_layout()
    figure.savefig(output_path, dpi=180)
    plt.close(figure)


def save_cluster_map(clustered_records: DataFrame, output_path: Path) -> None:
    """Save a bounded spatial scatter plot of the K-Means hotspot clusters."""
    points = (
        clustered_records.select("longitude", "latitude", "cluster")
        .sample(withReplacement=False, fraction=0.1, seed=42)
        .limit(20_000)
        .toPandas()
    )
    _prepare_output(output_path)
    figure, axis = plt.subplots(figsize=(7, 6))
    scatter = axis.scatter(points["longitude"], points["latitude"], c=points["cluster"], s=8, alpha=0.55)
    axis.set(xlabel="Longitude", ylabel="Latitude", title="Spatial Temporal Crime Clusters")
    axis.legend(*scatter.legend_elements(), title="Cluster")
    figure.tight_layout()
    figure.savefig(output_path, dpi=180)
    plt.close(figure)
