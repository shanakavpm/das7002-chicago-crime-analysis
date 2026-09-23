"""Task 3 spatial-temporal clustering using Spark ML K-Means."""

from dataclasses import dataclass

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.storagelevel import StorageLevel


CLUSTER_FEATURES = ("latitude", "longitude", "hour")
CLUSTER_TRAINING_FRACTION = 0.002
CLUSTER_TRAINING_PARTITIONS = 8
MINIMUM_SAMPLE_RECORDS = 100


@dataclass(frozen=True)
class ClusteringResult:
    """Model outputs required for Task 3 analysis and reporting."""

    best_k: int
    scores: DataFrame
    clustered_records: DataFrame
    cluster_summary: DataFrame
    district_alignment: DataFrame


def prepare_clustering_input(crime_frame: DataFrame) -> DataFrame:
    """Retain only the fields needed for spatial-temporal clustering and reporting."""
    return (
        crime_frame.filter(F.col("has_valid_coordinates"))
        .dropna(subset=list(CLUSTER_FEATURES))
        .select("crime_id", "latitude", "longitude", "hour", "district")
    )


def fit_spatial_temporal_clusters(
    crime_frame: DataFrame,
    candidates: range = range(2, 7),
) -> ClusteringResult:
    """Choose K on a seeded sample, then profile all valid crime records with the chosen model."""
    input_frame = prepare_clustering_input(crime_frame)
    sampled_input = (
        input_frame.sample(
            withReplacement=False,
            fraction=CLUSTER_TRAINING_FRACTION,
            seed=42,
        )
        .coalesce(CLUSTER_TRAINING_PARTITIONS)
        .persist(StorageLevel.DISK_ONLY)
    )
    if sampled_input.limit(MINIMUM_SAMPLE_RECORDS).count() < MINIMUM_SAMPLE_RECORDS:
        sampled_input.unpersist()
        sampled_input = input_frame.coalesce(CLUSTER_TRAINING_PARTITIONS).persist(
            StorageLevel.DISK_ONLY
        )
    if sampled_input.select("hour").distinct().count() < 2:
        raise ValueError(
            "Task 3 requires timestamps with varying time-of-day values. "
            "The current crime dataset contains date-only timestamps."
        )
    assembler = VectorAssembler(inputCols=list(CLUSTER_FEATURES), outputCol="features_raw")
    sampled_assembled = assembler.transform(sampled_input)
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True,
    )
    scaler_model = scaler.fit(sampled_assembled)
    training_features = scaler_model.transform(sampled_assembled).persist(StorageLevel.DISK_ONLY)
    training_record_count = training_features.count()
    valid_candidates = [k for k in candidates if 1 < k < training_record_count]
    if not valid_candidates:
        raise ValueError("Clustering requires at least three valid crime records.")

    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    )
    score_rows = []
    best_model = None
    best_k = None
    best_score = float("-inf")
    for k in valid_candidates:
        model = KMeans(
            k=k,
            seed=42,
            featuresCol="features",
            predictionCol="cluster",
        ).fit(training_features)
        training_predictions = model.transform(training_features).cache()
        score = evaluator.evaluate(training_predictions)
        cluster_sizes = [
            row.incident_count
            for row in training_predictions.groupBy("cluster")
            .agg(F.count("*").alias("incident_count"))
            .collect()
        ]
        district_counts = training_predictions.filter(F.col("district").isNotNull()).groupBy(
            "cluster", "district"
        ).count()
        dominant_district_share = (
            district_counts.withColumn(
                "district_share",
                F.col("count") / F.sum("count").over(Window.partitionBy("cluster")),
            )
            .groupBy("cluster")
            .agg(F.max("district_share").alias("dominant_district_share"))
            .agg(F.avg("dominant_district_share").alias("average_dominant_district_share"))
            .first()
            .average_dominant_district_share
        )
        score_rows.append(
            (
                k,
                score,
                float(model.summary.trainingCost),
                min(cluster_sizes),
                max(cluster_sizes),
                float(dominant_district_share),
            )
        )
        training_predictions.unpersist()
        if score > best_score:
            best_k = k
            best_model = model
            best_score = score

    scores = crime_frame.sparkSession.createDataFrame(
        score_rows,
        [
            "k",
            "silhouette_score",
            "training_cost",
            "minimum_cluster_size",
            "maximum_cluster_size",
            "average_dominant_district_share",
        ],
    ).orderBy("k")
    if best_model is None or best_k is None:
        raise RuntimeError("No K-Means candidate model was selected.")
    full_features = scaler_model.transform(assembler.transform(input_frame))
    clustered_records = (
        best_model.transform(full_features)
        .select("crime_id", "latitude", "longitude", "hour", "district", "cluster")
        .persist(StorageLevel.DISK_ONLY)
    )
    clustered_records.count()
    sampled_input.unpersist()
    training_features.unpersist()

    cluster_summary_base = (
        clustered_records.groupBy("cluster")
        .agg(
            F.count("*").alias("incident_count"),
            F.avg("latitude").alias("centroid_latitude"),
            F.avg("longitude").alias("centroid_longitude"),
            F.avg("hour").alias("average_hour"),
            F.countDistinct("district").alias("district_count"),
            F.var_pop("latitude").alias("latitude_variance"),
            F.var_pop("longitude").alias("longitude_variance"),
        )
        .withColumn(
            "spatial_dispersion",
            F.sqrt(F.col("latitude_variance") + F.col("longitude_variance")),
        )
        .withColumn(
            "activity_concentration_index",
            F.col("incident_count") / F.greatest(F.col("spatial_dispersion"), F.lit(1e-6)),
        )
    )
    concentration_threshold = cluster_summary_base.agg(
        F.avg("activity_concentration_index").alias("average_activity_concentration")
    )
    cluster_summary = (
        cluster_summary_base.crossJoin(concentration_threshold)
        .withColumn(
            "is_high_activity_zone",
            F.col("activity_concentration_index") >= F.col("average_activity_concentration"),
        )
        .withColumn("crosses_district_boundaries", F.col("district_count") > 1)
        .drop("latitude_variance", "longitude_variance", "average_activity_concentration")
        .orderBy("cluster")
    )

    district_alignment = (
        clustered_records.groupBy("cluster", "district")
        .count()
        .withColumnRenamed("count", "incident_count")
        .withColumn(
            "cluster_share",
            F.round(
                F.col("incident_count")
                / F.sum("incident_count").over(Window.partitionBy("cluster")),
                4,
            ),
        )
        .orderBy("cluster", F.desc("incident_count"))
    )
    return ClusteringResult(best_k, scores, clustered_records, cluster_summary, district_alignment)
