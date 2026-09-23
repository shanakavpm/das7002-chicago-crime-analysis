"""Task 4 distributed binary classification for arrest prediction."""

from dataclasses import dataclass

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel


NUMERIC_FEATURES = ("latitude", "longitude", "hour", "month", "district", "community_area")
MODEL_PARTITIONS = 8
RANDOM_FOREST_TREES = 20


@dataclass(frozen=True)
class ModelingResult:
    """Outputs required to evaluate and discuss the arrest model."""

    model: object
    predictions: DataFrame
    confusion_matrix: DataFrame
    metrics: DataFrame
    class_distribution: DataFrame
    crime_type_distribution: DataFrame
    roc_curve: DataFrame
    model_comparison: DataFrame
    threshold_metrics: DataFrame
    feature_importance: DataFrame


def prepare_modeling_data(crime_frame: DataFrame) -> DataFrame:
    """Build a typed, non-null modelling table while preserving the binary target."""
    return (
        crime_frame.filter(
            F.col("has_valid_coordinates")
            & F.col("has_valid_district")
            & F.col("has_valid_community_area")
        )
        .filter(F.col("arrest").isNotNull() & F.col("primary_type").isNotNull())
        .dropna(subset=[*NUMERIC_FEATURES])
        .withColumn("label", F.col("arrest").cast("double"))
        .select("crime_id", "primary_type", "label", "year", *NUMERIC_FEATURES)
    )


def add_class_weights(frame: DataFrame) -> DataFrame:
    """Add inverse-frequency binary class weights calculated from the training data."""
    counts = {float(row.label): int(row["count"]) for row in frame.groupBy("label").count().collect()}
    if set(counts) != {0.0, 1.0}:
        raise ValueError("Class weighting requires both arrest outcome classes.")
    total = sum(counts.values())
    negative_weight = total / (2 * counts[0.0])
    positive_weight = total / (2 * counts[1.0])
    return frame.withColumn(
        "class_weight",
        F.when(F.col("label") == 1.0, F.lit(positive_weight)).otherwise(
            F.lit(negative_weight)
        ),
    )


def create_random_forest_pipeline(weighted: bool) -> Pipeline:
    """Build one consistent feature and Random Forest pipeline."""
    type_indexer = StringIndexer(
        inputCol="primary_type",
        outputCol="primary_type_index",
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=[*NUMERIC_FEATURES, "primary_type_index"],
        outputCol="features",
    )
    classifier = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=RANDOM_FOREST_TREES,
        maxBins=64,
        seed=42,
    )
    if weighted:
        classifier = classifier.setWeightCol("class_weight")
    return Pipeline(stages=[type_indexer, assembler, classifier])


def calculate_binary_metrics(
    predictions: DataFrame,
    model_name: str,
    validation_strategy: str,
    temporal_cutoff_year: int = 0,
    probability_metrics: bool = True,
) -> tuple:
    """Collect compact confusion-matrix and ranking metrics for one model."""
    counts = predictions.agg(
        F.sum(F.when((F.col("label") == 1) & (F.col("prediction") == 1), 1).otherwise(0)).alias(
            "true_positive"
        ),
        F.sum(F.when((F.col("label") == 0) & (F.col("prediction") == 1), 1).otherwise(0)).alias(
            "false_positive"
        ),
        F.sum(F.when((F.col("label") == 1) & (F.col("prediction") == 0), 1).otherwise(0)).alias(
            "false_negative"
        ),
        F.sum(F.when((F.col("label") == 0) & (F.col("prediction") == 0), 1).otherwise(0)).alias(
            "true_negative"
        ),
    ).first()
    true_positive = int(counts.true_positive)
    false_positive = int(counts.false_positive)
    false_negative = int(counts.false_negative)
    true_negative = int(counts.true_negative)
    precision = true_positive / max(true_positive + false_positive, 1)
    recall = true_positive / max(true_positive + false_negative, 1)
    specificity = true_negative / max(true_negative + false_positive, 1)
    f1_score = 2 * precision * recall / max(precision + recall, 1e-12)
    balanced_accuracy = (recall + specificity) / 2
    positive_share = (true_positive + false_negative) / max(
        true_positive + false_positive + false_negative + true_negative,
        1,
    )
    if probability_metrics:
        roc_evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        )
        pr_evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderPR",
        )
        area_under_roc = float(roc_evaluator.evaluate(predictions))
        area_under_pr = float(pr_evaluator.evaluate(predictions))
    else:
        area_under_roc = 0.5
        area_under_pr = positive_share
    return (
        model_name,
        validation_strategy,
        temporal_cutoff_year,
        true_positive,
        false_positive,
        false_negative,
        true_negative,
        precision,
        recall,
        specificity,
        f1_score,
        balanced_accuracy,
        area_under_roc,
        area_under_pr,
    )


def create_threshold_metrics(predictions: DataFrame) -> DataFrame:
    """Evaluate precision-recall trade-offs across alternative arrest thresholds."""
    thresholds = predictions.sparkSession.createDataFrame(
        [(value,) for value in (0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8)],
        ["threshold"],
    )
    scored = predictions.select(
        "label",
        vector_to_array("probability")[1].alias("arrest_probability"),
    )
    evaluated = scored.crossJoin(F.broadcast(thresholds)).withColumn(
        "threshold_prediction",
        (F.col("arrest_probability") >= F.col("threshold")).cast("double"),
    )
    counts = evaluated.groupBy("threshold").agg(
        F.sum(F.when((F.col("label") == 1) & (F.col("threshold_prediction") == 1), 1).otherwise(0)).alias(
            "true_positive"
        ),
        F.sum(F.when((F.col("label") == 0) & (F.col("threshold_prediction") == 1), 1).otherwise(0)).alias(
            "false_positive"
        ),
        F.sum(F.when((F.col("label") == 1) & (F.col("threshold_prediction") == 0), 1).otherwise(0)).alias(
            "false_negative"
        ),
    )
    precision_denominator = F.col("true_positive") + F.col("false_positive")
    recall_denominator = F.col("true_positive") + F.col("false_negative")
    return (
        counts.withColumn(
            "precision",
            F.when(precision_denominator > 0, F.col("true_positive") / precision_denominator),
        )
        .withColumn(
            "recall",
            F.when(recall_denominator > 0, F.col("true_positive") / recall_denominator),
        )
        .withColumn(
            "f1_score",
            F.when(
                F.col("precision") + F.col("recall") > 0,
                2
                * F.col("precision")
                * F.col("recall")
                / (F.col("precision") + F.col("recall")),
            ),
        )
        .orderBy("threshold")
    )


def create_feature_importance(model, spark_session) -> DataFrame:
    """Map Random Forest importance values back to human-readable feature names."""
    feature_names = [*NUMERIC_FEATURES, "primary_type"]
    importances = model.stages[-1].featureImportances.toArray().tolist()
    rows = [(name, float(value)) for name, value in zip(feature_names, importances)]
    return spark_session.createDataFrame(rows, ["feature", "importance"]).orderBy(
        F.desc("importance")
    )


def create_roc_curve(predictions: DataFrame) -> DataFrame:
    """Create ROC points from predicted probabilities using Spark DataFrame operations."""
    scored_records = predictions.select(
        vector_to_array(F.col("probability"))[1].alias("score"),
        F.col("label"),
    )
    totals = scored_records.agg(
        F.sum(F.when(F.col("label") == 1, 1).otherwise(0)).alias("positive_count"),
        F.sum(F.when(F.col("label") == 0, 1).otherwise(0)).alias("negative_count"),
    )
    threshold_counts = scored_records.groupBy("score").agg(
        F.sum(F.when(F.col("label") == 1, 1).otherwise(0)).alias("positives_at_threshold"),
        F.sum(F.when(F.col("label") == 0, 1).otherwise(0)).alias("negatives_at_threshold"),
    )
    cumulative_window = Window.orderBy(F.col("score").desc()).rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )
    roc_points = (
        threshold_counts.crossJoin(totals)
        .withColumn(
            "true_positive_rate",
            F.sum("positives_at_threshold").over(cumulative_window) / F.col("positive_count"),
        )
        .withColumn(
            "false_positive_rate",
            F.sum("negatives_at_threshold").over(cumulative_window) / F.col("negative_count"),
        )
        .select("false_positive_rate", "true_positive_rate")
    )
    endpoints = predictions.sparkSession.createDataFrame(
        [(0.0, 0.0), (1.0, 1.0)],
        ["false_positive_rate", "true_positive_rate"],
    )
    return endpoints.unionByName(roc_points).orderBy("false_positive_rate", "true_positive_rate")


def fit_arrest_model(crime_frame: DataFrame) -> ModelingResult:
    """Fit Random Forest, evaluate the holdout set, and surface class imbalance."""
    # The source Parquet can contain hundreds of small partition files. A bounded
    # partition count avoids local scheduling overhead while retaining Spark ML.
    modelling_data = (
        prepare_modeling_data(crime_frame)
        .coalesce(MODEL_PARTITIONS)
        .persist(StorageLevel.DISK_ONLY)
    )
    modelling_record_count = modelling_data.count()
    if modelling_data.select("label").distinct().count() < 2:
        raise ValueError("Arrest prediction requires both arrest outcome classes.")

    class_distribution = (
        modelling_data.groupBy("label")
        .count()
        .withColumn(
            "class_share",
            F.round(F.col("count") / F.lit(modelling_record_count), 4),
        )
        .orderBy("label")
    )
    crime_type_distribution = (
        modelling_data.groupBy("primary_type")
        .count()
        .withColumn("type_share", F.round(F.col("count") / F.lit(modelling_record_count), 4))
        .orderBy(F.desc("count"), "primary_type")
    )
    train_frame, test_frame = modelling_data.randomSplit([0.8, 0.2], seed=42)
    if train_frame.select("label").distinct().count() < 2:
        raise ValueError("The training split requires both arrest outcome classes.")
    if test_frame.select("label").distinct().count() < 2:
        raise ValueError("The test split requires both arrest outcome classes.")

    unweighted_model = create_random_forest_pipeline(weighted=False).fit(train_frame)
    unweighted_predictions = unweighted_model.transform(test_frame).persist(StorageLevel.DISK_ONLY)
    weighted_model = create_random_forest_pipeline(weighted=True).fit(add_class_weights(train_frame))
    predictions = weighted_model.transform(test_frame).persist(StorageLevel.DISK_ONLY)

    majority_predictions = test_frame.select("label").withColumn("prediction", F.lit(0.0))
    comparison_rows = [
        calculate_binary_metrics(
            majority_predictions,
            model_name="majority_class_baseline",
            validation_strategy="seeded_random_holdout",
            probability_metrics=False,
        ),
        calculate_binary_metrics(
            unweighted_predictions,
            model_name="unweighted_random_forest",
            validation_strategy="seeded_random_holdout",
        ),
        calculate_binary_metrics(
            predictions,
            model_name="weighted_random_forest",
            validation_strategy="seeded_random_holdout",
        ),
    ]

    maximum_year = int(modelling_data.agg(F.max("year")).first()[0])
    temporal_cutoff_year = maximum_year - 3
    temporal_train = modelling_data.filter(F.col("year") < temporal_cutoff_year)
    temporal_test = modelling_data.filter(F.col("year") >= temporal_cutoff_year)
    if (
        temporal_train.select("label").distinct().count() == 2
        and temporal_test.select("label").distinct().count() == 2
    ):
        temporal_model = create_random_forest_pipeline(weighted=True).fit(
            add_class_weights(temporal_train)
        )
        temporal_predictions = temporal_model.transform(temporal_test).persist(
            StorageLevel.DISK_ONLY
        )
        comparison_rows.append(
            calculate_binary_metrics(
                temporal_predictions,
                model_name="weighted_random_forest",
                validation_strategy="temporal_holdout",
                temporal_cutoff_year=temporal_cutoff_year,
            )
        )
        temporal_predictions.unpersist()

    comparison_columns = [
        "model_name",
        "validation_strategy",
        "temporal_cutoff_year",
        "true_positive",
        "false_positive",
        "false_negative",
        "true_negative",
        "precision",
        "recall",
        "specificity",
        "f1_score",
        "balanced_accuracy",
        "area_under_roc",
        "area_under_pr",
    ]
    model_comparison = crime_frame.sparkSession.createDataFrame(
        comparison_rows,
        comparison_columns,
    )
    metrics = model_comparison.filter(
        (F.col("model_name") == "weighted_random_forest")
        & (F.col("validation_strategy") == "seeded_random_holdout")
    )
    confusion_matrix = predictions.groupBy("label", "prediction").count().orderBy(
        "label", "prediction"
    )
    roc_curve = create_roc_curve(predictions).cache()
    threshold_metrics = create_threshold_metrics(predictions)
    feature_importance = create_feature_importance(weighted_model, crime_frame.sparkSession)
    prediction_output = predictions.select(
        "crime_id",
        "label",
        "prediction",
        vector_to_array("probability")[1].alias("arrest_probability"),
    )
    return ModelingResult(
        model=weighted_model,
        predictions=prediction_output,
        confusion_matrix=confusion_matrix,
        metrics=metrics,
        class_distribution=class_distribution,
        crime_type_distribution=crime_type_distribution,
        roc_curve=roc_curve,
        model_comparison=model_comparison,
        threshold_metrics=threshold_metrics,
        feature_importance=feature_importance,
    )
