"""Focused tests for Task 4 evaluation helpers."""

from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as F

from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.feature import OneHotEncoder

from chicago_crime.modeling import (
    add_class_weights,
    create_gradient_boosted_tree_pipeline,
    create_logistic_regression_pipeline,
    create_roc_curve,
    create_threshold_metrics,
)


def test_roc_curve_contains_endpoints_and_ranked_predictions(spark):
    predictions = spark.createDataFrame(
        [
            (Vectors.dense([0.9, 0.1]), 0.0),
            (Vectors.dense([0.2, 0.8]), 1.0),
        ],
        ["probability", "label"],
    )

    points = {
        (round(row.false_positive_rate, 4), round(row.true_positive_rate, 4))
        for row in create_roc_curve(predictions).collect()
    }

    assert (0.0, 0.0) in points
    assert (1.0, 1.0) in points


def test_class_weights_give_each_class_equal_total_weight(spark):
    frame = spark.createDataFrame([(0.0,), (0.0,), (0.0,), (1.0,)], ["label"])

    totals = {
        row.label: round(row.total_weight, 6)
        for row in add_class_weights(frame)
        .groupBy("label")
        .agg(F.sum("class_weight").alias("total_weight"))
        .collect()
    }

    assert totals == {0.0: 2.0, 1.0: 2.0}


def test_threshold_metrics_show_precision_recall_tradeoff(spark):
    predictions = spark.createDataFrame(
        [
            (Vectors.dense([0.1, 0.9]), 1.0),
            (Vectors.dense([0.4, 0.6]), 0.0),
            (Vectors.dense([0.7, 0.3]), 1.0),
            (Vectors.dense([0.9, 0.1]), 0.0),
        ],
        ["probability", "label"],
    )

    rows = {row.threshold: row for row in create_threshold_metrics(predictions).collect()}

    assert rows[0.2].recall == 1.0
    assert rows[0.8].precision == 1.0


def test_comparison_pipelines_use_expected_estimators_and_encoding(spark):
    logistic_stages = create_logistic_regression_pipeline().getStages()
    gbt_stages = create_gradient_boosted_tree_pipeline().getStages()

    assert any(isinstance(stage, OneHotEncoder) for stage in logistic_stages)
    assert isinstance(logistic_stages[-1], LogisticRegression)
    assert logistic_stages[-1].getWeightCol() == "class_weight"
    assert isinstance(gbt_stages[-1], GBTClassifier)
    assert gbt_stages[-1].getWeightCol() == "class_weight"
