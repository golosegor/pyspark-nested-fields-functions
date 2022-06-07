from pyspark.sql import DataFrame

from nestedfunctions.processors.binary_generalization.binary_generalization_processor import BinaryGeneralizationProcessor


def binary_generalization(df: DataFrame, field: str) -> DataFrame:
    return BinaryGeneralizationProcessor(field).process(df)
