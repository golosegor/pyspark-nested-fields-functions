from pyspark.sql import DataFrame

from nestedfunctions.processors.dropping.drop_processor import DropProcessor


def drop(df: DataFrame, field: str) -> DataFrame:
    return DropProcessor(field).process(df)
