from pyspark.sql import DataFrame

from nestedfunctions.processors.filter.filter_processor import FilterProcessor


def df_filter(df: DataFrame, predicate: str) -> DataFrame:
    return FilterProcessor(predicate).process(df)
