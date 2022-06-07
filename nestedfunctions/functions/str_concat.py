from pyspark.sql import DataFrame

from nestedfunctions.processors.str_concat.str_concat import StringConcatProcessor


def str_concat(df: DataFrame, field: str, str_to_add: str) -> DataFrame:
    return StringConcatProcessor(field, str_to_add).process(df)
