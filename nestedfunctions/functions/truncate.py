from pyspark.sql import DataFrame

from nestedfunctions.processors.truncate.truncate_processor import TruncateProcessor


def truncate(df: DataFrame, field: str, character_size: int) -> DataFrame:
    return TruncateProcessor(column_to_process=field,
                             character_size=character_size).process(df)
