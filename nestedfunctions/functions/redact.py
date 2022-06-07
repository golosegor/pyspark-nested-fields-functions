from pyspark.sql import DataFrame

from nestedfunctions.processors.redact.redact_processor import RedactProcessor


def redact(df: DataFrame, field: str) -> DataFrame:
    return RedactProcessor(field).process(df)
