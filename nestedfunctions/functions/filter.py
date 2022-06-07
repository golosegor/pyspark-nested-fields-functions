import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def df_filter(df: DataFrame, predicate: str) -> DataFrame:
    return df.filter(F.expr(predicate))
