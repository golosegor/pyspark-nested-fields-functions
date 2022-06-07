from pyspark.sql import DataFrame

from nestedfunctions.processors.expr.expr import ExprProcessor


def expr(df: DataFrame, field: str, expr: str) -> DataFrame:
    if not expr:
        raise ValueError("Expr could not be empty")
    return ExprProcessor(field, expr).process(df)
