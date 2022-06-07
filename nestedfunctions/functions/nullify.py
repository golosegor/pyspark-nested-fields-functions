from pyspark.sql import Column, functions as F
from pyspark.sql import DataFrame

from nestedfunctions.functions.terminal_operations import apply_terminal_operation, \
    apply_terminal_operation_with_predicate


def nullify(df: DataFrame, field: str) -> DataFrame:
    return apply_terminal_operation(df, field,
                                    lambda c, t: __nullify())


def nullify_with_predicate(df: DataFrame,
                           field: str,
                           predicate_key: str,
                           predicate_value: str) -> DataFrame:
    return apply_terminal_operation_with_predicate(df,
                                                   field,
                                                   lambda c, t: __nullify(),
                                                   predicate_key,
                                                   predicate_value)


def __nullify() -> Column:
    return F.lit(None)
