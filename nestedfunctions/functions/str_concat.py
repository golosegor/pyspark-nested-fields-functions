import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql import DataFrame

from nestedfunctions.functions.terminal_operations import apply_terminal_operation


def str_concat(df: DataFrame, field: str, str_to_add: str) -> DataFrame:
    return apply_terminal_operation(df, field,
                                    lambda c, t: __str_concat(c, str_to_add))


def __str_concat(primitive_value: Column, value_to_add: str) -> Column:
    return F.concat(primitive_value, F.lit(value_to_add))
