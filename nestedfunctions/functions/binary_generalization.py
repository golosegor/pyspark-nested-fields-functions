import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType

from nestedfunctions.functions.terminal_operations import apply_terminal_operation


def binary_generalization(df: DataFrame, field: str) -> DataFrame:
    return apply_terminal_operation(df, field, lambda c, t: __binary_generalize(c))


def __binary_generalize(primitive_column: Column) -> Column:
    return (F.when(F.length(F.trim(primitive_column)) > 0, F.lit(True)).otherwise(F.lit(False))).cast(BooleanType())
