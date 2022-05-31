import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import BooleanType

from nestedfunctions.generic.terminal_operations import apply_terminal_operation


def binary_generalization(df: DataFrame, field: str) -> DataFrame:
    return apply_terminal_operation(df=df,
                                    field=field,
                                    f=lambda c, t: __binary_generalization(primitive_column=c))


def __binary_generalization(primitive_column: Column) -> Column:
    return (F.when(F.length(F.trim(primitive_column)) > 0, F.lit(True)).otherwise(F.lit(False))).cast(BooleanType())
