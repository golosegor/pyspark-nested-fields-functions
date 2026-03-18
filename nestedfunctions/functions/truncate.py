import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql import DataFrame

from nestedfunctions.functions.terminal_operations import apply_terminal_operation


def truncate(df: DataFrame, field: str, character_size: int) -> DataFrame:
    return apply_terminal_operation(df, field, lambda c, t: __truncate(c, character_size))


def __truncate(primitive_value: Column, character_size: int) -> Column:
    if character_size < 0:
        # it is not possible to obtain 'size' of primitive values from the array, so this workaround is mandatory
        # the easiest way would be
        # F.substring(primitive_value, 0, F.length(primitive_value) - 2)
        # but this does not work because "column is not callable". I didn't find a way so this workaround is here
        max_size = 9999_9999
        return F.reverse(F.substring(F.reverse(primitive_value), -character_size + 1, max_size))
    else:
        return F.substring(primitive_value, 1, character_size)
