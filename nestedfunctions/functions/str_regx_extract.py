import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql import DataFrame

from nestedfunctions.functions.terminal_operations import apply_terminal_operation
from nestedfunctions.validation.validators import validate_regexp_or_throw


def str_regx_extract(df: DataFrame, field: str, pattern: str, group_index_to_extract: int) -> DataFrame:
    validate_regexp_or_throw(pattern)
    return apply_terminal_operation(df,
                                    field,
                                    lambda c, t: __extract(c, pattern, group_index_to_extract))


def __extract(primitive_value: Column, pattern: str, group_index_to_extract: int) -> Column:
    return F.regexp_extract(primitive_value, pattern, group_index_to_extract)
