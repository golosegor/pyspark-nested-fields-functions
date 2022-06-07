import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from nestedfunctions.functions.terminal_operations import apply_terminal_operation, \
    apply_terminal_operation_with_predicate


def format_date(df: DataFrame, field: str, current_date_format: str, target_date_format: str) -> DataFrame:
    return apply_terminal_operation(df, field, lambda c, t: __format_date(c, current_date_format, target_date_format))


def format_date_with_predicate(df: DataFrame,
                               field: str,
                               current_date_format: str,
                               target_date_format: str,
                               predicate_key: str,
                               predicate_value: str) -> DataFrame:
    return apply_terminal_operation_with_predicate(df,
                                                   field,
                                                   lambda c, t: __format_date(c,
                                                                              current_date_format,
                                                                              target_date_format),
                                                   predicate_key,
                                                   predicate_value)


def __format_date(primitive_column: Column, current_date_format: str, target_date_format: str) -> Column:
    date = F.to_timestamp(primitive_column.cast(StringType()), current_date_format)
    return F.date_format(date, target_date_format).cast(StringType())
