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
    """
        Function to do dateformatting
        | if the source value is null -> preserve the null by casting it to 'string' type
        | if source value is NOT null and current_date_format is matching date column -> do format
        | if source value is NOT null and current_date_format is NOT matching date column -> throw exception
    """
    date_as_ts = F.to_timestamp(primitive_column.cast(StringType()), current_date_format)
    return F.when(F.isnull(primitive_column), primitive_column.cast(StringType())) \
        .otherwise((F.when(F.isnull(date_as_ts), F.raise_error(f"Wrong pattern {current_date_format}. "
                                                               f"Could not convert provided value to timestamp."))
                    .otherwise(F.date_format(date_as_ts, target_date_format).cast(StringType()))))
