import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType

from nestedfunctions.functions.terminal_operations import apply_terminal_operation, \
    apply_terminal_operation_with_predicate

DEFAULT_SALT_SEPARATOR = "¿"

DEFAULT_SALT_VALUE = "wof:?_fTNy/6bshXV@xh"


def hash_field(df: DataFrame, field: str, num_bits: int = 256) -> DataFrame:
    return apply_terminal_operation(df, field, lambda c, t: __apply_hash(c, num_bits))


def hash_field_with_salt(df: DataFrame,
                         field: str,
                         salt: str = DEFAULT_SALT_VALUE,
                         separator: str = DEFAULT_SALT_SEPARATOR,
                         num_bits: int = 256) -> DataFrame:
    return apply_terminal_operation(df, field,
                                    lambda c, t: __apply_hash(F.concat_ws(separator, F.lit(salt), c.cast(StringType())),
                                                              num_bits))


def hash_field_with_predicate(df: DataFrame,
                              field: str,
                              predicate_key: str,
                              predicate_value: str,
                              num_bits: int = 256) -> DataFrame:
    return apply_terminal_operation_with_predicate(df,
                                                   field,
                                                   lambda c, t: __apply_hash(c, num_bits),
                                                   predicate_key,
                                                   predicate_value)


def __apply_hash(c: Column, num_bits: int = 256):
    return F.sha2(c.cast(StringType()), num_bits)
