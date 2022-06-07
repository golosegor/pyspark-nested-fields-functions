import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType

from nestedfunctions.processors.hashing.hash_processors import HashProcessor, HashProcessorWithPredicate
from nestedfunctions.processors.hashing.hash_with_salt import HashWithSaltProcessor
from nestedfunctions.processors.terminal_operation_processor_with_predicate import PredicateProcessorParameters

DEFAULT_SALT_SEPARATOR = "¿"

DEFAULT_SALT_VALUE = "wof:?_fTNy/6bshXV@xh"


def hash_field(df: DataFrame, field: str, num_bits=256) -> DataFrame:
    return HashProcessor(field, num_bits).process(df)


def hash_field_with_salt(df: DataFrame,
                         field: str,
                         salt: str = DEFAULT_SALT_VALUE,
                         separator: str = DEFAULT_SALT_SEPARATOR,
                         num_bits=256) -> DataFrame:
    return HashWithSaltProcessor(field, salt, separator, num_bits).process(df)


def hash_field_with_predicate(df: DataFrame,
                              field: str,
                              predicate_key: str,
                              predicate_value: str,
                              num_bits=256) -> DataFrame:
    return HashProcessorWithPredicate(column_to_process=field,
                                      p=PredicateProcessorParameters(predicate_key=predicate_key,
                                                                     predicate_value=predicate_value),
                                      bits=num_bits) \
        .process(df)


def __apply_hash(c: Column, num_bits: 256):
    return F.sha2(c.cast(StringType()), num_bits)
