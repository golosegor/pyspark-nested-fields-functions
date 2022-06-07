from pyspark.sql import DataFrame

from nestedfunctions.processors.nullify.nullify_processor import NullabilityProcessor, NullabilityProcessorWithPredicate
from nestedfunctions.processors.terminal_operation_processor_with_predicate import PredicateProcessorParameters


def nullify(df: DataFrame, field: str) -> DataFrame:
    return NullabilityProcessor(field).process(df)


def nullify_with_predicate(df: DataFrame,
                           field: str,
                           predicate_key: str,
                           predicate_value: str) -> DataFrame:
    return NullabilityProcessorWithPredicate(field,
                                             PredicateProcessorParameters(predicate_key, predicate_value)).process(df)
