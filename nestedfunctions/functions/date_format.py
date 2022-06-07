from pyspark.sql import DataFrame

from nestedfunctions.processors.dateformat.date_format_processor import DateFormatProcessor, \
    DateFormatProcessorWithPredicate
from nestedfunctions.processors.terminal_operation_processor_with_predicate import PredicateProcessorParameters


def format_date(df: DataFrame, field: str, current_date_format: str, target_date_format: str) -> DataFrame:
    return DateFormatProcessor(field, current_date_format, target_date_format).process(df)


def format_date_with_predicate(df: DataFrame,
                               field: str,
                               current_date_format: str,
                               target_date_format: str,
                               predicate_key: str,
                               predicate_value: str) -> DataFrame:
    return DateFormatProcessorWithPredicate(PredicateProcessorParameters(predicate_key=predicate_key,
                                                                         predicate_value=predicate_value),
                                            field,
                                            current_date_format,
                                            target_date_format).process(df)
