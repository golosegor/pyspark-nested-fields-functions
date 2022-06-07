import logging

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.types import StringType, AtomicType

from nestedfunctions.processors.terminal_operation_processor import TerminalOperationProcessor
from nestedfunctions.processors.terminal_operation_processor_with_predicate import \
    TerminalOperationProcessorWithPredicate, PredicateProcessorParameters

log = logging.getLogger(__name__)


class DateFormatProcessor(TerminalOperationProcessor):

    def __init__(self, field_to_transform: str, current_date_format: str, target_date_format: str):
        super().__init__(field_to_transform)
        self.current_date_format = current_date_format
        self.target_date_format = target_date_format

    def transform_primitive(self, primitive_column: Column, fieldType: AtomicType) -> Column:
        date = F.to_timestamp(primitive_column.cast(StringType()), self.current_date_format)
        return F.date_format(date, self.target_date_format).cast(StringType())


class DateFormatProcessorWithPredicate(TerminalOperationProcessorWithPredicate):

    def __init__(self,
                 p: PredicateProcessorParameters,
                 column_to_process: str,
                 current_date_format: str,
                 target_date_format: str):
        super().__init__(column_to_process=column_to_process, p=p)
        self.current_date_format = current_date_format
        self.target_date_format = target_date_format

    def transform_primitive(self, primitive_value: Column, fieldType: AtomicType) -> Column:
        return DateFormatProcessor(field_to_transform=self.column_to_process,
                                   current_date_format=self.current_date_format,
                                   target_date_format=self.target_date_format).transform_primitive(primitive_value,
                                                                                                   fieldType)
