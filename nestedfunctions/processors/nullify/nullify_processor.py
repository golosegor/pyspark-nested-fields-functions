from pyspark.sql import Column, functions as F
from pyspark.sql.types import AtomicType

from nestedfunctions.processors.terminal_operation_processor import TerminalOperationProcessor
from nestedfunctions.processors.terminal_operation_processor_with_predicate import \
    TerminalOperationProcessorWithPredicate, PredicateProcessorParameters


class NullabilityProcessor(TerminalOperationProcessor):

    def transform_primitive(self, primitive_column: Column, fieldType: AtomicType) -> Column:
        return F.lit(None)


class NullabilityProcessorWithPredicate(TerminalOperationProcessorWithPredicate):

    def __init__(self, field: str, p: PredicateProcessorParameters):
        super().__init__(field, p)

    def transform_primitive(self, primitive_value: Column, fieldType: AtomicType) -> Column:
        return NullabilityProcessor(self.column_to_process).transform_primitive(primitive_value,
                                                                                fieldType)
