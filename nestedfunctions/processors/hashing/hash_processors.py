from pyspark.sql import Column, functions as F
from pyspark.sql.types import StringType, AtomicType

from nestedfunctions.processors.terminal_operation_processor import TerminalOperationProcessor
from nestedfunctions.processors.terminal_operation_processor_with_predicate import \
    TerminalOperationProcessorWithPredicate, PredicateProcessorParameters


class HashProcessor(TerminalOperationProcessor):

    def __init__(self, column_to_process: str, bits: int = 256):
        super().__init__(column_to_process)
        self.bits = bits

    def transform_primitive(self, primitive_column: Column, fieldType: AtomicType) -> Column:
        return F.sha2(primitive_column.cast(StringType()), self.bits)


class HashProcessorWithPredicate(TerminalOperationProcessorWithPredicate):

    def __init__(self, column_to_process: str, p: PredicateProcessorParameters, bits: int = 256):
        super().__init__(column_to_process=column_to_process, p=p)
        self.bits = bits

    def transform_primitive(self, primitive_value: Column, fieldType: AtomicType) -> Column:
        return HashProcessor(self.column_to_process, bits=self.bits).transform_primitive(primitive_value, fieldType)
