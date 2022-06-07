import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.types import AtomicType

from nestedfunctions.processors.terminal_operation_processor import TerminalOperationProcessor


class TruncateProcessor(TerminalOperationProcessor):

    def __init__(self, column_to_process: str, character_size: int):
        super().__init__(column_to_process)
        self.character_size = character_size

    def transform_primitive(self, primitive_value: Column, fieldType: AtomicType) -> Column:
        if self.character_size < 0:
            # it is not possible to obtain 'size' of primitive values from the array, so this workaround is mandatory
            # the most easier way would be
            # F.substring(primitive_value, 0, F.length(primitive_value) - 2)
            # but this does not work because "column is not callable". I didn't find a way so this workaround is here
            max_size = 9999_9999
            return F.reverse(F.substring(F.reverse(primitive_value), -self.character_size + 1, max_size))
        else:
            return F.substring(primitive_value, 1, self.character_size)
