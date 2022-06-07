import logging

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.types import AtomicType

from nestedfunctions.processors.terminal_operation_processor import TerminalOperationProcessor

log = logging.getLogger(__name__)


class StringConcatProcessor(TerminalOperationProcessor):

    def __init__(self, column_to_process: str, value_to_add: str):
        super().__init__(column_to_process)
        self.value_to_add = value_to_add

    def transform_primitive(self, primitive_value: Column, fieldType: AtomicType) -> Column:
        return F.concat(primitive_value, F.lit(self.value_to_add))
