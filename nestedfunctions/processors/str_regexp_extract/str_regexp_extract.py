import logging

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.types import AtomicType

from nestedfunctions.processors.terminal_operation_processor import TerminalOperationProcessor

log = logging.getLogger(__name__)


class StringRegexpExtractProcessor(TerminalOperationProcessor):

    def __init__(self, column_to_process: str, pattern: str, group_index_to_extract: int):
        super().__init__(column_to_process)
        self.pattern = pattern
        self.group_index_to_extract = group_index_to_extract

    def transform_primitive(self, primitive_value: Column, fieldType: AtomicType) -> Column:
        return F.regexp_extract(primitive_value, self.pattern, self.group_index_to_extract)
