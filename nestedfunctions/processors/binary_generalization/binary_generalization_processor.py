import logging

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.types import BooleanType, AtomicType

from nestedfunctions.processors.terminal_operation_processor import TerminalOperationProcessor

log = logging.getLogger(__name__)


class BinaryGeneralizationProcessor(TerminalOperationProcessor):

    def __init__(self, field_to_transform: str):
        super().__init__(field_to_transform)

    def transform_primitive(self, primitive_column: Column, fieldType: AtomicType) -> Column:
        return (F.when(F.length(F.trim(primitive_column)) > 0, F.lit(True)).otherwise(F.lit(False))).cast(BooleanType())
