from pyspark.sql import Column, functions as F
from pyspark.sql.types import StringType, AtomicType

from nestedfunctions.processors.terminal_operation_processor import TerminalOperationProcessor


class HashWithSaltProcessor(TerminalOperationProcessor):

    def __init__(self, column_to_process: str, salt="wof:?_fTNy/6bshXV@xh", separator="¿", bits: int = 256):
        super().__init__(column_to_process)
        self.salt = salt
        self.separator = separator
        self.bits = bits

    def transform_primitive(self, primitive_column: Column, fieldType: AtomicType) -> Column:
        return F.sha2(F.concat_ws(self.separator, F.lit(self.salt), primitive_column.cast(StringType())), self.bits)
