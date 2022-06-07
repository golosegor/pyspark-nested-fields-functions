from __future__ import annotations

import datetime
import logging
import sys
from typing import Any, Dict

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql.types import AtomicType, LongType, StringType, BooleanType, DoubleType, FloatType, IntegerType, \
    ShortType, BinaryType, DateType, TimestampType, DecimalType, ByteType

from nestedfunctions.processors.terminal_operation_processor import TerminalOperationProcessor
from nestedfunctions.spark_schema.utility import SparkSchemaUtility


def redact(df: DataFrame, field: str) -> DataFrame:
    return RedactProcessor(field).process(df)


log = logging.getLogger(__name__)

MIN_SHORT_VALUE = -32768
MIN_INTEGER_VALUE = -2147483648
MIN_BYTE_VALUE = -128
DEFAULT_DECIMAL_TYPE = 0

SPARK_TYPE_TO_REDACT_VALUE: Dict[AtomicType, Any] = {
    StringType(): "_REDACTED_VALUE",
    LongType(): -sys.maxsize,
    BooleanType(): False,
    DoubleType(): float(-sys.maxsize),
    FloatType(): float(-sys.maxsize),
    IntegerType(): MIN_INTEGER_VALUE,
    ShortType(): MIN_SHORT_VALUE,
    BinaryType(): bytearray(),
    DateType(): datetime.date(year=1970, month=1, day=1),
    TimestampType(): datetime.datetime(year=1970, month=1, day=1, hour=0, minute=0, second=0, microsecond=0),
    ByteType(): MIN_BYTE_VALUE,
    DecimalType(): 0,
}


def column_name_with_dedicated_field_type(fieldType: AtomicType) -> Column:
    """
    default values are set according to https://spark.apache.org/docs/latest/sql-ref-datatypes.html
    """
    value_to_set = SPARK_TYPE_TO_REDACT_VALUE[fieldType]
    return F.lit(value_to_set).cast(fieldType)


class RedactProcessor(TerminalOperationProcessor):
    def __init__(self, column_to_process: str):
        super().__init__(column_to_process)

    def process(self, df: DataFrame) -> DataFrame:
        utility = SparkSchemaUtility()
        if not utility.is_column_exist(df.schema, self.column_to_process):
            log.warning(f"Column {self.column_to_process} does not exist. Ignoring redacting process")
            return df
        field_type = utility.schema_for_field(df.schema, self.column_to_process)
        if not issubclass(type(field_type), AtomicType):
            raise Exception(
                f"Only primitive types could be redacted. Column ${self.column_to_process} has ${field_type} type. "
                f"Expected primitive type")
        return super().process(df)

    def transform_primitive(self, primitive_value: Column, fieldType: AtomicType) -> Column:
        try:
            return column_name_with_dedicated_field_type(fieldType)
        except KeyError:
            raise Exception(
                f'Unknown type {fieldType.simpleString()} for field {self.column_to_process}. '
                f'Known types: {SPARK_TYPE_TO_REDACT_VALUE.keys()}')
