import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StructType, AtomicType

from nestedfunctions.processors.any_level_processor import AnyLevelCoreProcessor
from nestedfunctions.spark_schema.utility import SparkSchemaUtility

log = logging.getLogger(__name__)


class TerminalOperationProcessor(AnyLevelCoreProcessor):

    def transform_primitive(self, primitive_value: Column, fieldType: AtomicType) -> Column:
        raise NotImplemented("Not implemented. Must be overridden")

    def apply_terminal_operation_on_root_level(self, df: DataFrame, column_name: str) -> DataFrame:
        field_type: AtomicType = SparkSchemaUtility.schema_for_field(df.schema, column_name)
        if SparkSchemaUtility.is_array(df.schema, column_name):
            return df.withColumn(column_name,
                                 F.transform(F.col(column_name), lambda d: self.transform_primitive(d, field_type)))
        else:
            return df.withColumn(column_name, self.transform_primitive(F.col(column_name), field_type))

    def apply_terminal_operation_on_structure(self,
                                              schema: StructType,
                                              column: Column,
                                              column_name: str,
                                              previous: str) -> Column:
        is_array = SparkSchemaUtility.is_array(schema, previous)
        field_type: AtomicType = SparkSchemaUtility.schema_for_field(schema, previous)
        if is_array:
            return column.withField(f"`{column_name}`",
                                    F.transform(previous, lambda d: self.transform_primitive(d,
                                                                                             field_type)))
        else:
            return self.apply_complex_transformation(column, column_name, field_type)

    def apply_complex_transformation(self, current_column: Column, current_column_name: str, fieldType: AtomicType):
        return current_column.withField(f"`{current_column_name}`",
                                        self.transform_primitive(current_column.getField(current_column_name),
                                                                 fieldType))
