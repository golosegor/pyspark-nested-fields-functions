import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StructType, AtomicType

from nestedfunctions.processors.any_level_processor import AnyLevelCoreProcessor
from nestedfunctions.spark_schema.utility import SparkSchemaUtility

log = logging.getLogger(__name__)
class AddOperationProcessor(AnyLevelCoreProcessor):

    def __init__(self, column_to_process: str, new_column_name: str):
        schema_utility = SparkSchemaUtility()
        (parent_column_to_process, _) = schema_utility.parent_child_elements(column=column_to_process, raise_exception_if_no_parent=False)
        (parent_new_column, child_new_column) = schema_utility.parent_child_elements(column=new_column_name, raise_exception_if_no_parent=False)
        if parent_new_column != parent_column_to_process:
            raise ValueError(f"Columns `{column_to_process}` and `{new_column_name}` must be at the same level !")
        if not parent_new_column:
            raise ValueError(f"new_column_name is {new_column_name} but root level transformation is not supported on AddOperationProcessor")

        super().__init__(column_to_process)
        self.new_field_name = child_new_column

    def transform_primitive(self, primitive_value: Column, fieldType: AtomicType) -> Column:
        raise NotImplemented("Not implemented. Must be overridden")

    def apply_terminal_operation_on_root_level(self, df: DataFrame, column_name: str) -> DataFrame:
        raise Exception("Root level transformation is not supported on AddOperationProcessor")

    def apply_terminal_operation_on_structure(self,
                                              schema: StructType,
                                              column: Column,
                                              column_name: str,
                                              previous: str) -> Column:
        # Previous is the full path to column_to_process while column_name is the last part of the name of this field
        # column can be the column corresponding with the parent of column_to_process
        # but can also be a transform / lambda combination applied to this parent.

        # Why need fieldType ?
        #  => With fieldType just pass the schema for the terminal field to the primitive functions such that they can use them if necessary.
        is_array = SparkSchemaUtility.is_array(schema, previous)
        field_type: AtomicType = SparkSchemaUtility.schema_for_field(schema, previous)
        if is_array:
            return column.withField(f"`{self.new_field_name}`",
                                    F.transform(column.getField(column_name), lambda d: self.transform_primitive(d,
                                                                                             field_type)))
        else:
            return column.withField(f"`{self.new_field_name}`",
                                        self.transform_primitive(column.getField(column_name),
                                                                 field_type))
