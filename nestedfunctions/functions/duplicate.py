import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from nestedfunctions.processors.any_level_processor import AnyLevelCoreProcessor
from nestedfunctions.spark_schema.utility import SparkSchemaUtility

def duplicate(df: DataFrame, column_to_duplicate: str, duplicated_column_name: str) -> DataFrame:
    return DuplicateProcessor(column_to_duplicate, duplicated_column_name).process(df)


class DuplicateProcessor(AnyLevelCoreProcessor):

    def __init__(self, column_to_duplicate: str, duplicated_column_name: str):
        schema_utility = SparkSchemaUtility()
        (parent_column_to_duplicate, _) = schema_utility.parent_child_elements(column=column_to_duplicate, raise_exception_if_no_parent=False)
        (parent_duplicated_column, child_duplicated_column) = schema_utility.parent_child_elements(column=duplicated_column_name, raise_exception_if_no_parent=False)
        if parent_column_to_duplicate != parent_duplicated_column:
            raise ValueError(f"Columns `{column_to_duplicate}` and `{duplicated_column_name}` must be at the same level !")
        super().__init__(column_to_duplicate)
        self.new_field_name = child_duplicated_column

    def apply_terminal_operation_on_root_level(self, df: DataFrame, column_name: str) -> DataFrame:
        return df.withColumn(self.new_field_name, F.col(column_name))

    def apply_terminal_operation_on_structure(self, schema: StructType, column: Column, column_name: str,
                                              previous: str) -> Column:
        return column.withField(f"`{self.new_field_name}`", column.getField(column_name))
