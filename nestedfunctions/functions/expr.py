import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import StructType

from nestedfunctions.processors.any_level_processor import AnyLevelCoreProcessor
from nestedfunctions.spark_schema.utility import SparkSchemaUtility


def expr(df: DataFrame, field: str, expr: str) -> DataFrame:
    if not expr:
        raise ValueError("Expr could not be empty")
    sparkSchemaUtility = SparkSchemaUtility()
    new_field = None
    if not sparkSchemaUtility.does_column_exist(df.schema, field):
        new_field = field

        if '.' not in new_field:
            root_level_fields = [x.name for x in df.schema if type(x.dataType) != StructType]
            return ExprProcessor(root_level_fields[0], expr, new_field).process(df)

        # Get an existing nested field at the same level as the new nested field.
        new_field_parent, _ = sparkSchemaUtility.parent_child_elements(field, raise_exception_if_no_parent=False)
        if not sparkSchemaUtility.does_column_exist(df.schema, new_field_parent):
            raise ValueError(f"Field `{field}` does not exist and neither has an existing parent!")
        for flattened_field in sparkSchemaUtility.flatten_schema(df.schema):
            direct_child_of_new_field_parent = f"{new_field_parent}." in flattened_field and '.' not in flattened_field.replace(f"{new_field_parent}.","")
            if direct_child_of_new_field_parent:
                field = flattened_field
                break

    return ExprProcessor(field, expr, new_field).process(df)


class ExprProcessor(AnyLevelCoreProcessor):

    def __init__(self, column_to_process: str, param_expr: str, new_column_name: str = None):
        super().__init__(column_to_process)
        self.expr = param_expr

        schema_utility = SparkSchemaUtility()
        (parent_column_to_process, child_column_to_process) = schema_utility.parent_child_elements(column=column_to_process, raise_exception_if_no_parent=False)
        if new_column_name:
            (parent_new_column, child_new_column) = schema_utility.parent_child_elements(column=new_column_name, raise_exception_if_no_parent=False)
            if parent_column_to_process != parent_new_column:
                raise ValueError(f"Columns `{column_to_process}` and `{new_column_name}` must be at the same level !")
            self.column_name = child_new_column
        else:
            self.column_name = child_column_to_process

    def apply_terminal_operation_on_root_level(self, df: DataFrame, column_name: str) -> DataFrame:
        return df.withColumn(self.column_name, self.expression())

    def expression(self) -> Column:
        return F.expr(self.expr)

    def apply_terminal_operation_on_structure(self, schema: StructType, column: Column, column_name: str,
                                              previous: str) -> Column:
        return column.withField(self.column_name, self.expression())
