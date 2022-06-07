import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import StructType

from nestedfunctions.processors.any_level_processor import AnyLevelCoreProcessor


def expr(df: DataFrame, field: str, expr: str) -> DataFrame:
    if not expr:
        raise ValueError("Expr could not be empty")
    return ExprProcessor(field, expr).process(df)


class ExprProcessor(AnyLevelCoreProcessor):

    def __init__(self, column_to_process: str, param_expr: str):
        super().__init__(column_to_process)
        self.expr = param_expr

    def apply_terminal_operation_on_root_level(self, df: DataFrame, column_name: str) -> DataFrame:
        return df.withColumn(column_name, self.expression())

    def expression(self) -> Column:
        return F.expr(self.expr)

    def apply_terminal_operation_on_structure(self, schema: StructType, column: Column, column_name: str,
                                              previous: str) -> Column:
        return column.withField(column_name, self.expression())
