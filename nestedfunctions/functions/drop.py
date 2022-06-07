import logging

from pyspark.sql import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

from nestedfunctions.processors.any_level_processor import AnyLevelCoreProcessor
from nestedfunctions.spark_schema.utility import SparkSchemaUtility

log = logging.getLogger(__name__)


def drop(df: DataFrame, field: str) -> DataFrame:
    return DropProcessor(field).process(df)


class DropProcessor(AnyLevelCoreProcessor):
    schema_util = SparkSchemaUtility()

    def apply_terminal_operation_on_structure(self,
                                              schema: StructType,
                                              column: Column,
                                              column_name: str,
                                              previous: str) -> Column:
        return column.dropFields(f"`{column_name}`")

    def apply_terminal_operation_on_root_level(self, df: DataFrame, column_name: str) -> DataFrame:
        return df.drop(column_name)

    def process(self, df: DataFrame) -> DataFrame:
        log.debug(f"Dropping {self.column_to_process}")
        if not self.schema_util.is_column_exist(df.schema, self.column_to_process):
            log.warning(f"Column {self.column_to_process} does not exist. Ignoring")
            return df
        if self.is_last_element_in_structure(df):
            parent_field = self.__find_parent_field()
            log.warning(
                f"{self.column_to_process} is the last element in structure.\n"
                f"Going to drop parent element to avoid spark exception: {parent_field}")
            return DropProcessor(parent_field).process(df)
        else:
            log.debug(f"Going to drop {self.column_to_process}")
            return super().process(df)

    def is_last_element_in_structure(self, df: DataFrame) -> bool:
        if "." not in self.column_to_process:
            return False
        parent_field = self.__find_parent_field()
        parent_field_schema = self.schema_util.schema_for_field(df.schema, parent_field)
        log.debug(
            f"Going to drop {self.column_to_process}. Parent field: {parent_field}. "
            f"Parent field schema: {parent_field_schema.names}")
        if len(parent_field_schema.names) == 1:
            return True
        else:
            return False

    def __find_parent_field(self):
        return self.schema_util.parent_element(self.column_to_process)
