import logging
from typing import Callable

from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StructType

from nestedfunctions.generic.rec_func import RecFunc
from nestedfunctions.utils.schema.schema_util import SchemaUtility

log = logging.getLogger(__name__)


def drop(df: DataFrame, field: str) -> DataFrame:
    return DropRecFunction(field=field,
                           root_level_processor=lambda d, s: d.drop(s),
                           structure_operation=
                           lambda strcut, column, field, previous: column.dropFields(f"`{field}`")).process(df)


class DropRecFunction(RecFunc):
    schema_util = SchemaUtility()

    def __init__(self,
                 field: str,
                 root_level_processor: Callable[[DataFrame, str], DataFrame],
                 structure_operation: Callable[[StructType, Column, str, str], Column]):
        super().__init__(field, root_level_processor, structure_operation)

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
            return drop(df, parent_field)
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
