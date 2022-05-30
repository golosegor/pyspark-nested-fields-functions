import logging
from typing import Callable, Optional, Tuple

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import StructType

from nestedfunctions.utils.schema.schema_util import SchemaUtility

log = logging.getLogger(__name__)


class RecFunc:
    def __init__(self,
                 field: str,
                 root_level_processor: Callable[[DataFrame, str], DataFrame],
                 structure_operation: Callable[[StructType, Column, str, str], Column]):
        self.column_to_process = field
        self.structure_operation = structure_operation
        self.root_level_processor = root_level_processor
        self.spark_utility = SchemaUtility()

    def apply_terminal_operation_on_root_level(self, df: DataFrame, column_name: str) -> DataFrame:
        return self.root_level_processor(df, column_name)

    def apply_terminal_operation_on_structure(self,
                                              schema: StructType,
                                              column: Column,
                                              column_name: str,
                                              previous: str) -> Column:
        return self.structure_operation(schema, column, column_name, previous)

    def process(self, df: DataFrame) -> DataFrame:
        if not self.spark_utility.is_column_exist(df.schema, self.column_to_process):
            log.error(f'Column `{self.column_to_process}` does not exist. This column will not be processed')
            return df
        (root, remaining) = self.__parse_head_tail(self.column_to_process)
        if remaining is None:
            return self.apply_terminal_operation_on_root_level(df, root)
        else:
            return df.withColumn(root,
                                 self.__process_field_recursive(schema=df.schema,
                                                                current_column_name=root,
                                                                next=remaining,
                                                                previous=root))

    def __process_field_with(self,
                             schema: StructType,
                             current_column_name: str,
                             next: str,
                             previous: str,
                             current_column: Column = None) -> Column:
        # terminal operation reached. Two use-cases. Primitive array -> normal field
        if next is None:
            return self.apply_terminal_operation_on_structure(schema, current_column, current_column_name, previous)
        return current_column.withField(f'`{current_column_name}`', self.__process_field_recursive(
            schema=schema,
            current_column_name=current_column_name,
            next=next,
            current_column=current_column,
            previous=previous
        ))

    def __process_field_recursive(self,
                                  schema: StructType,
                                  current_column_name: str,
                                  next: str,
                                  previous: str,
                                  current_column: Column = None) -> Column:
        (head, tail) = self.__parse_head_tail(next)
        is_array = self.spark_utility.is_array(schema, previous)
        aggregated_previous = previous + "." + head
        if is_array:
            # root level & non-root level column is different.
            # Root level have to use F.col,
            # where non-root level must use 'current_column.getField'
            column = F.col(current_column_name) \
                if self.__is_root_level_transformation(current_column) else current_column.getField(current_column_name)
            return F.transform(column, lambda d: self.__process_field_with(schema=schema,
                                                                           current_column_name=head,
                                                                           next=tail,
                                                                           current_column=d,
                                                                           previous=aggregated_previous))
        else:
            # non-array case. Calculating next column (based on 'root/non-root' level)
            col = self.__next_column(current_column, current_column_name)
            return self.__process_field_with(schema=schema,
                                             current_column_name=head,
                                             next=tail,
                                             current_column=col,
                                             previous=aggregated_previous)

    def __next_column(self, current_column, current_column_name):
        if self.__is_root_level_transformation(current_column):
            col = F.col(current_column_name)
        else:
            col = current_column.getField(current_column_name)
        return col

    def __is_root_level_transformation(self, column: Column) -> bool:
        return column is None

    def __parse_head_tail(self, param: str) -> Tuple[str, Optional[str]]:
        if "." in param:
            (head, tail) = param.split(".", maxsplit=1)
        else:
            (head, tail) = param, None
        head_tail = head, tail
        return head_tail
