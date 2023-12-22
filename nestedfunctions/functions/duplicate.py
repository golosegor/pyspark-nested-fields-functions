import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from nestedfunctions.processors.any_level_processor import AnyLevelCoreProcessor

def duplicate(df: DataFrame, column_to_duplicate: str, duplicated_column_name: str) -> DataFrame:
    return DuplicateProcessor(column_to_duplicate, duplicated_column_name).process(df)


class DuplicateProcessor(AnyLevelCoreProcessor):
    @staticmethod
    def __get_higher_and_lowest_level(column_name: str) -> (str, str):
        splitted_column_name = column_name.rsplit(".", maxsplit=1)
        if len(splitted_column_name) == 1:
            return None, splitted_column_name[0]
        else:
            return tuple(splitted_column_name)

    def __init__(self, column_to_duplicate: str, duplicated_column_name: str):
        (higher_levels_column_to_duplicate, _) = self.__get_higher_and_lowest_level(column_to_duplicate)
        (higher_levels_duplicated_column_name, lowest_level_duplicated_column_name) = self.__get_higher_and_lowest_level(duplicated_column_name)
        if higher_levels_column_to_duplicate != higher_levels_duplicated_column_name:
            raise ValueError(f"Columns `{column_to_duplicate}` and `{duplicated_column_name}` must be at the same level !")
        super().__init__(column_to_duplicate)
        self.new_field_name = lowest_level_duplicated_column_name

    def apply_terminal_operation_on_root_level(self, df: DataFrame, column_name: str) -> DataFrame:
        return df.withColumn(self.new_field_name, F.col(column_name))

    def apply_terminal_operation_on_structure(self, schema: StructType, column: Column, column_name: str,
                                              previous: str) -> Column:
        return column.withField(f"`{self.new_field_name}`", column.getField(column_name))
