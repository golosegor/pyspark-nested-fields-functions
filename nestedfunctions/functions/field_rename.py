from __future__ import annotations

from copy import copy
from typing import Callable

from pyspark.sql import *
from pyspark.sql.types import StructField, DataType, StructType, ArrayType

from nestedfunctions.processors.coreprocessor import CoreProcessor


def rename(df: DataFrame, rename_func: Callable[[str], str]) -> DataFrame:
    return FieldRenameProcessor(RenameLambda(rename_func)).process(df)


def rename_with_strategy(df: DataFrame, rename_func: FieldRenameFunc) -> DataFrame:
    return FieldRenameProcessor(rename_func).process(df)


def rename_to_parquet_compliant(df: DataFrame) -> DataFrame:
    return FieldRenameProcessor(ParquetComplianceFn()).process(df)


class FieldRenameProcessor(CoreProcessor):

    def __init__(self, rename_strategy: FieldRenameFunc):
        self.rename_func = rename_strategy
        super().__init__()

    def process(self, df: DataFrame) -> DataFrame:
        schema_renamer = SchemaRenamer()
        updated_schema = schema_renamer.rename_fields(df.schema, self.rename_func)
        return df.rdd.toDF(updated_schema)


class FieldRenameFunc:
    def convert_field_name(self, old_field_name: str) -> str:
        pass


class RenameLambda(FieldRenameFunc):

    def __init__(self, rename_func: Callable[[str], str]) -> None:
        super().__init__()
        self.rename_func = rename_func

    def convert_field_name(self, old_field_name: str) -> str:
        return self.rename_func(old_field_name)


class SchemaRenamer:

    def rename_fields(self, dataTypeArg: DataType, field_rename_func: FieldRenameFunc) -> StructType:
        data_type = copy(dataTypeArg)
        if isinstance(data_type, StructType):
            data_type.fields = [self.__update_field_name(f, field_rename_func) for f in data_type.fields]
        elif isinstance(data_type, ArrayType):
            data_type.elementType = self.rename_fields(data_type.elementType, field_rename_func)
        return data_type

    def __update_field_name(self, field: StructField, rename_func: FieldRenameFunc) -> StructField:
        field = copy(field)
        field.name = rename_func.convert_field_name(field.name)
        field.dataType = self.rename_fields(field.dataType, rename_func)
        return field


class ParquetComplianceFn(FieldRenameFunc):

    def convert_field_name(self, old_field_name: str) -> str:
        res = old_field_name
        problematic_chars = ',;{}()='
        for c in problematic_chars:
            res = res.replace(c, '')
        return res.replace(" ", "_")
