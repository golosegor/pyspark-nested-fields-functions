from typing import Callable

from pyspark.sql import DataFrame

from nestedfunctions.processors.field_rename.field_rename_processor import FieldRenameProcessor, RenameLambda, \
    FieldRenameFunc
from nestedfunctions.processors.field_rename.parquet.parquet_compliance_field_rename_pf import ParquetComplianceFn


def rename(df: DataFrame, rename_func: Callable[[str], str]) -> DataFrame:
    return FieldRenameProcessor(RenameLambda(rename_func)).process(df)


def rename_with_strategy(df: DataFrame, rename_func: FieldRenameFunc) -> DataFrame:
    return FieldRenameProcessor(rename_func).process(df)


def rename_to_parquet_compliant(df: DataFrame) -> DataFrame:
    return FieldRenameProcessor(ParquetComplianceFn()).process(df)
