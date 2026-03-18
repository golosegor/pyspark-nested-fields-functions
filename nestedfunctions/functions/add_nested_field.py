from __future__ import annotations

import logging
from typing import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql.types import AtomicType

from nestedfunctions.processors.add_operation_processor import AddOperationProcessor

log = logging.getLogger(__name__)


class LambdaBasedAddOperation(AddOperationProcessor):
    def __init__(self, column_to_process: str, new_column_name: str, f: Callable[[Column], Column])-> None:
        super().__init__(column_to_process, new_column_name)
        self.f = f

    def transform_primitive(self, primitive_value: Column, fieldType: AtomicType) -> Column:
        return self.f(primitive_value)

def add_nested_field(df: DataFrame,
                             column_to_process: str,
                             new_column_name: str,
                             f: Callable[[Column], Column]) -> DataFrame:
    return LambdaBasedAddOperation(column_to_process, new_column_name, f).process(df)
