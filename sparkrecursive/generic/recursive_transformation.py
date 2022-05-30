from typing import Callable

from pyspark.pandas import DataFrame
from pyspark.sql import Column
from pyspark.sql.types import AtomicType


def apply_terminal_operation(data_frame: DataFrame,
                             field: str,
                             operation: Callable[[Column, AtomicType], Column]) -> DataFrame:
    raise NotImplemented("Not implemented. Must be overridden")
