from typing import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql.types import AtomicType


def apply_terminal_operation(data_frame: DataFrame,
                             field: str,
                             operation: Callable[[Column, AtomicType], Column]) -> DataFrame:
    raise NotImplemented("Not implemented. Must be overridden")
