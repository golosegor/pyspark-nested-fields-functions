from __future__ import annotations

import logging
from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType, AtomicType

from nestedfunctions.processors.terminal_operation_processor import TerminalOperationProcessor

log = logging.getLogger(__name__)


class TerminalOperationProcessorWithPredicate(TerminalOperationProcessor):
    def __init__(self, column_to_process: str, p: PredicateProcessorParameters):
        super().__init__(column_to_process=column_to_process)
        self.p = p

    def apply_terminal_operation_on_root_level(self, df: DataFrame, column_name: str) -> DataFrame:
        raise Exception("Root level transformation is not supported on predicated-based transformer")

    def apply_complex_transformation(self, current_column: Column, current_column_name: str, fieldType: AtomicType):
        return current_column \
            .withField(current_column_name,
                       F.when(current_column.getField(self.p.predicate_key) == self.p.predicate_value,
                              self.transform_primitive(current_column.getField(current_column_name),
                                                       fieldType)) \
                       .otherwise(F.lit(current_column.getField(current_column_name).cast(StringType()))))


@dataclass
class PredicateProcessorParameters:
    predicate_key: str
    predicate_value: str
