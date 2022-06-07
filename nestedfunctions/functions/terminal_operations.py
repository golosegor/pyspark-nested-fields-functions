from __future__ import annotations

import logging
from typing import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql.types import AtomicType

from nestedfunctions.processors.terminal_operation_processor import TerminalOperationProcessor
from nestedfunctions.processors.terminal_operation_processor_with_predicate import \
    TerminalOperationProcessorWithPredicate, PredicateProcessorParameters

log = logging.getLogger(__name__)


def apply_terminal_operation(df: DataFrame,
                             field: str,
                             f: Callable[[Column, AtomicType], Column]) -> DataFrame:
    return LambdaBasedTerminalOperation(field, f).process(df)


def apply_terminal_operation_with_predicate(df: DataFrame,
                                            field: str,
                                            f: Callable[[Column, AtomicType], Column],
                                            predicate_key: str,
                                            predicate_value: str) -> DataFrame:
    return LambdaBasedTerminalOperationPredicate(field, f, predicate_key, predicate_value).process(df)


class LambdaBasedTerminalOperation(TerminalOperationProcessor):
    def __init__(self, column: str, f: Callable[[Column, AtomicType], Column]) -> None:
        super().__init__(column)
        self.f = f

    def transform_primitive(self, primitive_value: Column, fieldType: AtomicType) -> Column:
        return self.f(primitive_value, fieldType)


class LambdaBasedTerminalOperationPredicate(TerminalOperationProcessorWithPredicate):
    def __init__(self, column: str,
                 f: Callable[[Column, AtomicType], Column],
                 predicate_key: str,
                 predicate_value: str) -> None:
        super().__init__(column,
                         p=PredicateProcessorParameters(predicate_key=predicate_key,
                                                        predicate_value=predicate_value))
        self.f = f

    def transform_primitive(self, primitive_value: Column, fieldType: AtomicType) -> Column:
        return self.f(primitive_value, fieldType)
