from __future__ import annotations

import logging
from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import AtomicType, StructType, StringType

from nestedfunctions.generic.rec_func import RecFunc
from nestedfunctions.utils.schema.schema_util import SchemaUtility

log = logging.getLogger(__name__)


def apply_terminal_operation(df: DataFrame,
                             field: str,
                             f: Callable[[Column, AtomicType], Column]) -> DataFrame:
    return __apply_terminal_operation_with_strategy(df, field, TerminalOperation(f=f))


def apply_terminal_operation_with_predicate(df: DataFrame,
                                            field: str,
                                            f: Callable[[Column, AtomicType], Column],
                                            predicate_key: str,
                                            predicate_value: str) -> DataFrame:
    return __apply_terminal_operation_with_strategy(df=df,
                                                    field=field,
                                                    strategy=TerminalOperationWithPredicate(f=f,
                                                                                            predicate_key=predicate_key,
                                                                                            predicate_value=predicate_value))


def __apply_terminal_operation_with_strategy(df: DataFrame,
                                             field: str,
                                             strategy: TerminalOperation) -> DataFrame:
    return RecFunc(field=field,
                   root_level_processor=
                   lambda dff, column: strategy.apply_terminal_operation_on_root_level(df=dff, column_name=column),
                   structure_operation=
                   lambda struct_type, column, field, next: strategy.apply_terminal_operation_on_structure(
                       schema=struct_type,
                       column=column,
                       column_name=field,
                       previous=next)) \
        .process(df)


class TerminalOperation:

    def __init__(self, f: Callable[[Column, AtomicType], Column]) -> None:
        super().__init__()
        self.f = f

    def apply_terminal_operation_on_structure(self,
                                              schema: StructType,
                                              column: Column,
                                              column_name: str,
                                              previous: str) -> Column:
        is_array = SchemaUtility.is_array(schema, previous)
        field_type: AtomicType = SchemaUtility.schema_for_field(schema, previous)
        if is_array:
            return column.withField(f"`{column_name}`",
                                    F.transform(previous, lambda d: self.f(d, field_type)))
        else:
            return self.apply_complex_transformation(column, column_name, field_type)

    def apply_complex_transformation(self, current_column: Column, current_column_name: str, fieldType: AtomicType):
        return current_column.withField(f"`{current_column_name}`",
                                        self.f(current_column.getField(current_column_name), fieldType))

    def apply_terminal_operation_on_root_level(self, df: DataFrame,
                                               column_name: str) -> DataFrame:
        field_type: AtomicType = SchemaUtility.schema_for_field(df.schema, column_name)
        if SchemaUtility.is_array(df.schema, column_name):
            return df.withColumn(column_name,
                                 F.transform(F.col(column_name), lambda d: self.f(d, field_type)))
        else:
            transformed_column = self.f(F.col(column_name), field_type)
            return df.withColumn(column_name, transformed_column)


class TerminalOperationWithPredicate(TerminalOperation):

    def __init__(self,
                 f: Callable[[Column, AtomicType], Column],
                 predicate_key: str,
                 predicate_value: str) -> None:
        super().__init__(f)
        self.predicate_key = predicate_key
        self.predicate_value = predicate_value

    def apply_terminal_operation_on_root_level(self, df: DataFrame, column_name: str) -> DataFrame:
        raise Exception("Root level transformation is not supported on predicated-based transformer")

    def apply_complex_transformation(self, current_column: Column, current_column_name: str, fieldType: AtomicType):
        return current_column \
            .withField(current_column_name,
                       F.when(current_column.getField(self.predicate_key) == self.predicate_value,
                              self.f(current_column.getField(current_column_name), fieldType)) \
                       .otherwise(F.lit(current_column.getField(current_column_name).cast(StringType()))))
