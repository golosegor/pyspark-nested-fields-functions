import logging
from typing import List, Set, Union

from pyspark.sql.types import StructType, ArrayType, AtomicType

from nestedfunctions.spark_schema.schema_flattener import flatten_schema

log = logging.getLogger(__name__)


class SparkSchemaUtility:

    @staticmethod
    def flatten_schema_include_parents_fields(schema: StructType) -> Set[str]:
        """
        returns flattened representation of schema including parent fields names
        """
        return set(flatten_schema(schema=schema, include_parent_as_field=True))

    @staticmethod
    def flatten_schema(schema: StructType) -> List[str]:
        """
        returns flattened representation of schema including only fields which contain values
        (parent fields that holding structure are not presented in the result)
        """
        return flatten_schema(schema)

    @staticmethod
    def is_column_exist(schema: StructType, column: str) -> bool:
        columns_ordered = column.split('.')
        col = columns_ordered.pop(0)
        if not isinstance(schema, StructType):
            return False
        if col not in schema.names:
            return False
        if len(columns_ordered) == 0:
            return col in schema.names

        else:
            return SparkSchemaUtility.is_column_exist(SparkSchemaUtility.__get_schema_for_field(schema, col),
                                                      '.'.join(columns_ordered))

    @staticmethod
    def parent_element(column: str):
        separator = '.'
        if '.' not in column:
            raise Exception(f"No parent element in {column}")
        *parents, last = column.split(separator)
        return separator.join(parents)

    @staticmethod
    def __get_schema_for_field(schema: StructType, col: str):
        f = schema[col]
        dt = type(f.dataType)

        if dt is ArrayType:
            return f.dataType.elementType
        else:
            return f.dataType

    @staticmethod
    def is_array(schema: StructType, field: str) -> bool:
        log.debug(f"Checking is array for field: {field}")
        split = field.split(".")
        t = schema
        for sub_type in split:
            data_type = t[sub_type].dataType
            if type(data_type) == ArrayType:
                t = data_type.elementType
            else:
                t = data_type
        return type(data_type) == ArrayType

    @staticmethod
    def schema_for_field(schema: StructType, field: str) -> Union[StructType, AtomicType]:
        if not SparkSchemaUtility.is_column_exist(schema, field):
            raise Exception(f"Column `{field}` does not exist")
        return SparkSchemaUtility.__schema_for_field_rec(schema, field)

    @staticmethod
    def __schema_for_field_rec(schema: StructType, field: str) -> StructType:
        if field == "":
            return schema
        else:
            columns_ordered = field.split('.')
            col = columns_ordered.pop(0)
            return SparkSchemaUtility.__schema_for_field_rec(SparkSchemaUtility.__get_schema_for_field(schema, col),
                                                             '.'.join(columns_ordered))
