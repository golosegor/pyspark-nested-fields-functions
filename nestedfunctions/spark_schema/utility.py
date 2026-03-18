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
    def does_column_exist(schema: StructType, column: str) -> bool:
        columns_ordered = column.split('.')
        col = columns_ordered.pop(0)
        if not isinstance(schema, StructType):
            return False
        if col not in schema.names:
            return False
        if len(columns_ordered) == 0:
            return col in schema.names

        else:
            return SparkSchemaUtility.does_column_exist(SparkSchemaUtility.__get_schema_for_field(schema, col),
                                                      '.'.join(columns_ordered))

    @staticmethod
    def parents_for_field(field: str) -> Set[str]:
        separator = '.'
        *parents, _ = field.split(separator)

        all_parents, previous_parent = [] , ''
        for index, parent in enumerate(parents):
            all_parents.append(f"{previous_parent}{parent}")
            previous_parent = f"{all_parents[index]}{separator}"
        return set(all_parents)

    @staticmethod
    def parent_child_elements(column: str, raise_exception_if_no_parent: bool = True):
        separator = '.'
        if '.' not in column and raise_exception_if_no_parent:
            raise Exception(f"No parent element in {column}")
        *parents, last = column.split(separator)
        return separator.join(parents), last

    @staticmethod
    def __get_schema_for_field(schema: StructType, col: str):
        f = schema[col]
        dt = type(f.dataType)

        if dt is ArrayType:
            return f.dataType.elementType
        else:
            return f.dataType

    @staticmethod
    def __get_type_for_field(schema: StructType, field: str) -> AtomicType:
        split = field.split(".")
        t = schema
        for sub_type in split:
            data_type = t[sub_type].dataType
            if type(data_type) == ArrayType:
                t = data_type.elementType
            else:
                t = data_type
        return type(data_type)

    @staticmethod
    def is_array(schema: StructType, field: str) -> bool:
        log.debug(f"Checking is array for field: {field}")
        spark_sql_type = SparkSchemaUtility.__get_type_for_field(schema, field)
        return spark_sql_type == ArrayType

    @staticmethod
    def is_struct(schema: StructType, field: str) -> bool:
        log.debug(f"Checking is struct for field: {field}")
        spark_sql_type = SparkSchemaUtility.__get_type_for_field(schema, field)
        return spark_sql_type == StructType

    @staticmethod
    def schema_for_field(schema: StructType, field: str) -> Union[StructType, AtomicType]:
        if not SparkSchemaUtility.does_column_exist(schema, field):
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
