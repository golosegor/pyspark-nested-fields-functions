import logging
from typing import List

from pyspark.sql.types import StructType, StructField, ArrayType

log = logging.getLogger(__name__)


def flatten_schema(schema: StructType,
                   parent: str = None,
                   separator: str = ".",
                   include_parent_as_field: bool = False) -> List[str]:
    """
    accepts schema, returns all nested fields using column separator
    """

    res: List[str] = []
    fields = schema.fields
    log.debug(f"Parent {parent}, fields: {fields}")
    for field in fields:
        flattened_schema = __find_string_for_schema(parent, field, separator, include_parent_as_field)
        if include_parent_as_field and parent is not None:
            res.append(parent)
        res = res + flattened_schema
    return res


def __find_string_for_schema(parent: str, field: StructField, separator: str, include_parent: bool) -> List[str]:
    if isinstance(field.dataType, StructType):
        concated = __concat_with_parent(parent=parent, field=field.name, separator=separator)
        return flatten_schema(schema=field.dataType,
                              parent=concated,
                              include_parent_as_field=include_parent)
    elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
        concated = __concat_with_parent(parent=parent,
                                        field=field.name,
                                        separator=separator)
        return flatten_schema(schema=field.dataType.elementType,
                              parent=concated,
                              include_parent_as_field=include_parent)
    else:
        return [__concat_with_parent(parent=parent, field=field.name, separator=separator)]


def __concat_with_parent(parent: str, field: str, separator: str) -> str:
    if parent is None:
        return field
    else:
        return f'{parent}{separator}{field}'
