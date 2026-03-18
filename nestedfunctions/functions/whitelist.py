import itertools
import logging
import os
from typing import List, Set

from pyspark.sql import DataFrame

from nestedfunctions.functions.drop import drop
from nestedfunctions.processors.coreprocessor import CoreProcessor
from nestedfunctions.spark_schema.utility import SparkSchemaUtility


def whitelist(df: DataFrame, fields: List[str]) -> DataFrame:
    return WhitelistProcessor(fields, ).process(df)


log = logging.getLogger(__name__)


class WhitelistProcessor(CoreProcessor):
    """Processor that selects only required fields.

      In case non-existing field provided -> this field is ignored::

      In case all children of common parent are provided -> parent field is selected

      """

    schema_util = SparkSchemaUtility()

    def __init__(self, whitelist_fields: List[str]):
        self.whitelist_fields = set(whitelist_fields)

    def process(self, df: DataFrame) -> DataFrame:
        if self.no_fields_to_select(df, self.whitelist_fields):
            log.warning(
                f"No fields to select {self.whitelist_fields}. No intersections with {self.schema_util.flatten_schema_include_parents_fields(df.schema)}. "
                f"Returning empty df.")
            return df.limit(0)
        fields_to_drop = (
            WhitelistProcessor
            .find_fields_to_drop(
                flattened_fields=set(SparkSchemaUtility().flatten_schema(df.schema)),
                whitelist_fields=self.whitelist_fields
            )
        )
        log.debug(f"Whitelisting {self.whitelist_fields} requires dropping {fields_to_drop}")
        return drop(df, fields_to_drop)

    @staticmethod
    def find_fields_to_drop(flattened_fields: Set[str], whitelist_fields: Set[str]) -> Set[str]:
        # If the parent is in the whitelist and some of the children are also in the whitelist, only the parent is kept.
        whitelist_fields = filter_only_parents_fields(whitelist_fields)
        fields_to_drop = [
            field for field in flattened_fields if not
            WhitelistProcessor.is_field_or_ancestor_in_whitelist_fields(field=field, whitelist_fields=whitelist_fields)
        ]
        return fields_to_drop

    @staticmethod
    def is_field_or_ancestor_in_whitelist_fields(field: str, whitelist_fields: Set[str]) -> bool:
        if field in whitelist_fields:
            return True
        for parent_field in whitelist_fields:
            if field.startswith(f'{parent_field}.'):
                return True
        return False

    @classmethod
    def no_fields_to_select(cls, df: DataFrame, whitelist_fields: Set[str]) -> bool:
        flattened_fields = set(cls.schema_util.flatten_schema_include_parents_fields(df.schema))
        if (flattened_fields - whitelist_fields) == flattened_fields:
            return True
        else:
            return False

def filter_only_parents_fields(fields: Set[str]) -> Set[str]:
    combinations = itertools.combinations(fields, 2)
    sep = "."
    common_elements = {os.path.commonprefix([c1, c2]) for (c1, c2) in combinations
                       if os.path.commonprefix([c1, c2]) != ""
                       and (c2.startswith(f"{c1}{sep}") or c1.startswith(f"{c2}{sep}"))
                       }
    root_elements = {f for f in fields if f in common_elements}
    if len(root_elements) != 0:
        logging.warning(f"Root elements found {root_elements}. Be careful!! Child elements will be ignored!")
    return __replace_elements_with_roots(fields, root_elements)


def __replace_elements_with_roots(fields: Set[str], roots: Set[str]) -> Set[str]:
    res = fields.copy()
    for root in roots:
        res = __replace_elements_with_root(res, root)
    return res


def __replace_elements_with_root(fields: Set[str], root: str) -> Set[str]:
    res = set()
    root_added = False
    for f in fields:
        if root in f:
            if not root_added:
                root_added = True
                res.add(root)
        else:
            res.add(f)
    return res
