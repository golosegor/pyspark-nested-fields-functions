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

# https://issues.apache.org/jira/browse/SPARK-28090
# Spark hangs when an execution plan has many projections on nested structs
NUMBER_OF_ITERATIONS_TO_FORCE_CATALYST_FLUSH = 10
SPARK_ENABLED_FORCE_RECALCULATION_ENV_VARIABLE_NAME = "SPARK_FORCE_RECALCULATION_ENABLED"


class WhitelistProcessor(CoreProcessor):
    """Processor that selects only required fields.

      In case non-existing field provided -> this field is ignored::

      In case multiple fields with common root is provided -> parent field is selected

      """

    def __init__(self, whitelist_fields: List[str]):
        self.whitelist_fields = set(whitelist_fields)
        self.schema_util = SparkSchemaUtility()

    def process(self, df: DataFrame) -> DataFrame:
        if self.no_fields_to_select(df):
            log.warning(
                f"No fields to select {self.whitelist_fields}. No intersections with {self.schema_util.flatten_schema(df.schema)}. "
                f"Returning empty df.")
            return df.limit(0)
        fields_to_drop = WhitelistProcessor.find_fields_to_drop(
            flattened_fields=set(SparkSchemaUtility().flatten_schema(df.schema)),
            whitelist_fields=self.whitelist_fields)
        log.debug(f"Fields to drop {fields_to_drop}")
        return self.drop_fields(df, fields_to_drop)

    @staticmethod
    def find_fields_to_drop(flattened_fields: Set[str], whitelist_fields: Set[str]) -> Set[str]:
        # If the parent is in the whitelist and some of the children are also in the whitelist, only the parent is kept.
        whitelist_fields = filter_only_parents_fields(whitelist_fields)
        fields_to_drop = {field for field in flattened_fields if
                          WhitelistProcessor.does_field_need_to_be_dropped(field=field,
                                                                         whitelist_fields=whitelist_fields)}

        # If every child of a parent needs to be dropped all these children will be in fields_to_drop.
        # I.s.o dropping these children 1 by 1 the parent can just be dropped !
        # By sorting the parents make sure to start from the root and drop the parent as early as possible.
        parents_of_fields_to_drop = set()
        for field in fields_to_drop:
            parents_of_fields_to_drop.update(SparkSchemaUtility.parents_for_field(field))
        parents_of_fields_to_drop = sorted(parents_of_fields_to_drop)

        index = 0
        while index < len(parents_of_fields_to_drop):
            parent_field = parents_of_fields_to_drop[index]
            child_fields_to_whitelist = [x for x in whitelist_fields if x.startswith(f"{parent_field}.")]
            if not child_fields_to_whitelist:
                # Remove all the children of parent_field from fields_to_drop and add parent_field
                fields_to_drop.add(parent_field)
                fields_to_drop = {x for x in fields_to_drop if not x.startswith(f"{parent_field}.")}

                # Remove all the children of parent_field from parents_of_fields_to_drop to avoid unnecessary iterations
                parents_of_fields_to_drop = [x for x in parents_of_fields_to_drop if not x.startswith(f"{parent_field}.")]
            index += 1
        return fields_to_drop

    @staticmethod
    def drop_fields(df: DataFrame, fields_to_drop: Set[str]) -> DataFrame:
        for idx, field_to_drop in enumerate(fields_to_drop):
            df = drop(df, field_to_drop)
            if (idx + 1) % NUMBER_OF_ITERATIONS_TO_FORCE_CATALYST_FLUSH == 0:
                log.debug("Dropping: time to force calculation")
                df = force_calculation(df)
        return df

    @staticmethod
    def does_field_need_to_be_dropped(field: str, whitelist_fields: Set[str]) -> bool:
        if field in whitelist_fields:
            return False
        for parent_field in whitelist_fields:
            if field.startswith(f'{parent_field}.'):
                return False
        return True

    def no_fields_to_select(self, df: DataFrame) -> bool:
        flattened_fields = set(self.schema_util.flatten_schema(df.schema))
        if (flattened_fields - self.whitelist_fields) == flattened_fields:
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


def force_calculation(df: DataFrame) -> DataFrame:
    spark_force_calculation_enabled = os.environ.get(SPARK_ENABLED_FORCE_RECALCULATION_ENV_VARIABLE_NAME)
    if spark_force_calculation_enabled is not None and spark_force_calculation_enabled.lower().strip() == "true":
        return df.cache()
    else:
        return df
