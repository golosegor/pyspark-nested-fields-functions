import logging

from typing import List, Set, Union

from pyspark.sql import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

from nestedfunctions.processors.any_level_processor import AnyLevelCoreProcessor
from nestedfunctions.spark_schema.utility import SparkSchemaUtility

log = logging.getLogger(__name__)

def drop(df: DataFrame, fields_to_drop: Union[str, List[str]]) -> DataFrame:
    if isinstance(fields_to_drop, str):
        fields_to_drop = [fields_to_drop]
    return DropProcessor(fields_to_drop).process(df)

class DropProcessor(AnyLevelCoreProcessor):
    schema_util = SparkSchemaUtility()

    def __init__(self, fields_to_drop: List[str]):
        self.fields_to_drop = fields_to_drop
    
    @staticmethod
    def is_field_or_ancestor_in_fields_to_drop(field: str, fields_to_drop: Set[str]) -> bool:
        if field in fields_to_drop:
            return True
        for parent_field in fields_to_drop:
            if field.startswith(f'{parent_field}.'):
                return True
        return False

    @classmethod
    def consolidate_fields_to_drop(cls, dataframe_schema: StructType, fields_to_drop: List[str]) -> List[str]:
        fields_to_drop = set(fields_to_drop)
        non_existent_fields = {field for field in fields_to_drop if not cls.schema_util.does_column_exist(dataframe_schema, field)}
        if non_existent_fields:
            log.warning(f"Column(s) {non_existent_fields} don't exist. Ignoring")
        fields_to_drop -= non_existent_fields

        flattened_fields = cls.schema_util.flatten_schema(dataframe_schema)
        fields_to_keep = {x for x in flattened_fields if not cls.is_field_or_ancestor_in_fields_to_drop(x, fields_to_drop)}

        # If every child of a parent needs to be dropped all these children will be in fields_to_drop.
        # I.s.o dropping these children 1 by 1 the parent can just be dropped !
        # By sorting the parents make sure to start from the root and drop the parent as early as possible.
        parents_of_fields_to_drop = {parent for field in fields_to_drop for parent in cls.schema_util.parents_for_field(field)}
        parents_of_fields_to_drop = sorted(parents_of_fields_to_drop)

        index = 0
        while index < len(parents_of_fields_to_drop):
            parent_field = parents_of_fields_to_drop[index]
            child_fields_to_keep = {x for x in fields_to_keep if x.startswith(f"{parent_field}.")}
            if not child_fields_to_keep: 
                # Remove all the children of parent_field from fields_to_drop and add parent_field
                fields_to_drop.add(parent_field)
                fields_to_drop -= {x for x in fields_to_drop if x.startswith(f"{parent_field}.")}

                # Remove all the children of parent_field from parents_of_fields_to_drop to avoid unnecessary iterations.
                # Removing these children in combination with increasing the index is safe as parents_of_fields_to_drop is sorted.
                parents_of_fields_to_drop = [x for x in parents_of_fields_to_drop if not x.startswith(f"{parent_field}.")]
            index += 1
        return list(fields_to_drop)

    def process(self, df: DataFrame) -> DataFrame:
        log.debug(f"Got {self.fields_to_drop} as fields_to_drop")
        self.fields_to_drop = self.consolidate_fields_to_drop(df.schema, self.fields_to_drop)
        log.debug(f"Consolidated these fields_to_drop to {self.fields_to_drop}")
        
        # Sort the fields_to_drop by decreasing depth.
        self.fields_to_drop.sort(key=lambda x: x.count('.'), reverse=True)

        # Group fields_to_drop by shared direct parent.
        # The direct parents will be sorted by decreasing depth as fields_to_drop is sorted by decreasing depth.
        direct_parent_of_fields_to_drop = {}
        for field_to_drop in self.fields_to_drop:
            direct_parent, field = self.schema_util.parent_child_elements(field_to_drop, raise_exception_if_no_parent=False)
            direct_parent = direct_parent if direct_parent else "ROOT"
            fields_for_direct_parent = direct_parent_of_fields_to_drop.get(direct_parent,[])
            fields_for_direct_parent.append(field)
            direct_parent_of_fields_to_drop[direct_parent] = fields_for_direct_parent
        self.direct_parent_of_fields_to_drop = direct_parent_of_fields_to_drop

        # Will start with dropping field for the deepest parent.
        # While walking the tree for the deepest fields_to_drop will already drop the fields that share any ancestor.
        while self.direct_parent_of_fields_to_drop:
            deepest_remaining_parent, children_of_deepest_remaining_parent = list(self.direct_parent_of_fields_to_drop.items())[0]
            first_child_of_deepest_remaining_parent = children_of_deepest_remaining_parent[0]
            if deepest_remaining_parent == "ROOT":
                column_to_process = first_child_of_deepest_remaining_parent
            else:
                column_to_process = f"{deepest_remaining_parent}.{first_child_of_deepest_remaining_parent}"
            super().__init__(column_to_process)
            df = super().process(df)
            # TODO: Need to force calculation after x amount of drops ???
        return df

    def _process_field_with(self,
                             schema: StructType,
                             current_column_name: str,
                             next: str,
                             previous: str,
                             current_column: Column = None) -> Column:
        # terminal operation reached. Two use-cases. Primitive array -> normal field
        if next is None:
            return self.apply_terminal_operation_on_structure(schema, current_column, current_column_name, previous)

        # Already drop what can be dropped while walking the tree !
        col_name, _ = self.schema_util.parent_child_elements(previous)
        fields_to_drop_for_current_column = self.direct_parent_of_fields_to_drop.get(col_name)
        if fields_to_drop_for_current_column:
            current_column = current_column.dropFields(*fields_to_drop_for_current_column)
            # Do the cleanup
            self.fields_to_drop = [x for x in self.fields_to_drop if x not in fields_to_drop_for_current_column]
            del self.direct_parent_of_fields_to_drop[col_name]

        return current_column.withField(f'`{current_column_name}`', self._process_field_recursive(
            schema=schema,
            current_column_name=current_column_name,
            next=next,
            current_column=current_column,
            previous=previous)
        )

    def apply_terminal_operation_on_structure(self,
                                              schema: StructType,
                                              column: Column,
                                              column_name: str,
                                              previous: str) -> Column:
        # Previous is the full path to the field we want to fill while column_name is the last part of the name of this field
        # column can be the column corresponding with the parent of the field to fill
        # but can also be a transform / lambda combination applied to this parent.

        col_name, _ = self.schema_util.parent_child_elements(previous)
        column = column.dropFields(*self.direct_parent_of_fields_to_drop[col_name])
        # Do the cleanup
        self.fields_to_drop = [x for x in self.fields_to_drop if x not in self.direct_parent_of_fields_to_drop[col_name]]
        del self.direct_parent_of_fields_to_drop[col_name]
        return column

    def apply_terminal_operation_on_root_level(self, df: DataFrame, column_name: str) -> DataFrame:
        df = df.drop(*self.direct_parent_of_fields_to_drop["ROOT"])
        # Do the cleanup
        self.fields_to_drop = [x for x in self.fields_to_drop if x not in self.direct_parent_of_fields_to_drop["ROOT"]]
        del self.direct_parent_of_fields_to_drop["ROOT"]
        return df
