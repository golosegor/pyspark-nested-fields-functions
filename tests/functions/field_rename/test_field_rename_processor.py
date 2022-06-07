import logging

import pkg_resources

from nestedfunctions.functions.field_rename import rename_with_strategy, FieldRenameFunc
from nestedfunctions.spark_schema.utility import SparkSchemaUtility
from tests.functions.spark_base_test import SparkBaseTest
from tests.utils.testing_utils import parse_df_sample

log = logging.getLogger(__name__)


class FieldRenameProcessorTest(SparkBaseTest):

    def test_field_name_could_be_renamed(self):
        utility = SparkSchemaUtility()
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/field_with_space.json"))
        original_field_names = {
            'root field name with space',
            'root field name with space.id',
            'root field name with space.custom dimensions with space',
            'root field name with space.custom dimensions with space.nested value in the array with space',
        }
        self.assertEqual(original_field_names, utility.flatten_schema_include_parents_fields(df.schema))
        field_rename_strategy = AppendNameFn()
        processed = rename_with_strategy(df, field_rename_strategy)
        self.assertEqual({
            'root field name with space-@',
            'root field name with space-@.id-@',
            'root field name with space-@.custom dimensions with space-@.nested value in the array with space-@',
            'root field name with space-@.custom dimensions with space-@'
        }, utility.flatten_schema_include_parents_fields(processed.schema))

    class UppercaseFieldNameP(FieldRenameFunc):

        def convert_field_name(self, old_field_name: str) -> str:
            return old_field_name.upper()


class AppendNameFn(FieldRenameFunc):

    def convert_field_name(self, old_field_name: str) -> str:
        return f"{old_field_name}-@"
