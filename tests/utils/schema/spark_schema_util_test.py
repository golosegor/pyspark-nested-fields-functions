import logging

import pkg_resources
import pytest

from nestedfunctions.utils.schema.schema_util import SchemaUtility
from tests.utils.spark_base_test import SparkBaseTest, parse_df_sample

log = logging.getLogger(__name__)

logging.getLogger('metadata_core.utils.spark.schema.schema_flattener').setLevel(logging.DEBUG)


class SparkSchemaUtilityTest(SparkBaseTest):
    def test_one_level_nested_array(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/double_array_schema_check.json"))
        schema = df.schema
        utility = SchemaUtility()
        self.assertTrue(utility.is_array(schema, "root-field-array"))
        self.assertTrue(utility.is_array(schema, "root-field-array.one-level-nested-array"))
        self.assertFalse(utility.is_array(schema, "root-field-array.one-level-nested-array.id"))
        self.assertTrue(utility.is_array(schema, "root-field.array-field"))
        self.assertFalse(utility.is_array(schema, "root-field.array-field.id"))

        # three level hash
        self.assertFalse(utility.is_array(schema, "root-field"))
        self.assertFalse(utility.is_array(schema, "root-field.one-level-nested"))
        self.assertFalse(utility.is_array(schema, "root-field.one-level-nested.two-level-nested"))
        self.assertFalse(utility.is_array(schema, "root-field.one-level-nested.two-level-nested.three-level-nested"))

    def test_field_exist(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/double_array_schema_check.json"))
        schema = df.schema
        utility = SchemaUtility()
        self.assertFalse(utility.is_column_exist(schema, "analytics"))
        self.assertFalse(utility.is_column_exist(schema, "data.analytics._ga"))
        self.assertFalse(utility.is_column_exist(schema, "root-field-array.one-level-nested-array.non-existing-field"))
        self.assertTrue(utility.is_column_exist(schema, "root-field-array.one-level-nested-array.id"))
        self.assertFalse(utility.is_column_exist(schema, "root-field-primitive-array.non-existing-field"))

    def test_flatten_schema(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/simple_schema.json"))
        utility = SchemaUtility()
        flatten_schema = utility.flatten_schema(df.schema)
        expected_fields = ["root-element",
                           "root-element-array-primitive",
                           "root-element-array-of-structs.d1.d2",
                           "nested-structure.n1",
                           "nested-structure.d1.d2"]
        self.assertEqual(set(expected_fields), set(flatten_schema))

    def test_fields_flatten_schema_with_parent_fields(self):
        utility = SchemaUtility()
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/simple_schema.json"))
        flatten_schema = utility.flatten_schema_include_parents_fields(df.schema)
        expected_fields = ["root-element",
                           "root-element-array-primitive",
                           "root-element-array-of-structs",
                           "root-element-array-of-structs.d1",
                           "root-element-array-of-structs.d1.d2",
                           "nested-structure",
                           "nested-structure.n1",
                           "nested-structure.d1",
                           "nested-structure.d1.d2"]
        self.assertEqual(set(expected_fields), set(flatten_schema))

    def test_fields_for_schema(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/simple_schema.json"))
        utility = SchemaUtility()
        schema_for_field = utility.schema_for_field(df.schema, "root-element-array-of-structs.d1")
        self.assertEqual({"d2"}, set(schema_for_field.names))

    def test_parent_field_found_correctly(self):
        utility = SchemaUtility()
        self.assertEqual("juan", utility.parent_element("juan.miguel"))
        self.assertEqual("juan.miguel", utility.parent_element("juan.miguel.altube"))

    def test_parent_field_raise_exception_if_no_parent(self):
        utility = SchemaUtility()
        with pytest.raises(Exception):
            self.assertEqual("juan", utility.parent_element("juan"))
