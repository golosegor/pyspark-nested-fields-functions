import json
import logging
from pathlib import Path

import pkg_resources
import pyspark.sql.functions as F
import pytest
from pyspark.sql import DataFrame

from nestedfunctions.functions.drop import drop
from nestedfunctions.processors.coreprocessor import CoreProcessor
from nestedfunctions.spark_schema.utility import SparkSchemaUtility
from tests.unit.functions.spark_base_test import SparkBaseTest
from tests.unit.utils.testing_utils import parse_df_sample

log = logging.getLogger(__name__)


class DroppingColumnTest(SparkBaseTest):

    def test_root_level_dropping_supported(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/data_root_level_dropping_sample.json"))
        processed = drop(df, field="userId")
        expected_fields = ['username', 'home']
        dropped_fields = processed.columns

        self.assertEqual(set(dropped_fields), set(expected_fields))

    def check_nested_dropping_using_json_file(self,
                                              data_path: str,
                                              column_name: str,
                                              expected_schema_path: str,
                                              processor: CoreProcessor):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, data_path))

        processed = drop(df, field=column_name)
        manual = processor.process(df)
        self.assertEqual(manual.collect(), processed.collect())

        dropped_json = processed.schema.json()

        path = str(Path(pkg_resources.resource_filename(__name__, expected_schema_path)))

        with open(path) as json_file:
            expected_json = json.load(json_file)
            dropped_json2 = json.loads(dropped_json)

            json1 = json.dumps(expected_json, sort_keys=True)
            json2 = json.dumps(dropped_json2, sort_keys=True)

            self.assertEqual(json1, json2)

    def test_nested_level_dropping_supported(self):
        class ManualProcessor(CoreProcessor):

            def process(self, df: DataFrame) -> DataFrame:
                return df.withColumn("home", F.col("home").dropFields("home_id"))

        self.check_nested_dropping_using_json_file("fixtures/data_root_level_dropping_sample.json",
                                                   "home.home_id",
                                                   "fixtures/expected_json_without_home_id.json",
                                                   ManualProcessor())

    def test_drops_while_structure_in_case_no_fields_left(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/struct_with_last_nested_field.json"))
        self.assertEqual(["nested", "userId"], df.schema.names)
        df_with_dropped = drop(df, field="nested.CMA Brands")
        self.assertEqual(["userId"], df_with_dropped.schema.names)

    def test_drops_while_structure_in_case_no_fields_left2(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/struct_with_last_nested_field2.json"))
        self.assertEqual(["nested", "userId"], df.schema.names)
        df_with_dropped = drop(df, field="nested.home_id.value")
        self.assertEqual(["userId"], df_with_dropped.schema.names)

    def test_drops_while_structure_in_case_no_fields_left3(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/struct_with_last_nested_field3.json"))
        self.assertEqual(["nested", "userId"], df.schema.names)
        df_with_dropped = drop(df, field="nested.home_id.value")
        self.assertEqual(["userId"], df_with_dropped.schema.names)

    def test_drops_arrays(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/array_drop_fixture.json"))
        utility = SparkSchemaUtility()
        self.assertEqual({"creditCard", "addresses"}, set(df.schema.names))
        self.assertEqual({"zipCode", "flats"}, set(utility.schema_for_field(df.schema, "addresses").names))
        df_with_dropped = drop(df, field="addresses.flats")
        df_with_dropped_manually = df.withColumn("addresses",
                                                 F.transform(F.col("addresses"),
                                                             lambda e: e.dropFields("flats")))
        self.assertEqual(df_with_dropped_manually.collect(), df_with_dropped.collect())
        self.assertEqual({"creditCard", "addresses"}, set(df_with_dropped.schema.names))
        self.assertEqual({"zipCode"}, set(utility.schema_for_field(df_with_dropped.schema, "addresses").names))

    def test_drop_processor_throws_exception_if_field_is_invalid(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/array_drop_fixture.json"))
        with pytest.raises(Exception):
            drop(df, field="userId$")
