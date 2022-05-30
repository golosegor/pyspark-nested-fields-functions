import logging
from typing import Dict

import pkg_resources
from pyspark.sql import DataFrame

from nestedfunctions.transformations.hash import hash_field_with_predicate
from tests.transformations.hashing.hash_fun import hash_with_hashlib
from tests.utils.spark_base_test import SparkBaseTest, parse_df_sample

log = logging.getLogger(__name__)


class HashingWithPredicateTest(SparkBaseTest):
    def test_hashing_array_with_predicated_on_root_level_works_fine(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/array_on_root_level_transformation.json"))

        def parse_data(df: DataFrame) -> Dict[str, str]:
            custom_dimensions = df.select("customDimensions").collect()[0]
            return {r['index']: r['value'] for r in custom_dimensions[0]}

        self.assertEqual(parse_data(df), {
            13: "SUPER_SECRET_VALUE",
            2: "normal-value"
        })
        hashed = hash_field_with_predicate(df, field="customDimensions.index",predicate_key="value", predicate_value="SUPER_SECRET_VALUE")
        self.assertEqual(parse_data(hashed), {
            hash_with_hashlib(13): "SUPER_SECRET_VALUE",
            "2": "normal-value"
        })

    def test_hashing_array_with_predicated_on_nested_level_works_fine(self):
        def parse_data(df: DataFrame) -> Dict[str, str]:
            custom_dimensions = df.select("hits.custom dimensions").collect()[0][0]
            return {r['index']: r['value'] for r in custom_dimensions[0]}

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/array_on_nested_level_transformation.json"))

        self.assertEqual(parse_data(df), {
            13: "SUPER_SECRET_VALUE",
            2: "normal-value"
        })
        hashed = hash_field_with_predicate(df=df,
                                           field="hits.custom dimensions.value",
                                           predicate_key="index",
                                           predicate_value="13")
        self.assertEqual(parse_data(hashed), {
            13: hash_with_hashlib("SUPER_SECRET_VALUE"),
            2: "normal-value"
        })
