import logging
from typing import List, Dict

import pkg_resources
import pytest
from pyspark.sql import DataFrame

from nestedfunctions.functions.nullify import nullify, nullify_with_predicate
from tests.unit.functions.spark_base_test import SparkBaseTest
from tests.unit.utils.testing_utils import parse_df_sample

log = logging.getLogger(__name__)


class NullabilityTest(SparkBaseTest):

    def test_nullability(self):
        def parse_data(df: DataFrame) -> List[str]:
            return [d[0] for d in df.select("creditCard").collect()]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/nullify_sample.json"))
        self.assertEqual(["value1", "value2"], parse_data(df))
        processed = nullify(df, field="creditCard")
        self.assertEqual([None, None], parse_data(processed))

    def test_nullability_with_predicate(self):
        def parse_data(df: DataFrame) -> Dict[str, str]:
            custom_dimensions = df.select("customDimensions").collect()[0]
            return {r['index']: r['value'] for r in custom_dimensions[0]}

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/nullify_with_predicate.json"))
        self.assertEqual({
            13: "(not set)",
            2: "normal-value"
        }, parse_data(df))
        processed = nullify_with_predicate(df,
                                           field="customDimensions.value",
                                           predicate_key="value",
                                           predicate_value="(not set)")
        self.assertEqual({
            13: None,
            2: "normal-value"
        }, parse_data(processed))

    def test_nullify_processor_factory_throws_exception_if_field_is_invalid(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/nullify_with_predicate.json"))
        with pytest.raises(Exception):
            nullify(df, "userId$")
