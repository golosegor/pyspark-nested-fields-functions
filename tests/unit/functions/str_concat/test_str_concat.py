import logging

import pkg_resources
import pytest
from pyspark.sql import DataFrame

from nestedfunctions.functions.str_concat import str_concat
from tests.unit.functions.spark_base_test import SparkBaseTest
from tests.unit.utils.testing_utils import parse_df_sample

log = logging.getLogger(__name__)


class StringContactTest(SparkBaseTest):

    def test_one_level_nested(self):
        def parse_data(df: DataFrame) -> str:
            return df.select("creditCard").collect()[0][0]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/str_concat_sample.json"))
        self.assertEqual(parse_data(df), "my-value")
        const_to_add = "value-to-addd111231235123"
        processed = str_concat(df, "creditCard", const_to_add)
        self.assertEqual(parse_data(processed), f"my-value{const_to_add}")

    def test_str_concat_processor_factory_throws_exception_if_field_is_invalid(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/str_concat_sample.json"))

        with pytest.raises(Exception):
            str_concat(df, field="userId$", str_to_add="whatever$")

    def test_str_concat_processor_factory_not_throws_exception_if_field_is_invalid(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/str_concat_sample.json"))
        str_concat(df, "userId", "whatever$")
