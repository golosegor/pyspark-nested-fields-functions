import logging
from typing import List

import pkg_resources
import pytest
from pyspark.sql import DataFrame

from nestedfunctions.functions.truncate import truncate
from tests.unit.functions.spark_base_test import SparkBaseTest
from tests.unit.utils.testing_utils import parse_df_sample

log = logging.getLogger(__name__)


class TruncateTest(SparkBaseTest):

    def test_primitive_array(self):
        self.__execute_array_flow("address.zipCodes", 2, expected_result=["65", "76", "87"])

    def test_primitive_array_negative_supported(self):
        self.__execute_array_flow("address.zipCodes", -2, expected_result=["6543", "76543", "876543"])

    def __execute_array_flow(self, field: str, size: int, expected_result: List[str]):
        def parse_data(df: DataFrame) -> List[str]:
            return df.select("address.zipCodes").collect()[0][0]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/truncate_sample.json"))
        self.assertEqual(parse_data(df), ["654321", "7654321", "87654321"])
        processed = truncate(df, field, size)
        self.assertEqual(expected_result, parse_data(processed))

    def test_one_level_nested(self):
        def parse_data(df: DataFrame) -> str:
            return df.select("address.postalCode").collect()[0]["postalCode"]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/truncate_sample.json"))
        self.assertEqual(parse_data(df), "1234567")
        processed = truncate(df, "address.postalCode", -3)
        self.assertEqual("1234", parse_data(processed))

    def test_root_level(self):
        self.__execute_test_root_level("creditCard", 2, expected_result="12")

    def test_root_level_reversed(self):
        self.__execute_test_root_level("creditCard", -2, expected_result="1234")

    def __execute_test_root_level(self, field: str, size: int, expected_result: str):
        def parse_data(df: DataFrame) -> str:
            return df.select("creditCard").collect()[0]["creditCard"]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/truncate_sample.json"))
        self.assertEqual(parse_data(df), "123456")
        processed = truncate(df, field, size)
        self.assertEqual(expected_result, parse_data(processed))

    def test_truncate_processor_factory_throws_exception_if_field_is_invalid(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/truncate_sample.json"))
        with pytest.raises(Exception):
            truncate(df, "address.postalCode$", -3)
