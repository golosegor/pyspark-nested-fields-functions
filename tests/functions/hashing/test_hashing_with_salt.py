import logging

import pkg_resources
import pytest
from pyspark.sql import DataFrame

from nestedfunctions.functions.hash import hash_field_with_salt, DEFAULT_SALT_VALUE, DEFAULT_SALT_SEPARATOR
from tests.functions.hashing.hash_fun import hash_udf
from tests.functions.spark_base_test import SparkBaseTest
from tests.utils.testing_utils import parse_df_sample

log = logging.getLogger(__name__)


class HashingWithSaltTest(SparkBaseTest):

    def test_root_level_hash_with_salt(self):
        def parse_data(df: DataFrame) -> str:
            return df.select("id").collect()[0][0]

        df = parse_df_sample(self.spark, pkg_resources.resource_filename(__name__, "fixtures/root_hash.json"))
        self.assertEqual(parse_data(df), "my-key-to-hash")
        hashed = hash_field_with_salt(df, "id")
        self.assertEqual(hash_udf(f"{DEFAULT_SALT_VALUE}{DEFAULT_SALT_SEPARATOR}my-key-to-hash"), parse_data(hashed))

    def test_root_level_hash_with_salt_empty_field(self):
        def parse_data(df: DataFrame) -> str:
            return df.select("emptyField").collect()[0][0]

        df = parse_df_sample(self.spark, pkg_resources.resource_filename(__name__, "fixtures/root_hash.json"))
        self.assertEqual(parse_data(df), "")
        hashed = hash_field_with_salt(df, field="emptyField")
        self.assertEqual(hash_udf(f"{DEFAULT_SALT_VALUE}{DEFAULT_SALT_SEPARATOR}"), parse_data(hashed))

    def test_hash_with_salt_processor_throws_exception_if_field_is_invalid(self):
        df = parse_df_sample(self.spark, pkg_resources.resource_filename(__name__, "fixtures/root_hash.json"))
        with pytest.raises(Exception):
            hash_field_with_salt(df, field="userId$")
