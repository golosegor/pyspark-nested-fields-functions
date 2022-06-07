import logging
from typing import List, Dict

import pkg_resources
import pyspark.sql.functions as F
import pytest
from pyspark.sql import DataFrame

from nestedfunctions.functions.hash import hash_field, hash_field_with_predicate
from tests.functions.hashing.hash_fun import hash_udf
from tests.functions.spark_base_test import SparkBaseTest
from tests.utils.testing_utils import parse_df_sample

log = logging.getLogger(__name__)


class HashingTest(SparkBaseTest):

    def test_root_level_hash(self):
        def parse_data(df: DataFrame) -> str:
            return df.select("id").collect()[0][0]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/root_hash.json"))
        self.assertEqual(parse_data(df), "my-key-to-hash")
        hashed = hash_field(df, field="id")
        manual = df.withColumn("id", F.sha2(F.col("id"), 256))
        self.assertEqual(hash_udf("my-key-to-hash"), parse_data(hashed))
        self.assertEqual(hashed.collect(), manual.collect())

    def test_root_level_primitive_array(self):
        def parse_data(df: DataFrame) -> str:
            return df.select("emails").collect()[0][0]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/root_level_primitive_array.json"))
        self.assertEqual(parse_data(df), ["email@veryhotmail.com", "name@geemail.com"])
        hashed = hash_field(df, field="emails")
        manual = df.withColumn("emails", F.transform("emails", lambda d: F.sha2(d, 256)))
        self.assertEqual([hash_udf("email@veryhotmail.com"), hash_udf("name@geemail.com")], parse_data(hashed))
        self.assertEqual(hashed.collect(), manual.collect())

    def test_two_level_nested_array(self):
        def parse_data(df: DataFrame) -> List[str]:
            addresses = df.select("data.addresses").collect()[0][0]
            return [r['id'] for r in addresses]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/two_level_nested_array.json"))
        self.assertEqual(parse_data(df), ["my-id", "my-id2"])
        hashed = hash_field(df, "data.addresses.id")
        manual = df.withColumn("data",
                               F.col("data").withField("addresses",
                                                       F.transform(F.col("data").getField("addresses"),
                                                                   lambda c: c.withField("id", F.sha2(c.getField("id"),
                                                                                                      256)))))
        self.assertEqual(parse_data(hashed), [hash_udf("my-id"), hash_udf("my-id2")])
        self.assertEqual(hashed.collect(), manual.collect())

    def test_one_level_nested_primitive_array(self):
        def parse_data(df: DataFrame) -> List[str]:
            return df.select("emails.unverified").collect()[0][0]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/one_level_primitive_array.json"))
        self.assertEqual(parse_data(df), ["email@veryhotmail.com", "name@geemail.com"])
        manual = df.withColumn("emails",
                               F.col("emails").withField("unverified",
                                                         F.transform("emails.unverified", lambda d: F.sha2(d, 256))))
        hashed = hash_field(df, field="emails.unverified")
        self.assertEqual(parse_data(hashed), [hash_udf("email@veryhotmail.com"), hash_udf("name@geemail.com")])
        self.assertEqual(hashed.collect(), manual.collect())

    def test_three_level_nested_array(self):
        def parse_data(df: DataFrame) -> List[str]:
            addresses = df.select("data.city.addresses").collect()[0][0]
            return [r['id'] for r in addresses]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/three_level_nested_array.json"))
        self.assertEqual(parse_data(df), ["my-id", "my-id2"])
        manual = df.withColumn("data",
                               F.col("data").withField("city", F.col("data.city").withField("addresses", F.transform(
                                   "data.city.addresses",
                                   lambda c: c.withField("id", F.sha2(c.getField("id"), 256))))))
        hashed = hash_field(df, "data.city.addresses.id")
        self.assertEqual(parse_data(hashed), [hash_udf("my-id"), hash_udf("my-id2")])
        self.assertEqual(hashed.collect(), manual.collect())

    def test_three_level_nested_field(self):
        def parse_data(df: DataFrame) -> str:
            data_to_hash = df.select("data.city.address.id").collect()[0][0]
            return data_to_hash

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/three_level_nested_field.json"))
        self.assertEqual(parse_data(df), "data-to-hash")
        manual = df.withColumn("data",
                               F.col("data").withField("city",
                                                       F.col("data.city")
                                                       .withField("address",
                                                                  F.col("data.city.address").withField("id", F.sha2(
                                                                      F.col("data.city.address.id"), 256)))))
        hashed = hash_field(df, "data.city.address.id")
        self.assertEqual(parse_data(hashed), hash_udf("data-to-hash"))
        self.assertEqual(hashed.collect(), manual.collect())

    def test_two_level_nested_field(self):
        def parse_data(df: DataFrame) -> str:
            data_to_hash = df.select("data.address.id").collect()[0][0]
            return data_to_hash

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/two_level_nested_field.json"))
        self.assertEqual(parse_data(df), "data-to-hash")
        hashed = hash_field(df, field="data.address.id")
        self.assertEqual(parse_data(hashed), hash_udf("data-to-hash"))
        manual_script = df.withColumn("data",
                                      F.col("data").withField("address",
                                                              F.col("data.address").withField("id", F.sha2(
                                                                  F.col("data.address.id"), 256))))
        self.assertEqual(hashed.collect(), manual_script.collect())

    def test_double_array(self):
        def parse_data(df: DataFrame) -> List[str]:
            addresses = df.select("data.addresses").collect()[0][0]
            return [r['id'] for r in addresses[0]]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/double_array.json"))
        self.assertEqual(parse_data(df), ["my-id", "my-id2"])
        hashed = hash_field(df, field="data.addresses.id")
        manual = df.withColumn("data",
                               F.transform(F.col("data"),
                                           lambda e: e.withField("addresses",
                                                                 F.transform(e.getField("addresses"),
                                                                             lambda c: c.withField("id", F.sha2(
                                                                                 c.getField("id"), 256))))))
        self.assertEqual(parse_data(hashed), [hash_udf("my-id"), hash_udf("my-id2")])
        self.assertEqual(hashed.collect(), manual.collect())

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
        hashed = hash_field_with_predicate(df, "customDimensions.index", "value", "SUPER_SECRET_VALUE")
        self.assertEqual(parse_data(hashed), {
            hash_udf(13): "SUPER_SECRET_VALUE",
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
        hashed = hash_field_with_predicate(df, field="hits.custom dimensions.value",
                                           predicate_key="index",
                                           predicate_value="13")
        self.assertEqual(parse_data(hashed), {
            13: hash_udf("SUPER_SECRET_VALUE"),
            2: "normal-value"
        })

    def test_four_level_primitive_array(self):
        def parse_data(df: DataFrame) -> str:
            return df.collect()[0][0][0][0][0][0]["parcels"][0]["trackingData"]["trackingId"]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/four_level_nested_array.json"))
        expected_value = 'https://www.dhl.de/de/privatkunden/pakete-empfangen/verfolgen.html?lang=de&idc=00340434462561258861'
        self.assertEqual(expected_value, parse_data(df))
        hashed = hash_field(df, field="results.shippingInfo.deliveries.parcels.trackingData.trackingId")
        self.assertEqual(hash_udf(expected_value), parse_data(hashed))

    def test_hash_processor_throws_exception_if_field_is_invalid(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/four_level_nested_array.json"))
        with pytest.raises(Exception):
            hash_field(df, field="userId$")
