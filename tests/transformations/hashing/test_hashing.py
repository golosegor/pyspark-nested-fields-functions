import logging
from typing import List

import pkg_resources
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from sparkrecursive.transformations.transformations import hash_field
from tests.transformations.hashing.hash_fun import hash_with_hashlib
from tests.utils.spark_base_test import SparkBaseTest, parse_df_sample

log = logging.getLogger(__name__)


class HashingTest(SparkBaseTest):

    def test_root_level_hash(self):
        def parse_data(df: DataFrame) -> str:
            return df.select("id").collect()[0][0]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/root_hash.json"))
        self.assertEqual(parse_data(df), "my-key-to-hash")
        hashed = hash_field(df, "id")
        manual = df.withColumn("id", F.sha2(F.col("id"), 256))
        self.assertEqual(hash_with_hashlib("my-key-to-hash"), parse_data(hashed))
        self.assertEqual(hashed.collect(), manual.collect())

    def test_root_level_primitive_array(self):
        def parse_data(df: DataFrame) -> str:
            return df.select("emails").collect()[0][0]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/root_level_primitive_array.json"))
        self.assertEqual(parse_data(df), ["email@veryhotmail.com", "name@geemail.com"])
        hashed = hash_field(df, "emails")
        manual = df.withColumn("emails", F.transform("emails", lambda d: F.sha2(d, 256)))
        self.assertEqual([hash_with_hashlib("email@veryhotmail.com"), hash_with_hashlib("name@geemail.com")],
                         parse_data(hashed))
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
        self.assertEqual(parse_data(hashed), [hash_with_hashlib("my-id"), hash_with_hashlib("my-id2")])
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
        hashed = hash_field(df, "emails.unverified")
        self.assertEqual(parse_data(hashed),
                         [hash_with_hashlib("email@veryhotmail.com"), hash_with_hashlib("name@geemail.com")])
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
        self.assertEqual(parse_data(hashed), [hash_with_hashlib("my-id"), hash_with_hashlib("my-id2")])
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
        hashed = hash_field(df, field="data.city.address.id")
        self.assertEqual(parse_data(hashed), hash_with_hashlib("data-to-hash"))
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
        self.assertEqual(parse_data(hashed), hash_with_hashlib("data-to-hash"))
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
        self.assertEqual(parse_data(hashed), [hash_with_hashlib("my-id"), hash_with_hashlib("my-id2")])
        self.assertEqual(hashed.collect(), manual.collect())

    def test_four_level_primitive_array(self):
        def parse_data(df: DataFrame) -> str:
            return df.collect()[0][0][0][0][0][0]["parcels"][0]["trackingData"]["trackingId"]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/four_level_nested_array.json"))
        expected_value = 'https://www.dhl.de/de/privatkunden/pakete-empfangen/verfolgen.html?lang=de&idc=121241241fff124124'
        self.assertEqual(expected_value, parse_data(df))
        hashed = hash_field(df, field="results.shippingInfo.deliveries.parcels.trackingData.trackingId")
        self.assertEqual(hash_with_hashlib(expected_value), parse_data(hashed))
