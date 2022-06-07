import logging
from typing import List

import pkg_resources
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

from nestedfunctions.functions.whitelist import whitelist
from nestedfunctions.processors.whitelist.whitelist_processor import filter_only_parents_fields
from tests.functions.spark_base_test import SparkBaseTest
from tests.utils.testing_utils import parse_df_sample

logging.getLogger('nestedfunctions.processors.generic').setLevel(logging.DEBUG)
logging.getLogger('nestedfunctions.processors.dropping').setLevel(logging.DEBUG)


class WhitelistTest(SparkBaseTest):

    def test_whitelist_select_only_common_element_integration(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/white-list-sample.json"))
        processed = whitelist(df, ["addresses.postalCode", "addresses.postalCode", "addresses", "non-existing",
                                   "creditCard"])
        self.assertEqual(df.collect(), processed.collect())

    def test_whitelist_select_only_common_element(self):
        filtered = filter_only_parents_fields(["address",
                                               "address.postalCode",
                                               "address.whatever",
                                               "data",
                                               "data.name",
                                               "data.surname",
                                               "egorka228"])
        self.assertEqual({'address', 'data', 'egorka228'}, set(filtered))

    def test_whitelist_select_only_common_element_in_second_level(self):
        filtered = filter_only_parents_fields(["address.postalCode.number",
                                               "address.postalCode.region",
                                               "address.whatever"])
        self.assertEqual({'address.postalCode.number', 'address.postalCode.region', 'address.whatever'}, set(filtered))

    def test_whitelist_select_only_common_element_in_second_level_v2(self):
        filtered = filter_only_parents_fields(["address.postalCode.number",
                                               "address.postalCode.region",
                                               "address.whatever",
                                               "address.postalCode"])
        self.assertEqual({'address.postalCode', 'address.whatever'}, set(filtered))

    def test_whitelist_select_only_common_element_t1(self):
        filtered = filter_only_parents_fields([])
        self.assertEqual([], filtered)

    def test_whitelist_select_only_common_element_t2(self):
        filtered = filter_only_parents_fields(["element1"])
        self.assertEqual(["element1"], filtered)

    def test_whitelist_ignored_unknown_fields(self):
        def parse_data(df: DataFrame) -> str:
            return df.select("creditCard").collect()[0]["creditCard"]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/white-list-sample.json"))
        processed = whitelist(df, ["creditCard", "non-existing-field.asdfasdf.asdfasdf"])
        self.assertEqual("123456", parse_data(processed))
        struct_type = StructType([StructField("creditCard", StringType())])
        self.assertEqual(struct_type.jsonValue(), processed.schema.jsonValue())

    def test_whitelist_could_handled_double_array(self):
        def parse_data(dfff: DataFrame) -> List[int]:
            addresses = dfff.select("addresses.flats").collect()[0][0]
            return [r['piso'] for r in addresses[0]]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/white-list-sample.json"))
        self.assertEqual([5, 15], parse_data(df))
        processed = whitelist(df, ["addresses.flats.piso"])
        self.assertEqual([5, 15], parse_data(df))
        s = StructType([StructField("addresses",
                                    ArrayType(StructType([
                                        StructField("flats",
                                                    ArrayType(StructType([StructField("piso",
                                                                                      LongType())])))])))])

        self.assertEqual(processed.schema.jsonValue(), s.jsonValue())

    def test_whitelisting_non_existing_fields_empty_df_returned(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/white-list-sample.json"))
        self.assertTrue(df.count() > 0)
        processed = whitelist(df, ["non-existing-field"])
        self.assertTrue(processed.count() == 0)

    def test_drops_with_special_field_name(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/whitelist_with_complex_structure.json"))
        self.assertEqual({"field1", "results"}, set(df.schema.names))
        processed = whitelist(df, ["field1"])
        self.assertEqual({"field1"}, set(processed.schema.names))
