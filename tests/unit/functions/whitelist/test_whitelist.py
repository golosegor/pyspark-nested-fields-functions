import logging
from typing import List

import pkg_resources
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

from nestedfunctions.functions.whitelist import whitelist, filter_only_parents_fields
from tests.unit.functions.spark_base_test import SparkBaseTest
from tests.unit.utils.testing_utils import parse_df_sample

logging.getLogger('metadata_core.processors.generic').setLevel(logging.DEBUG)
logging.getLogger('metadata_core.processors.dropping').setLevel(logging.DEBUG)


class WhitelistTest(SparkBaseTest):

    def test_filter_parent_select_root_common_ancestor(self):
        filtered = filter_only_parents_fields({"address",
                                               "address.postalCode",
                                               "address.whatever",
                                               "data",
                                               "data.name",
                                               "data.surname",
                                               "egorka228"})
        self.assertEqual({'address', 'data', 'egorka228'}, filtered)

    def test_filter_parent_select_level_2_common_ancestor(self):
        filtered = filter_only_parents_fields({"address.postalCode.number",
                                               "address.postalCode.region",
                                               "address.whatever"})
        self.assertEqual({'address.postalCode.number', 'address.postalCode.region', 'address.whatever'}, filtered)

    def test_filter_parent_select_level_2_common_ancestor_2(self):
        filtered = filter_only_parents_fields({"address.postalCode.number",
                                               "address.postalCode.region",
                                               "address.whatever",
                                               "address.postalCode"})
        self.assertEqual({'address.postalCode', 'address.whatever'}, filtered)

    def test_filter_parent_empty_set(self):
        filtered = filter_only_parents_fields(set())
        self.assertEqual(set(), filtered)

    def test_filter_parent_single_root_element(self):
        filtered = filter_only_parents_fields({"element1"})
        self.assertEqual({"element1"}, filtered)

    def test_filter_parent_two_root_elements(self):
        whitelist = {
            'lastUpdated',
            'lastUpdatedTimestamp',
        }
        parents_only = filter_only_parents_fields(whitelist)
        self.assertEqual(whitelist, parents_only)

    def test_whitelist_select_root_common_ancestor(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/white-list-sample.json"))
        processed = whitelist(df, ["addresses.postalCode",
                                   "addresses.postalCode",
                                   "addresses",
                                   "non-existing",
                                   "creditCard"])
        self.assertEqual(df.collect(), processed.collect())

    def test_whitelist_ignored_unknown_fields(self):
        def parse_data(df: DataFrame) -> str:
            return df.select("creditCard").collect()[0]["creditCard"]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/white-list-sample.json"))
        processed = whitelist(df, ["creditCard", "non-existing-field.asdfasdf.asdfasdf"])
        self.assertEqual("123456", parse_data(processed))
        struct_type = StructType([StructField("creditCard", StringType())])
        self.assertEqual(struct_type.jsonValue(), processed.schema.jsonValue())

    def test_whitelist_can_handle_array_in_array(self):
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

    def test_whitelist_non_existing_fields_empty_df_returned(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/white-list-sample.json"))
        self.assertTrue(df.count() > 0)
        processed = whitelist(df, ["non-existing-field"])
        self.assertTrue(processed.count() == 0)

    def test_whitelist_drops_nested_structure(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/whitelist_with_complex_structure.json"))
        self.assertEqual({"field1", "results"}, set(df.schema.names))
        processed = whitelist(df, ["field1"])
        self.assertEqual({"field1"}, set(processed.schema.names))

    def test_whitelist_multiple_root_fields(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/whitelist-input.json"))
        self.assertEqual({
            "UID",
            "created",
            "identities",
        }, set(df.schema.names))
        processed = whitelist(df, ["UID", "created"])
        self.assertEqual({"UID", "created"}, set(processed.schema.names))
    
    def test_whitelist_keep_parent_if_only_child_whitelisted(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/whitelist_with_complex_structure.json"))
        self.assertEqual({"field1", "results"}, set(df.schema.names))
        processed = whitelist(df, ["results.lineItems"])
        self.assertEqual({"results"}, set(processed.schema.names))
