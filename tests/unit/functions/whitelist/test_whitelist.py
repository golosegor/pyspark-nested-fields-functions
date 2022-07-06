import logging
from typing import List

import pkg_resources
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

from nestedfunctions.functions.whitelist import whitelist, filter_only_parents_fields, WhitelistProcessor
from tests.unit.functions.spark_base_test import SparkBaseTest
from tests.unit.utils.testing_utils import parse_df_sample

logging.getLogger('metadata_core.processors.generic').setLevel(logging.DEBUG)
logging.getLogger('metadata_core.processors.dropping').setLevel(logging.DEBUG)


class WhitelistTest(SparkBaseTest):

    def test_whitelist_select_only_common_element_integration(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/white-list-sample.json"))
        processed = whitelist(df, ["addresses.postalCode",
                                   "addresses.postalCode",
                                   "addresses",
                                   "non-existing",
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

    def test_whitelist_select_only_common_element_gigya_bug(self):
        whitelist = {
            'lastUpdated',
            'lastUpdatedTimestamp',
        }
        parents_only = filter_only_parents_fields(list(whitelist))
        self.assertEqual(whitelist, set(parents_only))

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

    def test_whitelist_is_working_properly(self):
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

    def test_fields_to_delete_found_correctly(self):
        fields_to_drop = WhitelistProcessor.find_fields_to_drop(
            flattened_fields={"UID", "created", "identities.providerUID"},
            whitelist={"UID", "created"})
        self.assertEqual({"identities.providerUID"}, fields_to_drop)

    def test_fields_to_delete_found_correctly2(self):
        fields_to_drop = WhitelistProcessor.find_fields_to_drop(
            flattened_fields={
                "creditCard",
                "addresses.postalCode",
                "addresses.flats.escalera",
                "addresses.flats.piso",
                "addresses.zipCode"},
            whitelist={"addresses.postalCode",
                       "addresses.postalCode",
                       "addresses",
                       "non-existing",
                       "creditCard"})
        self.assertEqual(set(), fields_to_drop)

    def all_fields_are_about_to_drop(self):
        fields_to_drop = WhitelistProcessor.find_fields_to_drop(
            flattened_fields={
                "creditCard",
                "addresses.postalCode",
                "addresses.flats.escalera",
                "addresses.flats.piso",
                "addresses.zipCode"},
            whitelist={"addresses.postalCode",
                       "addresses.postalCode",
                       "addresses",
                       "non-existing",
                       "creditCard"})
        self.assertEqual(set(), fields_to_drop)

    def test_gigya_scenario(self):
        fields_to_drop = WhitelistProcessor.find_fields_to_drop(
            flattened_fields={
                "creditCard",
                "addresses.postalCode",
                "addresses.flats.escalera",
                "addresses.flats.piso",
                "addresses.zipCode"},
            whitelist={"addresses.postalCode",
                       "addresses.postalCode",
                       "addresses",
                       "non-existing",
                       "creditCard"})
        self.assertEqual(set(), fields_to_drop)

    def test_ga_scenario(self):
        fields = {'hits.page.pagePathLevel1'}
        whitelist = {
            "hits.page.hostname",
            "hits.page.pagePath",
            "hits.page.pagePathLevel1"
        }
        fields_to_drop = WhitelistProcessor.find_fields_to_drop(flattened_fields=fields, whitelist=whitelist)
        self.assertFalse("hits.page.pagePathLevel1" in fields_to_drop)
