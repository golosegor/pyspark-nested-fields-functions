import logging
from typing import Any, Dict, List

import pkg_resources
from pyspark.sql import DataFrame

from nestedfunctions.functions.duplicate import duplicate
from tests.unit.functions.spark_base_test import SparkBaseTest
from tests.unit.utils.testing_utils import parse_df_sample

log = logging.getLogger(__name__)


class DuplicateTest(SparkBaseTest):

    def test_duplicate_root(self):
        def parse_root_column(df: DataFrame, column_name:str) -> Any:
            return df.select(column_name).collect()[0][0]

        # Check if as expected before duplicate
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/duplicate_sample.json"))
        self.assertEqual(parse_root_column(df, "eventVersion"), 1.0)

        # Check if as expected after duplicate
        processed = duplicate(df, column_to_duplicate="eventVersion", duplicated_column_name="eventVersionDuplicate")
        self.assertEqual(parse_root_column(processed, "eventVersion"), 1.0)
        self.assertEqual(parse_root_column(processed, "eventVersionDuplicate"), 1.0)

    def test_duplicate_root_array(self):
        def parse_root_column(df: DataFrame, column_name:str) -> Any:
            return df.select(column_name).collect()[0][0]

        # Check if as expected before duplicate
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/duplicate_sample.json"))
        self.assertEqual(parse_root_column(df, "supportedSystems"), ['SAP_S4HANA_CLOUD', None])

        # Check if as expected after duplicate
        processed = duplicate(df, column_to_duplicate="supportedSystems", duplicated_column_name="supportedSystemsDuplicate")
        self.assertEqual(parse_root_column(processed, "supportedSystems"), ['SAP_S4HANA_CLOUD', None])
        self.assertEqual(parse_root_column(processed, "supportedSystemsDuplicate"), ['SAP_S4HANA_CLOUD', None])

    def test_duplicate_field_within_struct_within_nested_arrays(self):
        def parse_item_store_is_on_stock(df: DataFrame, item_index:str, store_index: int) -> bool:
            return df.select("payload.lineItems").collect()[0][0][item_index]["availability"]["stores"][store_index]["isOnStock"]
        def parse_item_store_is_on_stock_duplicate(df: DataFrame, item_index:str, store_index: int) -> bool:
            return df.select("payload.lineItems").collect()[0][0][item_index]["availability"]["stores"][store_index]["isOnStockDuplicate"]

        # Check if as expected before duplicate
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/duplicate_sample.json"))
        self.assertEqual(parse_item_store_is_on_stock(df, item_index=0, store_index=0), True)
        self.assertEqual(parse_item_store_is_on_stock(df, item_index=0, store_index=1), None)
        self.assertEqual(parse_item_store_is_on_stock(df, item_index=1, store_index=0), True)
        self.assertEqual(parse_item_store_is_on_stock(df, item_index=1, store_index=1), None)

        # Check if as expected after duplicate
        processed = duplicate(df, column_to_duplicate="payload.lineItems.availability.stores.isOnStock", duplicated_column_name="payload.lineItems.availability.stores.isOnStockDuplicate")
        self.assertEqual(parse_item_store_is_on_stock(processed, item_index=0, store_index=0), True)
        self.assertEqual(parse_item_store_is_on_stock(processed, item_index=0, store_index=1), None)
        self.assertEqual(parse_item_store_is_on_stock(processed, item_index=1, store_index=0), True)
        self.assertEqual(parse_item_store_is_on_stock(processed, item_index=1, store_index=1), None)
        self.assertEqual(parse_item_store_is_on_stock_duplicate(processed, item_index=0, store_index=0), True)
        self.assertEqual(parse_item_store_is_on_stock_duplicate(processed, item_index=0, store_index=1), None)
        self.assertEqual(parse_item_store_is_on_stock_duplicate(processed, item_index=1, store_index=0), True)
        self.assertEqual(parse_item_store_is_on_stock_duplicate(processed, item_index=1, store_index=1), None)

    def test_duplicate_array_within_arrays(self):
        def parse_comments_for_line_item(df: DataFrame, item_index:str) -> str:
            return df.select("payload.lineItems").collect()[0][0][item_index]["comments"]
        def parse_comments_duplicate_for_line_item(df: DataFrame, item_index:str) -> str:
            return df.select("payload.lineItems").collect()[0][0][item_index]["commentsDuplicate"]

        # Check if as expected before duplicate
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/duplicate_sample.json"))
        self.assertEqual(parse_comments_for_line_item(df, item_index=0), ['Manually triggered stock check', None])
        self.assertEqual(parse_comments_for_line_item(df, item_index=1), None)

        processed = duplicate(df, column_to_duplicate="payload.lineItems.comments", duplicated_column_name="payload.lineItems.commentsDuplicate")
        self.assertEqual(parse_comments_for_line_item(processed, item_index=0), ['Manually triggered stock check', None])
        self.assertEqual(parse_comments_for_line_item(processed, item_index=1), None)
        self.assertEqual(parse_comments_duplicate_for_line_item(processed, item_index=0), ['Manually triggered stock check', None])
        self.assertEqual(parse_comments_duplicate_for_line_item(processed, item_index=1), None)

