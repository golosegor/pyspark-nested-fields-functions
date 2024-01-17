import logging
from typing import Dict, List

import pkg_resources
from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F

from nestedfunctions.functions.add_nested_field import add_nested_field

from tests.unit.functions.spark_base_test import SparkBaseTest
from tests.unit.utils.testing_utils import parse_df_sample

log = logging.getLogger(__name__)


class AddOperationsTest(SparkBaseTest):

    def test_boolean_field_within_struct_within_nested_arrays_to_char_1(self):
        def parse_item_store_is_on_stock(df: DataFrame, item_index:str, store_index: int, is_on_stock_column_name: str) -> bool:
            return df.select("payload.lineItems").collect()[0][0][item_index]["availability"]["stores"][store_index][is_on_stock_column_name]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/add_operations_sample.json"))
        self.assertEqual(parse_item_store_is_on_stock(df, item_index=0, store_index=0, is_on_stock_column_name="isOnStock"), True)
        self.assertEqual(parse_item_store_is_on_stock(df, item_index=0, store_index=1, is_on_stock_column_name="isOnStock"), False)
        self.assertEqual(parse_item_store_is_on_stock(df, item_index=1, store_index=0, is_on_stock_column_name="isOnStock"), True)
        self.assertEqual(parse_item_store_is_on_stock(df, item_index=1, store_index=1, is_on_stock_column_name="isOnStock"), None)

        def boolean_column_to_char_1_column(column: Column) -> Column:
           return F.when(column== True,"Y").when(column== False,"N").otherwise("")
        processed = add_nested_field(df,
                                        "payload.lineItems.availability.stores.isOnStock",
                                        "payload.lineItems.availability.stores.isOnStockChar1",
                                        lambda column: boolean_column_to_char_1_column(column))
        self.assertEqual(parse_item_store_is_on_stock(processed, item_index=0, store_index=0, is_on_stock_column_name="isOnStockChar1"), "Y")
        self.assertEqual(parse_item_store_is_on_stock(processed, item_index=0, store_index=1, is_on_stock_column_name="isOnStockChar1"), "N")
        self.assertEqual(parse_item_store_is_on_stock(processed, item_index=1, store_index=0, is_on_stock_column_name="isOnStockChar1"), "Y")
        self.assertEqual(parse_item_store_is_on_stock(processed, item_index=1, store_index=1, is_on_stock_column_name="isOnStockChar1"), "")
