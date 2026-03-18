import logging

import pkg_resources
from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *

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

    def test_add_nested_field_when_ancestor_struct_null(self):
        schema = StructType(
            [
                StructField('eventName', StringType(), True),
                StructField('eventVersion', StringType(), True),
                StructField('payload', StructType(
                    [
                        StructField('lineItems', ArrayType(
                            StructType(
                                [
                                    StructField('availability', StructType(
                                        [
                                            StructField('availableQuantity', LongType(), True),
                                            StructField('isOnStock', BooleanType(), True),
                                            StructField('storeId', StringType(), True),
                                            StructField('availableQuantityForSize', StructType(
                                                [
                                                    StructField('availableQuantityS', LongType(), True),
                                                    StructField('availableQuantityM', LongType(), True),
                                                    StructField('availableQuantityL', LongType(), True)
                                                ]
                                            ), True),
                                            StructField('availableQuantityForColor', ArrayType(
                                                StructType(
                                                    [
                                                        StructField('Color', LongType(), True),
                                                        StructField('availableQuantity', LongType(), True)
                                                    ]
                                                ), True)
                                                , True),
                                        ]
                                    ), True),
                                    StructField('itemId', StringType(), True)
                                ]
                            ), True),
                        True)
                    ]
                ), True),
                StructField('supportedSystems', ArrayType(StringType(), True), True)
            ]
        )

        path = pkg_resources.resource_filename(__name__, "fixtures/add_operations_ancestor_struct_missing.json")
        df = self.spark.read.format('json').option("multiLine", True).schema(schema).load(path=path)

        # Availability struct is NULL and want to check if availableQuantityFilled gets filled correctly !
        processed_parent_null = add_nested_field(df,
                    "payload.lineItems.availability.availableQuantity",
                    "payload.lineItems.availability.availableQuantityFilled",
                    lambda column: F.coalesce(column, F.lit(0))
        )
        processed_line_items_dict = processed_parent_null.head().asDict()['payload']['lineItems']
        for i in range(2):
            self.assertEqual(processed_line_items_dict[i]["availability"]["availableQuantityFilled"], 0)

        # Availability struct is NULL and want to check if availableQuantityForSize.availableQuantityS gets filled correctly !
        processed_grand_parent_null = add_nested_field(df,
                    "payload.lineItems.availability.availableQuantityForSize.availableQuantityS",
                    "payload.lineItems.availability.availableQuantityForSize.availableQuantitySFilled",
                    lambda column: F.coalesce(column, F.lit(0))
        )
        processed_line_items_dict = processed_grand_parent_null.head().asDict()['payload']['lineItems']
        for i in range(2):
            self.assertEqual(processed_line_items_dict[i]["availability"]["availableQuantityForSize"]["availableQuantitySFilled"], 0)

        # Availability struct is NULL so child availableQuantityForColor array is NULL as well.
        # Want to check that don't add elements to the availableQuantityForColor array and keep it NULL !
        processed_grand_parent_null_array = add_nested_field(df,
                    "payload.lineItems.availability.availableQuantityForColor.availableQuantity",
                    "payload.lineItems.availability.availableQuantityForColor.availableQuantityFilled",
                    lambda column: F.coalesce(column, F.lit(0))
        )
        processed_line_items_dict = processed_grand_parent_null_array.head().asDict()['payload']['lineItems']
        for i in range(2):
            self.assertEqual(processed_line_items_dict[i]["availability"]["availableQuantityForColor"], None)