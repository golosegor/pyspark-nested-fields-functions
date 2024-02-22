import logging
from typing import List

import pkg_resources

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *

from nestedfunctions.functions.fillna import fillna
from tests.unit.functions.spark_base_test import SparkBaseTest
from tests.unit.utils.testing_utils import parse_df_sample

log = logging.getLogger(__name__)


class FillNaTest(SparkBaseTest):

    def test_fillna_root(self):
        def parse_event_version(df: DataFrame) -> str:
            return df.select("eventVersion").collect()[0][0]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/fillna_sample.json"))
        self.assertEqual(parse_event_version(df), None)

        # Check if correctly filled
        processed = fillna(df, subset="eventVersion", value="1.0")
        self.assertEqual(parse_event_version(processed), "1.0")

    def test_fillna_root_array_is_null(self):
        def parse_test_array(df: DataFrame) -> List[str]:
            return df.select("test_array").collect()[0][0]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/fillna_sample.json"))

        test_df = df.withColumn("test_array", F.lit(None).cast("array<string>"))
        self.assertEqual(parse_test_array(test_df), None)

        # Check if filling of null array with empty array works
        try:
            processed = fillna(test_df, value={"test_array": []})
        except:
            # In Spark versions < 3.4.0 get below error when try F.lit(some_list)
            # org.apache.spark.SparkRuntimeException: The feature is not supported: literal for '[Automatically triggered stock check]'
            # of class java.util.ArrayList.
            return
        self.assertEqual(parse_test_array(processed), [])

    def test_fillna_null_inside_root_array(self):
        def parse_supported_systems(df: DataFrame) -> List[str]:
            return df.select("supportedSystems").collect()[0][0]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/fillna_sample.json"))
        self.assertEqual(parse_supported_systems(df), ['SAP_S4HANA_CLOUD', None])

        # Check if the null elements within an array are being filled correctly
        processed = fillna(df, subset="supportedSystems", value="UNKNOWN")
        self.assertEqual(parse_supported_systems(processed), ['SAP_S4HANA_CLOUD', "UNKNOWN"])

    def test_fillna_boolean_field_within_struct_within_nested_arrays(self):
        def parse_item_store_is_on_stock(df: DataFrame, item_index:str, store_index: int) -> bool:
            return df.select("payload.lineItems").collect()[0][0][item_index]["availability"]["stores"][store_index]["isOnStock"]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/fillna_sample.json"))
        self.assertEqual(parse_item_store_is_on_stock(df, item_index=0, store_index=0), True)
        self.assertEqual(parse_item_store_is_on_stock(df, item_index=0, store_index=1), None)
        self.assertEqual(parse_item_store_is_on_stock(df, item_index=1, store_index=0), True)
        self.assertEqual(parse_item_store_is_on_stock(df, item_index=1, store_index=1), None)

        # "payload.lineItems.availability.stores.isOnStock" is the only boolean field so will be the only one filled
        processed = fillna(df, value=False)
        self.assertEqual(parse_item_store_is_on_stock(processed, item_index=0, store_index=0), True)
        self.assertEqual(parse_item_store_is_on_stock(processed, item_index=0, store_index=1), False)
        self.assertEqual(parse_item_store_is_on_stock(processed, item_index=1, store_index=0), True)
        self.assertEqual(parse_item_store_is_on_stock(processed, item_index=1, store_index=1), False)

    def test_fillna_integer_field_within_struct_within_nested_arrays(self):
        def parse_item_store_available_quantity(df: DataFrame, item_index:str, store_index: int) -> int:
            return df.select("payload.lineItems").collect()[0][0][item_index]["availability"]["stores"][store_index]["availableQuantity"]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/fillna_sample.json"))
        self.assertEqual(parse_item_store_available_quantity(df, item_index=0, store_index=0), 1)
        self.assertEqual(parse_item_store_available_quantity(df, item_index=0, store_index=1), None)
        self.assertEqual(parse_item_store_available_quantity(df, item_index=1, store_index=0), 2)
        self.assertEqual(parse_item_store_available_quantity(df, item_index=1, store_index=1), None)

        processed = fillna(df, subset="payload.lineItems.availability.stores.availableQuantity", value=0)
        self.assertEqual(parse_item_store_available_quantity(processed, item_index=0, store_index=0), 1)
        self.assertEqual(parse_item_store_available_quantity(processed, item_index=0, store_index=1), 0)
        self.assertEqual(parse_item_store_available_quantity(processed, item_index=1, store_index=0), 2)
        self.assertEqual(parse_item_store_available_quantity(processed, item_index=1, store_index=1), 0)

    def test_fillna_null_element_within_array_of_arrays(self):
        def parse_comments_for_line_item(df: DataFrame, item_index:str) -> str:
            return df.select("payload.lineItems").collect()[0][0][item_index]["comments"]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/fillna_sample.json"))
        self.assertEqual(parse_comments_for_line_item(df, item_index=0), ['Manually triggered stock check', None])
        self.assertEqual(parse_comments_for_line_item(df, item_index=1), None)

        # To fill null element within array of arrays specify string as value
        processed = fillna(df, value={"payload.lineItems.comments" : "Empty comment"})
        self.assertEqual(parse_comments_for_line_item(processed, item_index=0), ['Manually triggered stock check', "Empty comment"])
        self.assertEqual(parse_comments_for_line_item(processed, item_index=1), None)

        # To fill array within array which is null specify list of strings as value
        try:
            processed = fillna(df, value={"payload.lineItems.comments" : ["Automatically triggered stock check"]})
        except:
            # In Spark versions < 3.4.0 get below error when try F.lit(some_list)
            # org.apache.spark.SparkRuntimeException: The feature is not supported: literal for '[Automatically triggered stock check]'
            # of class java.util.ArrayList.
            return
        self.assertEqual(parse_comments_for_line_item(processed, item_index=0), ['Manually triggered stock check', None])
        self.assertEqual(parse_comments_for_line_item(processed, item_index=1), ["Automatically triggered stock check"])

    def test_fillna_multiple_root_columns(self):
        def parse_event_version(df: DataFrame) -> str:
            return df.select("eventVersion").collect()[0][0]
        def parse_supported_systems(df: DataFrame) -> List[str]:
            return df.select("supportedSystems").collect()[0][0]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/fillna_sample.json"))
        self.assertEqual(parse_event_version(df), None)
        self.assertEqual(parse_supported_systems(df), ['SAP_S4HANA_CLOUD', None])

        # Check if the null elements within an array are being filled correctly
        processed = fillna(df, value={"supportedSystems":"UNKNOWN", "eventVersion":"1.0"})
        self.assertEqual(parse_event_version(processed), "1.0")
        self.assertEqual(parse_supported_systems(processed), ['SAP_S4HANA_CLOUD', "UNKNOWN"])

    def test_fillna_ancestor_struct_is_null(self):
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

        path = pkg_resources.resource_filename(__name__, "fixtures/fillna_ancestor_struct_missing.json")
        df = self.spark.read.format('json').option("multiLine", True).schema(schema).load(path=path)

        processed_parent_null = fillna(df,
                    value={
                        "payload.lineItems.availability.availableQuantity" : 0,
                        "payload.lineItems.availability.availableQuantityForSize.availableQuantityS" : 0,
                        "payload.lineItems.availability.availableQuantityForColor.availableQuantity" : 0,
                        }
        )

        processed_line_items_dict = processed_parent_null.head().asDict()['payload']['lineItems']
        # Availability struct is NULL and want to check if the to be filled fields get filled correctly !
        for i in range(2):
            # If parent of the to be filled field is NULL then the to be filled field should be filled with the specified value
            self.assertEqual(processed_line_items_dict[i]["availability"]["availableQuantity"], 0)
            # If the grand-parent of the to be filled field is NULL then the to be filled field should be filled with the specified value
            self.assertEqual(processed_line_items_dict[i]["availability"]["availableQuantityForSize"]["availableQuantityS"], 0)

            # If the ancestor of the to be filled field is NULL but the parent is an array the array should remain NULL.
            # We don't add elements to the availableQuantityForColor array and keep it NULL !
            self.assertEqual(processed_line_items_dict[i]["availability"]["availableQuantityForColor"], None)
