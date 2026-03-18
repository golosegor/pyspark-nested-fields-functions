import logging
import json

from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType, TimestampType

from nestedfunctions.functions.drop import drop, DropProcessor
from nestedfunctions.spark_schema.utility import SparkSchemaUtility
from tests.unit.functions.spark_base_test import SparkBaseTest

log = logging.getLogger(__name__)


class DropTest(SparkBaseTest):
    COMPLEX_SCHEMA = StructType(
            [
                StructField('creditCard', StringType(), True),
                StructField('addresses', 
                    ArrayType(
                        StructType(
                            [
                                StructField('postalCode', StringType(), True),
                                StructField('mailboxes', 
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField("box_number", IntegerType(), True),
                                                        StructField("suffix", StringType(), True)
                                                    ]
                                                )
                                                ,True)
                                        , True),
                                StructField('zipCode', StringType(), True),
                            ]
                        ),True)
                    ,True),
            ]
        )

    def test_drop_parent_because_children_dropped(self):
        schema = StructType(
            [
                StructField('UID', StringType(), True),
                StructField('created', TimestampType(), True),
                StructField('identities', 
                    StructType(
                        [
                            StructField('providerUID', StringType(), True),
                        ]
                    ),True),
            ]
        )
        fields_to_drop = DropProcessor.consolidate_fields_to_drop(
            dataframe_schema=schema,
            fields_to_drop=["identities.providerUID"])
        self.assertEqual(["identities"], fields_to_drop)

        empty_df = self.spark.createDataFrame([],schema)
        expected_schema = empty_df.select("UID", "created").schema
        dropped_df = drop(empty_df, ["identities.providerUID"])
        self.assertEqual(dropped_df.schema, expected_schema)

    def test_only_drop_single_field(self):
        fields_to_drop = DropProcessor.consolidate_fields_to_drop(
            dataframe_schema=self.COMPLEX_SCHEMA,
            fields_to_drop=["addresses.postalCode",
                       "addresses.postalCode",
                       "non-existing"])
        self.assertEqual(["addresses.postalCode"], fields_to_drop)

        empty_df = self.spark.createDataFrame([],self.COMPLEX_SCHEMA)
        json_str_without_postal_code = (
            self.COMPLEX_SCHEMA.json()
            .replace('{"metadata":{},"name":"postalCode","nullable":true,"type":"string"},','')
        )
        expected_schema = StructType.fromJson(json.loads(json_str_without_postal_code))
        dropped_df = drop(empty_df, ["addresses.postalCode","addresses.postalCode","non-existing"])
        self.assertEqual(dropped_df.schema, expected_schema)

    def test_drop_all_fields(self):
        flattened_fields = SparkSchemaUtility().flatten_schema(self.COMPLEX_SCHEMA)
        fields_to_drop = DropProcessor.consolidate_fields_to_drop(
            dataframe_schema=self.COMPLEX_SCHEMA,
            fields_to_drop=flattened_fields)
        self.assertEqual(sorted(["creditCard", "addresses"]), sorted(fields_to_drop))

        empty_df = self.spark.createDataFrame([],self.COMPLEX_SCHEMA)
        dropped_df = drop(empty_df, flattened_fields)
        self.assertEqual(dropped_df.schema, StructType([]))

    def test_drop_parent_and_one_of_multiple_children(self):
        fields_to_drop = DropProcessor.consolidate_fields_to_drop(
            dataframe_schema=self.COMPLEX_SCHEMA,
            fields_to_drop=["addresses.postalCode",
                       "addresses.postalCode",
                       "addresses",
                       "non-existing"])
        self.assertEqual(["addresses"], fields_to_drop)

        empty_df = self.spark.createDataFrame([],self.COMPLEX_SCHEMA)
        expected_schema = empty_df.drop("addresses").schema
        dropped_df = drop(empty_df, 
                          ["addresses.postalCode","addresses.postalCode","addresses","non-existing"])
        self.assertEqual(dropped_df.schema, expected_schema)

    def test_drop_ripples_through(self):
        schema = StructType(
            [
                StructField('hits',
                    StructType(
                        [
                            StructField('pages',
                                ArrayType(
                                    StructType(
                                        [
                                            StructField('pagePathLevel1', StringType(), True),
                                        ]
                                    ),True)
                            ,True),
                        ]
                    )
                ,True)
            ]
        )
        
        fields_to_drop = DropProcessor.consolidate_fields_to_drop(
            dataframe_schema=schema,
            fields_to_drop=["hits.pages.hostname",
                       "hits.pages.pagePath",
                       "hits.pages.pagePathLevel1"])
        self.assertEqual(["hits"], fields_to_drop)

        empty_df = self.spark.createDataFrame([],schema)
        expected_schema = empty_df.drop("hits").schema
        dropped_df = drop(empty_df, 
                          ["hits.pages.hostname","hits.pages.pagePath","hits.pages.pagePathLevel1"])
        self.assertEqual(dropped_df.schema, expected_schema)

    def test_drop_parent_iso_all_childs(self):
        fields_to_drop = DropProcessor.consolidate_fields_to_drop(
            dataframe_schema=self.COMPLEX_SCHEMA,
            fields_to_drop=["creditCard",
                        "addresses.mailboxes.box_number",
                       "addresses.mailboxes.suffix"])
        self.assertEqual({"creditCard", "addresses.mailboxes"}, set(fields_to_drop))

        empty_df = self.spark.createDataFrame([],self.COMPLEX_SCHEMA)
        json_str_without_creditcard_and_mailboxes = (
            empty_df.drop("creditCard").schema.json()
            .replace('{"metadata":{},"name":"mailboxes","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"box_number","nullable":true,"type":"integer"},{"metadata":{},"name":"suffix","nullable":true,"type":"string"}],"type":"struct"},"type":"array"}},','')
        )
        expected_schema = StructType.fromJson(json.loads(json_str_without_creditcard_and_mailboxes))
        dropped_df = drop(empty_df, ["creditCard","addresses.mailboxes.box_number","addresses.mailboxes.suffix"])
        self.assertEqual(dropped_df.schema, expected_schema)

    def test_drop_parent_and_field_at_same_level(self):
        fields_to_drop = DropProcessor.consolidate_fields_to_drop(
            dataframe_schema=self.COMPLEX_SCHEMA,
            fields_to_drop=["addresses.zipCode",
                        "addresses.mailboxes.box_number",
                       "addresses.mailboxes.suffix"])
        self.assertEqual({"addresses.zipCode", "addresses.mailboxes"}, set(fields_to_drop))

        empty_df = self.spark.createDataFrame([],self.COMPLEX_SCHEMA)
        json_str_without_zipcode_and_mailboxes = (
            self.COMPLEX_SCHEMA.json()
            .replace(',{"metadata":{},"name":"zipCode","nullable":true,"type":"string"}','')
            .replace(',{"metadata":{},"name":"mailboxes","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"box_number","nullable":true,"type":"integer"},{"metadata":{},"name":"suffix","nullable":true,"type":"string"}],"type":"struct"},"type":"array"}}','')
        )
        expected_schema = StructType.fromJson(json.loads(json_str_without_zipcode_and_mailboxes))
        dropped_df = drop(empty_df, ["addresses.zipCode","addresses.mailboxes.box_number","addresses.mailboxes.suffix"])
        self.assertEqual(dropped_df.schema, expected_schema)
    
    def test_drop_while_walking_tree(self):
        fields_to_drop = DropProcessor.consolidate_fields_to_drop(
            dataframe_schema=self.COMPLEX_SCHEMA,
            fields_to_drop=["addresses.zipCode","addresses.mailboxes.suffix"])
        self.assertEqual({"addresses.zipCode", "addresses.mailboxes.suffix"}, set(fields_to_drop))

        empty_df = self.spark.createDataFrame([],self.COMPLEX_SCHEMA)
        json_str_without_zipcode_and_mailboxes_suffix = (
            self.COMPLEX_SCHEMA.json()
            .replace(',{"metadata":{},"name":"zipCode","nullable":true,"type":"string"}','')
            .replace(',{"metadata":{},"name":"suffix","nullable":true,"type":"string"}','')
        )
        expected_schema = StructType.fromJson(json.loads(json_str_without_zipcode_and_mailboxes_suffix))
        dropped_df = drop(empty_df, ["addresses.zipCode","addresses.mailboxes.suffix"])
        self.assertEqual(dropped_df.schema, expected_schema)
    
    def test_drop_3_levels_deep(self):
        fields_to_drop = DropProcessor.consolidate_fields_to_drop(
            dataframe_schema=self.COMPLEX_SCHEMA,
            fields_to_drop=["addresses.mailboxes.suffix"])
        self.assertEqual(["addresses.mailboxes.suffix"], fields_to_drop)

        empty_df = self.spark.createDataFrame([],self.COMPLEX_SCHEMA)
        json_str_without_mailboxes_suffix = (
            self.COMPLEX_SCHEMA.json()
            .replace(',{"metadata":{},"name":"suffix","nullable":true,"type":"string"}','')
        )
        expected_schema = StructType.fromJson(json.loads(json_str_without_mailboxes_suffix))
        dropped_df = drop(empty_df, ["addresses.mailboxes.suffix"])
        self.assertEqual(dropped_df.schema, expected_schema)
