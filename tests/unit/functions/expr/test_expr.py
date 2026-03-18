import logging
from typing import List

import pkg_resources
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import *

from nestedfunctions.functions.expr import expr
from nestedfunctions.utils.iterators.iterator_utils import flatten
from tests.unit.functions.spark_base_test import SparkBaseTest
from tests.unit.utils.testing_utils import parse_df_sample

log = logging.getLogger(__name__)


class ExprTest(SparkBaseTest):

    def test_validate_email_with_regex_and_lower_email_field(self):
        def parse_data(df: DataFrame, col_name: str) -> List[str]:
            return [d[col_name] for d in df.select(col_name).collect()]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/exp_sample_01.json"))
        self.assertEqual([''' Fuat@hotmail.com\t''', '''test Egor@hotmail.com\n'''], parse_data(df, "email"))

        # Test in place expr
        processed = expr(df, field="email", expr=r"lower(regexp_extract(email, '(\\S+@\\S+)', 1))")
        self.assertEqual(["fuat@hotmail.com", "egor@hotmail.com"], parse_data(processed, "email"))
        # Test expr that adds field
        processed = expr(df, field="email_cleansed", expr=r"lower(regexp_extract(email, '(\\S+@\\S+)', 1))")
        self.assertEqual(["fuat@hotmail.com", "egor@hotmail.com"], parse_data(processed, "email_cleansed"))

    def test_validate_email_with_regex_and_lower_email_field_using_struct(self):
        def parse_data(df: DataFrame, col_name: str) -> List[str]:
            return [d[col_name] for d in df.select(f"profile.{col_name}").alias(col_name).collect()]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/exp_sample_03.json"))
        self.assertEqual([''' Fuat@hotmail.com\t''', '''test Egor@hotmail.com\n'''], parse_data(df, "email"))

        # Test in place expr
        processed = expr(df, "profile.email", r"lower(regexp_extract(profile.email, '(\\S+@\\S+)', 1))")
        self.assertEqual(["fuat@hotmail.com", "egor@hotmail.com"], parse_data(processed, "email"))
        # Test expr that adds field
        processed = expr(df, "profile.email_cleansed", r"lower(regexp_extract(profile.email, '(\\S+@\\S+)', 1))")
        self.assertEqual(["fuat@hotmail.com", "egor@hotmail.com"], parse_data(processed, "email_cleansed"))

    def test_expr_nested_array(self):
        field = "emails.unverified"

        def parse_data(df: DataFrame, col_name: str) -> List[str]:
            return flatten([d[0] for d in df.select(f"emails.{col_name}").collect()])

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/nested_array_sample.json"))
        self.assertEqual(["egorka@gmail.com\t", "juan-miguel@gmail.com\t"], parse_data(df, "unverified"))

        # Test in place expr
        processed = expr(df, field, f"transform({field}, x -> (upper(x)))")
        self.assertEqual(["EGORKA@GMAIL.COM\t", "JUAN-MIGUEL@GMAIL.COM\t"], parse_data(processed, "unverified"))
        # Test expr that adds field
        processed = expr(df, "emails.unverified_upper", f"transform({field}, x -> (upper(x)))")
        self.assertEqual(["EGORKA@GMAIL.COM\t", "JUAN-MIGUEL@GMAIL.COM\t"], parse_data(processed, "unverified_upper"))

    def test_expr_on_root_level(self):
        def parse_data(df: DataFrame, col_name: str) -> List[str]:
            return [d[0] for d in df.select(col_name).collect()]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/exp_sample_01.json"))
        self.assertEqual([' Fuat@hotmail.com\t', 'test Egor@hotmail.com\n'], parse_data(df, "email"))

        # Test in place expr
        processed = expr(df, "email", f"upper(email)")
        self.assertEqual([' FUAT@HOTMAIL.COM\t', 'TEST EGOR@HOTMAIL.COM\n'], parse_data(processed, "email"))
        # Test expr that adds field
        processed = expr(df, "email_upper", f"upper(email)")
        self.assertEqual([' FUAT@HOTMAIL.COM\t', 'TEST EGOR@HOTMAIL.COM\n'], parse_data(processed, "email_upper"))

    def test_expr_factory_throws_exception_if_expr_is_empty(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/exp_sample_03.json"))
        with pytest.raises(ValueError):
            expr(df, field="userId", expr="")

    def test_expr_factory_throws_exception_if_non_existing_field_and_parent_of_field(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/exp_sample_01.json"))
        with pytest.raises(ValueError):
            expr(df, field="profile.email", expr="'literalValue'")

    def test_expr_ancestor_struct_is_null(self):
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

        path = pkg_resources.resource_filename(__name__, "fixtures/expr_ancestor_struct_missing.json")
        df = self.spark.read.format('json').option("multiLine", True).schema(schema).load(path=path)

        processed_parent_null = expr(
            df=df,
            field="payload.lineItems.availability.dummyField",
            expr="0"
        )
        processed_line_items_dict = processed_parent_null.head().asDict()['payload']['lineItems']
        # If parent of the to be filled field is NULL then the to be filled field should be filled with the specified value
        for i in range(2):
            self.assertEqual(processed_line_items_dict[i]["availability"]["dummyField"], 0)

        processed_grand_parent_null = expr(
            df=df,
            field="payload.lineItems.availability.availableQuantityForSize.dummyField",
            expr="0"
        )
        processed_line_items_dict = processed_grand_parent_null.head().asDict()['payload']['lineItems']
       # If the grand-parent of the to be filled field is NULL then the to be filled field should be filled with the specified value
        for i in range(2):
            self.assertEqual(processed_line_items_dict[i]["availability"]["availableQuantityForSize"]["dummyField"], 0)

        processed_grand_parent_null_array = expr(
            df=df,
            field="payload.lineItems.availability.availableQuantityForColor.dummyField",
            expr="0"
        )
        processed_line_items_dict = processed_grand_parent_null_array.head().asDict()['payload']['lineItems']
        # If the ancestor of the to be filled field is NULL but the parent is an array the array should remain NULL.
        # We don't add elements to the availableQuantityForColor array and keep it NULL !
        for i in range(2):
            self.assertEqual(processed_line_items_dict[i]["availability"]["availableQuantityForColor"], None)
