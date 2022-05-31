import logging
from typing import List

import pkg_resources
from pyspark.sql import DataFrame

from nestedfunctions.transformations.expr import expr
from nestedfunctions.utils.iterators.iterator_utils import flatten
from tests.utils.spark_base_test import SparkBaseTest, parse_df_sample

log = logging.getLogger(__name__)


class ExprTest(SparkBaseTest):

    def test_validate_email_with_regex_and_lower_email_field(self):
        def parse_data(df: DataFrame) -> List[str]:
            return [d["email"] for d in df.select("email").collect()]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/exp_sample_01.json"))
        self.assertEqual([''' Fuat@hotmail.com\t''', '''test Egor@hotmail.com\n'''], parse_data(df))
        processed = expr(df=df, field="email", expr=r"lower(regexp_extract(email, '(\\S+@\\S+)', 1))")

        self.assertEqual(["fuat@hotmail.com", "egor@hotmail.com"], parse_data(processed))

    def test_validate_email_with_regex_and_lower_email_field_using_struct(self):
        def parse_data(df: DataFrame) -> List[str]:
            return [d["email"] for d in df.select("profile.email").alias('email').collect()]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/exp_sample_03.json"))

        self.assertEqual([''' Fuat@hotmail.com\t''', '''test Egor@hotmail.com\n'''], parse_data(df))
        processed = expr(df, field="profile.email", expr=r"lower(regexp_extract(profile.email, '(\\S+@\\S+)', 1))")

        self.assertEqual(["fuat@hotmail.com", "egor@hotmail.com"], parse_data(processed))

    def test_expr_nested_array(self):
        field = "emails.unverified"

        def parse_data(df: DataFrame) -> List[str]:
            return flatten([d[0] for d in df.select("emails.unverified").collect()])

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/nested_array_sample.json"))
        self.assertEqual(["egorka@gmail.com\t", "juan-miguel@gmail.com\t"], parse_data(df))

        processed = expr(df, field=field, expr=f"transform({field}, x -> (upper(x)))")

        self.assertEqual(["EGORKA@GMAIL.COM\t", "JUAN-MIGUEL@GMAIL.COM\t"], parse_data(processed))

    def test_expr_on_root_level(self):
        def parse_data(df: DataFrame) -> List[str]:
            return [d[0] for d in df.select("email").collect()]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/exp_sample_01.json"))
        self.assertEqual([' Fuat@hotmail.com\t', 'test Egor@hotmail.com\n'], parse_data(df))

        processed = expr(df, field="email", expr=f"upper(email)")

        self.assertEqual([' FUAT@HOTMAIL.COM\t', 'TEST EGOR@HOTMAIL.COM\n'], parse_data(processed))
