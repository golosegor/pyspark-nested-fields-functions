import logging

import pkg_resources
import pyspark.sql.functions as F
import pytest
from pyspark.sql import DataFrame

from nestedfunctions.functions.str_regx_extract import str_regx_extract
from tests.unit.functions.spark_base_test import SparkBaseTest
from tests.unit.utils.testing_utils import parse_df_sample

log = logging.getLogger(__name__)


class StringRegexpExtractTest(SparkBaseTest):

    def test_regexp_extract_working_fine(self):
        def parse_data(df: DataFrame) -> str:
            df = df.select(F.explode("data.analytics").alias("analytics"))
            return df.select("analytics._ga").collect()[0][0]

        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/str_concat_sample.json"))
        self.assertEqual(parse_data(df), "GA1.1.747308676.1625645535")
        processed = str_regx_extract(df, field="data.analytics._ga",
                                     pattern=r"(.*\..*)\.(.*.\..*)",
                                     group_index_to_extract=2)
        self.assertEqual("747308676.1625645535", parse_data(processed))

    def test_factory_throws_if_regexp_is_not_valid(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/str_concat_sample.json"))
        with pytest.raises(ValueError):
            str_regx_extract(df, "field_name", "[", 5)

    def test_when_field_is_not_found_then_calculation_ignored(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/str_concat_sample.json"))
        processed = str_regx_extract(df, "analytics._ga", r"(.*\..*)\.(.*.\..*)", 2)
        self.assertEqual(processed.collect(), df.collect())

    def test_regexp_extractor_processor_factory_throws_exception_if_field_is_invalid(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/str_concat_sample.json"))
        with pytest.raises(Exception):
            str_regx_extract(df, "analytics._ga$ ", r"(.*\..*)\.(.*.\..*)", 2)
