from typing import Dict

import pkg_resources
import pytest
from pyspark.sql import DataFrame

from nestedfunctions.functions.date_format import format_date, format_date_with_predicate
from tests.unit.functions.spark_base_test import SparkBaseTest
from tests.unit.utils.testing_utils import parse_df_sample


class DateFormatProcessorTest(SparkBaseTest):

    def test_date_formatted_throws_exception_if_field_is_invalid(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/date_formatting_on_nested_fields.json"))
        with pytest.raises(Exception):
            format_date(df, "customDimensions.value$", "y-dd-MM", "y-MM")

    def test_date_formatted_in_array_with_predicated_on_root_level(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/date_formatting_on_nested_fields.json"))
        self.assertEqual({
            2: "2021-12-01",
            3: "2019-15-02",
            4: "2021-15-02"
        }, self.__parse_data_dateformatting(df))
        transformed = format_date_with_predicate(df, "customDimensions.value", "y-dd-MM", "y-MM", "index", "2")
        self.assertEqual({
            2: "2021-01",
            3: "2019-15-02",
            4: "2021-15-02"
        }, self.__parse_data_dateformatting(transformed))

    def test_date_formatted_no_predicate_test(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/date_formatting_on_nested_fields.json"))
        self.assertEqual(self.__parse_data_dateformatting(df), {
            2: "2021-12-01",
            3: "2019-15-02",
            4: "2021-15-02"
        })
        transformed = format_date(df, "customDimensions.value", "y-d-M", "y-MM")
        self.assertEqual({
            2: "2021-01",
            3: "2019-02",
            4: "2021-02"
        }, self.__parse_data_dateformatting(transformed))

    def test_date_formatted_no_predicate_on_array(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/date_formatting_on_array.json"))
        self.assertEqual(df.select("customDimensions.dates").collect()[0][0][0], ["2021-12-01","2019-15-02","2021-15-02"])
        transformed = format_date(df, "customDimensions.dates", "y-d-M", "y-MM")
        self.assertEqual(["2021-01","2019-02","2021-02"], transformed.select("customDimensions.dates").collect()[0][0][0])

    def test_date_formatted_null_remains_null(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/date_formatting_on_nested_fields_nulls.json"))
        self.assertEqual(self.__parse_data_dateformatting(df), {
            2: None,
            3: "2019-15-02",
        })
        transformed = format_date(df, "customDimensions.value", "y-d-M", "y-MM")
        self.assertEqual({
            2: None,
            3: "2019-02",
        }, self.__parse_data_dateformatting(transformed))

    def test_date_formatted_throws_exception_if_input_pattern_is_incorrect(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/date_formatting_on_nested_fields.json"))
        with pytest.raises(Exception):
            format_date(df, "customDimensions.value", "y/d/M", "not-used").collect()

    def test_date_formatted_throws_exception_if_output_pattern_is_incorrect(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__,
                                                             "fixtures/date_formatting_on_nested_fields.json"))
        with pytest.raises(Exception):
            format_date(df, "customDimensions.value", "y-d-M", "incorrect-pattern").collect()

    def __parse_data_dateformatting(self, df: DataFrame) -> Dict[str, str]:
        custom_dimensions = df.select("customDimensions").collect()[0]
        return {r['index']: r['value'] for r in custom_dimensions[0]}
