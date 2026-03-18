import logging
from typing import List

import pkg_resources
from pyspark.sql import DataFrame

from nestedfunctions.functions.filter import df_filter
from tests.unit.functions.spark_base_test import SparkBaseTest
from tests.unit.utils.testing_utils import parse_df_sample

log = logging.getLogger(__name__)


class FilterTest(SparkBaseTest):
    @staticmethod
    def __parse_data(df: DataFrame) -> List[str]:
        return [d["userId"] for d in df.select("userId").collect()]

    def test_data_could_be_filtered(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/filter_sample.json"))
        self.assertEqual(self.__parse_data(df), [1, 2])
        processed = df_filter(df, 'isActive == true')
        self.assertEqual(self.__parse_data(processed), [1])

    def test_data_could_be_filtered_string_case(self):
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/filter_sample.json"))
        self.assertEqual(self.__parse_data(df), [1, 2])
        processed = df_filter(df, 'CRY_DSC = "GB"')
        self.assertEqual(self.__parse_data(processed), [1])
