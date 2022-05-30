import unittest

import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame


class SparkBaseTest(unittest.TestCase):

    def __init__(self, methodName: str) -> None:
        super(SparkBaseTest, self).__init__(methodName=methodName)

    @pytest.fixture(autouse=True)
    def setup(self):
        conf = (SparkConf()
                # .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
                )
        self.spark = (SparkSession
                      .builder
                      .config(conf=conf)
                      .master('local[4]')
                      .appName("base-test")
                      .getOrCreate())

        self.spark.sparkContext.setLogLevel("ERROR")

    def teardown(self):
        self.spark.stop()


def parse_df_sample(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.load(path=path, **{'format': 'json', 'multiLine': 'true'})
