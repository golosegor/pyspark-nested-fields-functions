import unittest

import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkBaseTest(unittest.TestCase):

    def __init__(self, methodName: str) -> None:
        super(SparkBaseTest, self).__init__(methodName=methodName)

    @pytest.fixture(autouse=True)
    def setup(self):
        self.spark = (
            SparkSession
            .builder
            .config(conf=SparkConf())
            .master('local[4]')
            .appName("base-test")
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("ERROR")

    def teardown(self):
        self.spark.stop()
