import unittest

import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession

from nestedfunctions.functions.whitelist import SPARK_ENABLED_FORCE_RECALCULATION_ENV_VARIABLE_NAME


class SparkBaseTest(unittest.TestCase):

    def __init__(self, methodName: str) -> None:
        super(SparkBaseTest, self).__init__(methodName=methodName)

    @pytest.fixture(autouse=True)
    def setup(self):
        import os
        os.environ[SPARK_ENABLED_FORCE_RECALCULATION_ENV_VARIABLE_NAME] = str(True)
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
