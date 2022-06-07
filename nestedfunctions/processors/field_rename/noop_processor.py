from pyspark.sql import DataFrame

from nestedfunctions.processors.coreprocessor import CoreProcessor


class NoOpProcessor(CoreProcessor):
    def process(self, df: DataFrame) -> DataFrame:
        return df
