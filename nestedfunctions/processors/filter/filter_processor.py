import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from nestedfunctions.processors.coreprocessor import CoreProcessor


class FilterProcessor(CoreProcessor):
    def __init__(self, predicate: str):
        self.expr = predicate

    def process(self, df: DataFrame) -> DataFrame:
        return df.filter(F.expr(self.expr))
