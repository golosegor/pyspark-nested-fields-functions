import logging

from pyspark.sql.dataframe import DataFrame

log = logging.getLogger(__name__)


class CoreProcessor:
    def process(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError("THIS METHOD MUST BE OVERRIDDEN ACCORDING TO DOCUMENTATION.")
