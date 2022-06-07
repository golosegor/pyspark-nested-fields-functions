from typing import List

from pyspark.sql import DataFrame

from nestedfunctions.processors.whitelist.whitelist_processor import WhitelistProcessor


def whitelist(df: DataFrame, fields: List[str]) -> DataFrame:
    return WhitelistProcessor(fields, ).process(df)
