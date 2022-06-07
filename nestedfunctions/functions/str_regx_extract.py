from pyspark.sql import DataFrame

from nestedfunctions.processors.str_regexp_extract.str_regexp_extract import StringRegexpExtractProcessor
from nestedfunctions.validation.validators import validate_regexp_or_throw


def str_regx_extract(df: DataFrame, field: str, pattern: str, group_index_to_extract: int) -> DataFrame:
    return StringRegexpExtractProcessor(column_to_process=field,
                                        pattern=validate_regexp_or_throw(pattern),
                                        group_index_to_extract=group_index_to_extract).process(df)
