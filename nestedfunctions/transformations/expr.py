import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column

from nestedfunctions.generic.rec_func import RecFunc


def expr(df: DataFrame, field: str, expr: str) -> DataFrame:
    return RecFunc(field=field, root_level_processor=lambda df, str: df.withColumn(field, __expression(expr)),
                   structure_operation=lambda type, column, ff, previous: column.withField(ff, __expression(expr))) \
        .process(df)


def __expression(expr: str) -> Column:
    return F.expr(expr)
