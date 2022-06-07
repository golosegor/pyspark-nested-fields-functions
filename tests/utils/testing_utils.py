from pyspark.sql import SparkSession, DataFrame


def parse_df_sample(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.load(path=path, **{'format': 'json', 'multiLine': 'true'})
