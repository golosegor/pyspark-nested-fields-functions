import logging
from typing import List, Any

import pkg_resources
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from nestedfunctions.functions.redact import redact, SPARK_TYPE_TO_REDACT_VALUE, column_name_with_dedicated_field_type
from nestedfunctions.utils.iterators.iterator_utils import flatten
from tests.unit.functions.spark_base_test import SparkBaseTest
from tests.unit.utils.testing_utils import parse_df_sample

log = logging.getLogger(__name__)
logging.getLogger(__name__).setLevel(logging.INFO)


class RedactTest(SparkBaseTest):

    def test_redact_root_level(self):
        def parse_data(df: DataFrame) -> List[str]:
            return [d[0] for d in df.select("root level string").collect()]

        df = self.parse_df()
        self.assertEqual(["yo"], parse_data(df))
        processed = redact(df, field="root level string")
        self.assertEqual([SPARK_TYPE_TO_REDACT_VALUE[StringType()]], parse_data(processed))

    def test_redact_nested_structure(self):
        def parse_data(df_to_extract: DataFrame) -> List[str]:
            return [d[0] for d in df_to_extract.select("customDimensions.Metabolics Conditions").collect()]

        df = self.parse_df()
        self.assertEqual([13, 2], flatten(parse_data(df)))
        processed = redact(df, field="customDimensions.Metabolics Conditions")
        primitive_value_type = df.schema["customDimensions"].dataType.elementType['Metabolics Conditions'].dataType
        expected_value = SPARK_TYPE_TO_REDACT_VALUE[primitive_value_type]
        self.assertEqual([expected_value, expected_value], flatten(parse_data(processed)))

    def test_redact_throws_exception_if_field_is_not_primitive(self):
        df = self.parse_df()
        with pytest.raises(Exception) as excinfo:
            process = redact(df, field="customDimensions")
            process.count()
            self.assertIn("Column", str(excinfo.value))
            self.assertIn("Only primitive types could be redacted", str(excinfo.value))

    def test_redact_does_not_throw_exception_if_field_does_not_exist(self):
        df = self.parse_df()
        process = redact(df, field="non-existing-field")
        self.assertEqual(df.collect(), process.collect())

    def parse_df(self) -> DataFrame:
        df = parse_df_sample(self.spark,
                             pkg_resources.resource_filename(__name__, "fixtures/redact_sample.json"))
        return df

    def test_default_redacted_values_has_no_casting_issues(self):
        column_to_experiment_with = "root level string"

        def parse_data(df_to_parse: DataFrame) -> Any:
            return [d[0] for d in df_to_parse.select(column_to_experiment_with).collect()][0]

        df = self.parse_df()
        for spark_atomic_type in SPARK_TYPE_TO_REDACT_VALUE.keys():
            try:
                log.info(f"Processing type `{spark_atomic_type}`")
                processed = df.withColumn(column_to_experiment_with,
                                          column_name_with_dedicated_field_type(spark_atomic_type))
                self.assertEqual(parse_data(processed), SPARK_TYPE_TO_REDACT_VALUE[spark_atomic_type])
                self.assertEqual(spark_atomic_type, processed.schema[column_to_experiment_with].dataType,
                                 "Failed to cast type")
                log.info(f"Type `{spark_atomic_type}` successfully processed")
            except Exception as e:
                log.error(f"An exception occurred during processing type {spark_atomic_type}", e)
