import pkg_resources
import pytest

from nestedfunctions.functions.binary_generalization import binary_generalization
from tests.unit.functions.spark_base_test import SparkBaseTest
from tests.unit.utils.testing_utils import parse_df_sample


class BinaryGeneralizationProcessorTest(SparkBaseTest):

    def test_data_could_be_generalized_if_null(self):
        df = self.__parse_original_sample()
        null_field = df.collect()[0]["nullField"]
        self.assertEqual(null_field, None)
        processed = binary_generalization(df, field="nullField")
        self.assertEqual(False, processed.collect()[0]["nullField"])

    def test_data_could_be_generalized_if_empty(self):
        df = self.__parse_original_sample()
        empty_field = df.collect()[0]["emptyField"]
        self.assertEqual(empty_field, "")
        processed = binary_generalization(df, field="emptyField")
        self.assertEqual(False, processed.collect()[0]["emptyField"])

    def test_data_could_be_generalized_if_not_empty(self):
        df = self.__parse_original_sample()
        non_empty_field = df.collect()[0]["nonEmptyField"]
        self.assertEqual(non_empty_field, "1234")
        processed = binary_generalization(df, field="nonEmptyField")
        self.assertEqual(True, processed.collect()[0]["nonEmptyField"])

    def test_binary_processor_factory_throw_exception_in_case_column_contains_illegal_charactes(self):
        df = self.__parse_original_sample()
        with pytest.raises(Exception):
            binary_generalization(df, "field_name_with_illegal_character$")

    def __parse_original_sample(self):
        return parse_df_sample(self.spark,
                               pkg_resources.resource_filename(__name__,
                                                               "fixtures/binary-generalization.json"))
