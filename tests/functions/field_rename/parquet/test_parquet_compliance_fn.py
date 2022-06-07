import unittest

from nestedfunctions.processors.field_rename.parquet.parquet_compliance_field_rename_pf import ParquetComplianceFn


class ParquetComplianceFnTest(unittest.TestCase):
    def test_spaces_are_removed(self):
        converter = ParquetComplianceFn()
        self.assertEqual("field_with_space", converter.convert_field_name("field with space"))
        self.assertEqual("field_with_brace", converter.convert_field_name("field_with_brace}"))
        self.assertEqual("field_with_comma", converter.convert_field_name("field_with_comma,"))
