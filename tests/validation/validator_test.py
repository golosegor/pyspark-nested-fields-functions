import unittest

from nestedfunctions.validation.validators import field_is_valid, regexp_is_valid


class FieldValidatorTest(unittest.TestCase):
    def test_predicate_processor_parse_input(self):
        self.assertFalse(field_is_valid(""))
        self.assertTrue(field_is_valid("d.d.d.d"))
        self.assertTrue(field_is_valid("d.d-aaa.d.d"))
        self.assertTrue(field_is_valid("d.d_aaa.d.d"))
        self.assertTrue(field_is_valid("hits.radar.tag.custom.CMA Brands"))
        illegal_symbols = [
            ',',
            ';',
            '{',
            '}',
            '(',
            ')',
            '\\'
        ]
        for illegal_symbol in illegal_symbols:
            self.assertFalse(field_is_valid(f"field.{illegal_symbol}"))
        self.assertTrue(field_is_valid("field_with_underscore"))


class RegexpValidationTest(unittest.TestCase):
    def test_regexp_validation(self):
        self.assertTrue(regexp_is_valid(".*"))
        self.assertFalse(regexp_is_valid("[ | 5"))
