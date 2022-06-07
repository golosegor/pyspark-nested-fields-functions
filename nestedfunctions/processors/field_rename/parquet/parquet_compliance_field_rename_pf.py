from nestedfunctions.processors.field_rename.field_rename_processor import FieldRenameFunc


class ParquetComplianceFn(FieldRenameFunc):

    def convert_field_name(self, old_field_name: str) -> str:
        res = old_field_name
        problematic_chars = ',;{}()='
        for c in problematic_chars:
            res = res.replace(c, '')
        return res.replace(" ", "_")
