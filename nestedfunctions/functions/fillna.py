from typing import Any, Dict, List, Optional, Tuple, Union

from pyspark.sql import Column
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, AtomicType,  StructType
from pyspark.sql.types import _infer_type

from nestedfunctions.processors.any_level_processor import AnyLevelCoreProcessor
from nestedfunctions.spark_schema.utility import SparkSchemaUtility

# Below function mimics the vanilla pyspark fillna functionality with added support for filling nested fields
def fillna(
        df: DataFrame,
        value: Union[Any, Dict[str, Any]],
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> DataFrame:
        """
        Parameters
        ----------
        value : int, float, string, bool, list or dict
            Value to replace null values with.
            If the value is a dict, then `subset` is ignored and `value` must be a mapping
            from column name (string) to replacement value. The replacement value must be
            an int, float, boolean, or string.
        subset : str, tuple or list, optional
            optional list of column names to consider.

        Returns
        -------
        :class:`DataFrame`
            DataFrame with replaced null values.
        """
        if not isinstance(value, (float, int, str, bool, list, dict)):
            raise TypeError(f"value argument should be int, float, string, bool, list or dict and is {type(value)}")

        if isinstance(value, dict):
            for col_name, value_to_fill in value.items():
                if __col_name_can_be_filled_with_value(df.schema, col_name, value_to_fill):
                    df = FillNaProcessor(col_name, value_to_fill).process(df)
            return df
        elif subset is None:
            col_names = SparkSchemaUtility().flatten_schema(df.schema)
            for col_name in col_names:
                if __col_name_can_be_filled_with_value(df.schema, col_name, value):
                    df = FillNaProcessor(col_name, value).process(df)
            return df
        else:
            if isinstance(subset, str):
                subset = [subset]
            elif not isinstance(subset, (list, tuple)):
                raise TypeError(f"subset argument should be list or tuple and is {type(subset)}")
            for col_name in subset:
                if __col_name_can_be_filled_with_value(df.schema, col_name, value):
                    df = FillNaProcessor(col_name, value).process(df)
            return df

def __col_name_can_be_filled_with_value(schema: StructType, col_name: str, value: Any) -> bool:
    column_spark_type = SparkSchemaUtility.schema_for_field(schema, col_name)
    value_spark_type = _infer_type(value)
    if column_spark_type == value_spark_type:
        return True
    elif type(value_spark_type) is ArrayType and SparkSchemaUtility.is_array(schema, col_name):
        value_array_element_type = value_spark_type.simpleString().replace("array<","")[:-1] 
        if value_array_element_type == column_spark_type.simpleString():
            return True
        elif value_array_element_type.isin(["void", "null"]):
            # value is an empty list and col_name is an array so can fill col_name with value
            return True
        else:
            return False
    else:
        return False


class FillNaProcessor(AnyLevelCoreProcessor):

    def __init__(self, column_to_process: str, value: Any):
        super().__init__(column_to_process)
        self.value = value

    def __coalesce(self, primitive_value: Column) -> Column:
        return F.coalesce(primitive_value, F.lit(self.value))

    def apply_terminal_operation_on_root_level(self, df: DataFrame, column_name: str) -> DataFrame:
        field_type: AtomicType = SparkSchemaUtility.schema_for_field(df.schema, column_name)
        if SparkSchemaUtility.is_array(df.schema, column_name):
            if isinstance(self.value, list):
                # Fill the array when it's null
                return df.withColumn(column_name,
                                     F.when(F.col(column_name).isNull(), F.lit(self.value)).otherwise(F.col(column_name)))
            else:
                # Fill the element within the array when it's null
                return df.withColumn(column_name,
                                     F.transform(F.col(column_name), lambda d: self.__coalesce(d)))
        else:
            return df.withColumn(column_name, self.__coalesce(F.col(column_name)))

    def apply_terminal_operation_on_structure(self, schema: StructType, column: Column, column_name: str,
                                              previous: str) -> Column:
        # Previous is the full path to the field we want to fill while column_name is the last part of the name of this field
        # column can be the column corresponding with the parent of the field to fill
        # but can also be a transform / lambda combination applied to this parent.
        is_array = SparkSchemaUtility.is_array(schema, previous)
        if is_array:
            if isinstance(self.value, list):
                return column.withField(f"`{column_name}`",
                                    F.when(column.getField(column_name).isNull(), F.lit(self.value)).otherwise(column.getField(column_name)))
            else:
                return column.withField(f"`{column_name}`",
                                    F.transform(column.getField(column_name), lambda d: self.__coalesce(d)))
        else:
            return column.withField(f"`{column_name}`",
                                        self.__coalesce(column.getField(column_name)))
