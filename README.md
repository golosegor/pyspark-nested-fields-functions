[![Python tests](https://github.com/golosegor/pyspark-nested-fields-functions/actions/workflows/python-tests.yml/badge.svg)](https://github.com/golosegor/pyspark-nested-fields-functions/actions/workflows/python-tests.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![PyPI version](https://img.shields.io/pypi/v/pyspark-nested-functions.svg)](https://pypi.org/project/pyspark-nested-functions/)
![PYPI - Downloads](https://static.pepy.tech/badge/pyspark-nested-functions)
![PYPI - Python Version](https://img.shields.io/pypi/pyversions/pyspark-nested-functions.svg)

### Nested fields transformation for pyspark

## Motivation

Applying transformations to nested structures is tricky in Spark.
Assume we have below nested JSON data:

```json
[
  {
    "data": {
      "city": {
        "addresses": [
          {
            "id": "my-id"
          },
          {
            "id": "my-id2"
          }
        ]
      }
    }
  }
]
```

To hash the nested `id` field you need to write the following PySpark code:

```python
import pyspark.sql.functions as F

hashed = df.withColumn("data",
                       (F.col("data")
                        .withField("city", F.col("data.city")
                                   .withField("addresses", F.transform("data.city.addresses",
                                                                       lambda c: c.withField("id",
                                                                                             F.sha2(c.getField("id"),
                                                                                                    256)))))))
```

With the library the code above can be simplified to:

```python
from nestedfunctions.functions.hash import hash_field
hashed = hash_field(df, "data.city.addresses.id", num_bits=256)
```

## Install

To install the current release

```
$ pip install pyspark-nested-functions
```

## Available functions

#### Add nested field

Adding a nested field called new_column_name based on a lambda function working on the column_to_process nested field.
Fields column_to_process and new_column_name need to have the same parent or be at the root!

```python
from nestedfunctions.functions.add_nested_field import add_nested_field
from pyspark.sql.functions import when
processed = add_nested_field(
      df,
      column_to_process="payload.array.booleanField",
      new_column_name="payload.array.booleanFieldAsString",
      f=lambda column: when(column, "Y").when(~column, "N").otherwise(""),
  )
```

#### Date Format

Format a nested date field from current_date_format to target_date_format.

```python
from nestedfunctions.functions.date_format import format_date
date_formatted_df = format_date(
      df,
      field="customDimensions.value",
      current_date_format="y-d-M",
      target_date_format="y-MM"
  )
```

#### Drop

Recursively drop fields on any nested level.

```python
from nestedfunctions.functions.drop import drop

dropped_df = drop(df, field="root_level.children1.children2")
```

#### Duplicate

Duplicate the nested field column_to_duplicate as duplicated_column_name.
Fields column_to_duplicate and duplicated_column_name need to have the same parent or be at the root!

```python
from nestedfunctions.functions.duplicate import duplicate
duplicated_df = duplicate(
      df,
      column_to_duplicate="payload.lineItems.comments",
      duplicated_column_name="payload.lineItems.commentsDuplicate"
  )
```

#### Expr

Add or overwrite a nested field based on an expression.

```python
from nestedfunctions.functions.expr import expr
field = "emails.unverified"
processed = expr(df, field=field, expr=f"transform({field}, x -> (upper(x)))")
```

#### Field Rename

Rename all the fields based on any rename function.

(If you only want to rename specific fields filter on them in your rename function)

```python
from nestedfunctions.functions.field_rename import rename
def capitalize_field_name(field_name: str) -> str:
  return field_name.upper()
renamed_df = rename(df, rename_func=capitalize_field_name())
```

#### Fillna

This function mimics the vanilla pyspark fillna functionality with added support for filling nested fields.
The use of the input parameters value and subset is exactly the same as for the vanilla pyspark implementation as described [here](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.fillna.html).

```python
from nestedfunctions.functions.fillna import fillna
# Fill all null boolean fields with False
filled_df = fillna(df, value=False)
# Fill nested field with value
filled_df = fillna(df, subset="payload.lineItems.availability.stores.availableQuantity", value=0)
# To fill array which is null specify list of values
filled_df = fillna(df, value={"payload.comments" : ["Automatically triggered stock check"]})
# To fill elements of array that are null specify single value
filled_df = fillna(df, value={"payload.comments" : "Empty comment"})
```

#### Flattener

Return the flattened representation of the dataframe's schema.

```python
from nestedfunctions.spark_schema.utility import SparkSchemaUtility

flattened_schema = SparkSchemaUtility().flatten_schema(df.schema)
# flattened_schema = ["root-element",
#                   "root-element-array-primitive",
#                   "root-element-array-of-structs.d1.d2",
#                   "nested-structure.n1",
#                   "nested-structure.d1.d2"]
```

#### Hash

Replace a nested field by its SHA-2 hash value.
By default the number of bits in the output hash value will be 256 but a different value can be set.

```python
from nestedfunctions.functions.hash import hash_field
hashed_df = hash_field(df, "data.city.addresses.id", num_bits=256)
```

#### Nullify

Making a field null on any nested level.

```python
from nestedfunctions.functions.nullify import nullify

nullified_df = nullify(df, field="creditCard.id")
```

#### Overwrite nested field

Overwrites a nested field based on a lambda function working on this nested field.

```python
from nestedfunctions.functions.terminal_operations import apply_terminal_operation
from pyspark.sql.functions import when
processed = apply_terminal_operation(
      df,
      field="payload.array.someBooleanField",
      f=lambda column, type: when(column, "Y").when(~column, "N").otherwise(""),
  )
```

#### Redact

Replace a field by the default value of its data type.
The default value of a data type is typically its min or max value and can be found [here](https://github.com/golosegor/pyspark-nested-fields-functions/blob/main/nestedfunctions/functions/redact.py#L29).

```python
from nestedfunctions.functions.redact import redact
redacted_df = redact(df, field="customDimensions.metabolicsConditions")
```

#### Whitelist

Preserving all fields listed in parameters. All other fields will be dropped

```python
from nestedfunctions.functions.whitelist import whitelist

whitelisted_df = whitelist(df, ["addresses.postalCode", "creditCard"]) 
```

## Predicate variations of above functions
Some of the above functions like `hash`, `nullify` and `date_format` have predicate variations.
For these variations you can specify a single `predicate_key`/ `predicate_value` pair for which the function will be run. This is mainly handy when you only want to adapt a nested value when one of the root columns has a specific value.

## License

[Apache License 2.0](LICENSE)