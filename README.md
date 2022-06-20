[![Python tests](https://github.com/golosegor/pyspark-nested-fields-functions/actions/workflows/python-tests.yml/badge.svg)](https://github.com/golosegor/pyspark-nested-fields-functions/actions/workflows/python-tests.yml)
[![PyPI version](https://badge.fury.io/py/pyspark-nested-functions.svg)](https://badge.fury.io/py/pyspark-nested-functions)

### Nested fields transformation for pyspark

## Motivation

Applying transformations for nested structures in spark is tricky.
Assuming we have JSON data with highly nested structure:

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

To hash nested "id" field you need to write following spark code

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

With the library the code above could be simplified to

```python
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from nestedfunctions.functions.terminal_operations import apply_terminal_operation

hashed = apply_terminal_operation(df, "data.city.addresses.id", lambda c, t: F.sha2(c.cast(StringType()), 256))
```

Instead of dealing of nested transformation functions you could specify terminal operation as 'lambda' and field
hierarchy in flat format and library will generate spark codebase for you.

## Install

To install the current release

```
$ pip install pyspark-nested-functions
```

## Available functions

#### Whitelist

Preserving all fields listed in parameters. All other fields will be dropped

```python
from nestedfunctions.functions.whitelist import whitelist

whitelisted_df = whitelist(df, ["addresses.postalCode", "creditCard"]) 
```

#### Drop

Recursively drop fields on any nested level (including child)

```python
from nestedfunctions.functions.drop import drop

processed = drop(df, field="root_level.children1.children2")
```

### Flattener

Return flattened representation of the data frame.

```python
from nestedfunctions.spark_schema.utility import SparkSchemaUtility

flatten_schema = SparkSchemaUtility().flatten_schema(df.schema)
# flatten_schema = ["root-element",
#                   "root-element-array-primitive",
#                   "root-element-array-of-structs.d1.d2",
#                   "nested-structure.n1",
#                   "nested-structure.d1.d2"]
```

### Nullify

Making field null on any nested level

```python
from nestedfunctions.functions.nullify import nullify

processed = nullify(df, field="creditCard.id")
```

## License

[Apache License 2.0](LICENSE)