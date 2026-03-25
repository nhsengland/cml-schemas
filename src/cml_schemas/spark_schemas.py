from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType, BooleanType


METRIC_SCHEMA = StructType([
    StructField("datapoint_id",                     StringType(),    nullable=False),
    StructField("metric_id",                        StringType(),    nullable=False),
    StructField("metric_dimension_id",              StringType(),    nullable=False),
    StructField("dimension_cohort_id",              StringType(),    nullable=False),
    StructField("location_id",                      StringType(),    nullable=False),
    StructField("location_type",                    StringType(),    nullable=False),
    StructField("reporting_period_start_datetime",  TimestampType(), nullable=False),
    StructField("last_record_timestamp",            TimestampType(), nullable=False),
    StructField("last_ingest_timestamp",            TimestampType(), nullable=False),
    StructField("publication_date",                 TimestampType(), nullable=False),
    StructField("metric_value",                     IntegerType(),   nullable=True),
    StructField("additional_metric_values",         StringType(),    nullable=True),
])


DIMENSIONS_SCHEMA = StructType([
    StructField("dimension_cohort_id",  StringType(), nullable=False),
    StructField("metric_id",            StringType(), nullable=False),
])


_METRIC_VALUE_DTYPE_MAP = {
    "int":    IntegerType(),
    "float":  FloatType(),
    "string": StringType(),
    "bool":   BooleanType(),
}


def get_metric_schema(metric_value_dtype: str) -> StructType:
    if metric_value_dtype not in _METRIC_VALUE_DTYPE_MAP:
        raise ValueError(f"Unsupported metric_value_dtype '{metric_value_dtype}'. Must be one of: {list(_METRIC_VALUE_DTYPE_MAP)}")
    dtype = _METRIC_VALUE_DTYPE_MAP[metric_value_dtype]
    fields = [
        StructField(f.name, dtype if f.name == "metric_value" else f.dataType, nullable=f.nullable)
        for f in METRIC_SCHEMA.fields
    ]
    return StructType(fields)


def create_dimensions_schema(dimensions: list[str]) -> StructType:
    dimension_fields = [StructField(col, StringType(), nullable=True) for col in dimensions]
    return StructType(DIMENSIONS_SCHEMA.fields + dimension_fields)


def select_from_schema(df: DataFrame, schema: StructType) -> DataFrame:
    return df.select(*[field.name for field in schema.fields])


def validate_schema(df: DataFrame, schema: StructType) -> None:
    df_types = dict(df.dtypes)
    errors = []

    for field in schema.fields:
        col_name = field.name

        if col_name not in df_types:
            errors.append(f"  - '{col_name}': column is missing")
            continue

        actual = df_types[col_name]
        expected = field.dataType.simpleString()
        if actual != expected:
            errors.append(f"  - '{col_name}': expected {expected}, got {actual}")

    if errors:
        raise TypeError("Schema validation failed:\n" + "\n".join(errors))
