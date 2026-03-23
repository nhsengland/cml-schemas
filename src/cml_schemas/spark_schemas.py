from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


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
    StructField("metric_value",                     StringType(),    nullable=True),
    StructField("additional_metric_values",         StringType(),    nullable=True),
])


DIMENSIONS_SCHEMA = StructType([
    StructField("dimension_cohort_id",  StringType(), nullable=False),
    StructField("metric_id",            StringType(), nullable=False),
])


def create_dimensions_schema(dimensions: list[str]) -> StructType:
    dimension_fields = [StructField(col, StringType(), nullable=True) for col in dimensions]
    return StructType(DIMENSIONS_SCHEMA.fields + dimension_fields)


def select_from_schema(df: DataFrame, schema) -> DataFrame:
    return df.select(*[field.name for field in schema.fields])


def validate_schema(df: DataFrame, schema) -> None:
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
        