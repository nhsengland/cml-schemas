from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType, BooleanType


METRIC_SCHEMA = StructType([
    StructField("datapoint_id",                     StringType(),    nullable=False),
    StructField("metric_id",                        StringType(),    nullable=False),
    StructField("dimension_id",                     StringType(),    nullable=True),
    StructField("reporting_grain",                  StringType(),    nullable=False),
    StructField("location_id",                      StringType(),    nullable=False),
    StructField("location_type",                    StringType(),    nullable=False),
    StructField("reporting_period_start_datetime",  TimestampType(), nullable=False),
    StructField("publication_datetime",             TimestampType(), nullable=True),
    StructField("metric_value",                     IntegerType(),   nullable=False),
    StructField("additional_metric_values",         StringType(),    nullable=True),
])


SOURCE_SCHEMA = StructType([
    StructField("source_name",             StringType(),    nullable=False),
    StructField("project_id",              StringType(),    nullable=True),
    StructField("source_platform",         StringType(),    nullable=True),
    StructField("first_record_timestamp",  TimestampType(), nullable=False),
    StructField("last_record_timestamp",   TimestampType(), nullable=False),
    StructField("last_ingest_timestamp",   TimestampType(), nullable=False),
    StructField("version_release_date",    TimestampType(), nullable=False),
    StructField("version_release",         IntegerType(),   nullable=False),
])


DIMENSIONS_SCHEMA = StructType([
    StructField("dimension_id",       StringType(),  nullable=True),
    StructField("dimension_type_id",  StringType(),  nullable=True),
    StructField("dimension_names",    StringType(),  nullable=True),
    StructField("dimension_count",    IntegerType(), nullable=True),
])


RELATIONSHIPS_SCHEMA = StructType(
    [
        StructField("metric_id",                StringType(), nullable=False),
        StructField("metric_short_name",        StringType(), nullable=True),
        StructField("child_metric_id",          StringType(), nullable=False),
        StructField("child_metric_short_name",  StringType(), nullable=False),
        StructField("relationship_type",        StringType(), nullable=False),
        StructField("relationship_category",    StringType(), nullable=False),
        StructField("relationship_description", StringType(), nullable=False),
    ]
)


METADATA_SCHEMA = StructType(
    [
        StructField("metric_family_id",                             StringType(),    nullable=False),
        StructField("metric_id",                                    StringType(),    nullable=False),
        StructField("metric_status",                                StringType(),    nullable=False),
        StructField("metric_title",                                 StringType(),    nullable=False),
        StructField("metric_start_date",                            TimestampType(), nullable=False),
        StructField("metric_end_date",                              TimestampType(), nullable=True),
        StructField("available_metric_category",                    StringType(),    nullable=False),
        StructField("available_reporting_grain",                    StringType(),    nullable=False),
        StructField("available_location_types",                     StringType(),    nullable=False),
        StructField("available_dimension_types",                    StringType(),    nullable=True),
        StructField("business_area",                                StringType(),    nullable=False),
        StructField("business_sub_area",                            StringType(),    nullable=False),
        StructField("domain",                                       StringType(),    nullable=True),
        StructField("source_name",                                  StringType(),    nullable=False),
        StructField("project_id",                                   StringType(),    nullable=True),
        StructField("source_platform",                              StringType(),    nullable=True),
        StructField("metric_owner",                                 StringType(),    nullable=False),
        StructField("metric_short_name",                            StringType(),    nullable=True),
        StructField("metric_alias",                                 StringType(),    nullable=True),
        StructField("metric_description",                           StringType(),    nullable=False),
        StructField("calculation",                                  StringType(),    nullable=True),
        StructField("metric_purpose",                               StringType(),    nullable=False),
        StructField("unit",                                         StringType(),    nullable=False),
        StructField("format",                                       StringType(),    nullable=False),
        StructField("target",                                       StringType(),    nullable=True),
        StructField("frequency_of_refresh",                         StringType(),    nullable=False),
        StructField("aggregation",                                  StringType(),    nullable=False),
        StructField("statistical_disclosure_control_flag",          StringType(),    nullable=False),
        StructField("statistical_disclosure_control_description",   StringType(),    nullable=True),
        StructField("interpretation",                               StringType(),    nullable=True),
        StructField("inclusion_exclusion_rules",                    StringType(),    nullable=True),
        StructField("usage",                                        StringType(),    nullable=True),
        StructField("organisation_geog_type",                       StringType(),    nullable=True),
        StructField("footnotes",                                    StringType(),    nullable=True),
        StructField("notes",                                        StringType(),    nullable=True),
    ]
)


_METRIC_VALUE_DTYPE_MAP = {
    "int":    IntegerType(),
    "float":  FloatType(),
    "string": StringType(),
    "bool":   BooleanType(),
}


def get_metric_schema(metric_value_dtype: str) -> StructType:
    """Return a copy of METRIC_SCHEMA with the ``metric_value`` field cast to the requested dtype.

    Args:
        metric_value_dtype: The desired dtype for the ``metric_value`` column.
            Must be one of ``"int"``, ``"float"``, ``"string"``, or ``"bool"``.

    Returns:
        A ``StructType`` identical to ``METRIC_SCHEMA`` except that the
        ``metric_value`` field uses the Spark type corresponding to
        ``metric_value_dtype``.

    Raises:
        ValueError: If ``metric_value_dtype`` is not a supported dtype.
    """
    if metric_value_dtype not in _METRIC_VALUE_DTYPE_MAP:
        raise ValueError(f"Unsupported metric_value_dtype '{metric_value_dtype}'. Must be one of: {list(_METRIC_VALUE_DTYPE_MAP)}")
    dtype = _METRIC_VALUE_DTYPE_MAP[metric_value_dtype]
    fields = [
        StructField(f.name, dtype if f.name == "metric_value" else f.dataType, nullable=f.nullable)
        for f in METRIC_SCHEMA.fields
    ]
    return StructType(fields)


def create_dimensions_schema(dimensions: list[str]) -> StructType:
    """Extend DIMENSIONS_SCHEMA with additional nullable string columns.

    Args:
        dimensions: Column names to append to the base ``DIMENSIONS_SCHEMA`` fields.

    Returns:
        A ``StructType`` containing all fields from ``DIMENSIONS_SCHEMA`` followed
        by a nullable ``StringType`` field for each name in ``dimensions``.
    """
    dimension_fields = [StructField(col, StringType(), nullable=True) for col in dimensions]
    return StructType(DIMENSIONS_SCHEMA.fields + dimension_fields)


def select_from_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """Select only the columns defined in ``schema`` from ``df``, in schema order.

    Args:
        df: The source Spark DataFrame.
        schema: A ``StructType`` whose field names define the columns to select.

    Returns:
        A DataFrame containing only the columns present in ``schema``, ordered
        as they appear in ``schema``.
    """
    return df.select(*[field.name for field in schema.fields])


def validate_schema(df: DataFrame, schema: StructType) -> None:
    """Assert that ``df`` contains all columns defined in ``schema`` with matching types
    and that non-nullable fields contain no null values.

    Args:
        df: The Spark DataFrame to validate.
        schema: The expected ``StructType`` schema.

    Raises:
        TypeError: If any expected column is missing, has a mismatched type, or
            violates a non-nullable constraint. The error message lists all violations found.
    """
    df_types = dict(df.dtypes)
    errors = []

    non_nullable_to_check = []
    for field in schema.fields:
        col_name = field.name

        if col_name not in df_types:
            errors.append(f"  - '{col_name}': column is missing")
            continue

        actual = df_types[col_name]
        expected = field.dataType.simpleString()
        if actual != expected:
            errors.append(f"  - '{col_name}': expected {expected}, got {actual}")
            continue

        if not field.nullable:
            non_nullable_to_check.append(col_name)

    if non_nullable_to_check:
        null_counts = df.select(
            *[F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in non_nullable_to_check]
        ).collect()[0]
        for col_name in non_nullable_to_check:
            if null_counts[col_name] > 0:
                errors.append(f"  - '{col_name}': non-nullable field contains {null_counts[col_name]} null(s)")

    if errors:
        raise TypeError("Schema validation failed:\n" + "\n".join(errors))
