import pytest
import datetime

from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, IntegerType, FloatType, BooleanType
)

from cml_schemas import spark_schemas

DIMENSIONS = ["AgeAtBookingMotherAvg", "BirthweightTermGroup", "TotalDeliveries"]
SCHEMA = StructType([
    StructField("id",         StringType(),    nullable=False),
    StructField("created_at", TimestampType(), nullable=False),
    StructField("label",      StringType(),    nullable=True),
])


def test_create_dimensions_schema_contains_base_fields():
    schema = spark_schemas.create_dimensions_schema(DIMENSIONS)
    field_names = [f.name for f in schema.fields]

    assert "dimension_cohort_id" in field_names
    assert "metric_id" in field_names


def test_create_dimensions_schema_contains_dimension_fields():
    schema = spark_schemas.create_dimensions_schema(DIMENSIONS)
    field_names = [f.name for f in schema.fields]

    for col in DIMENSIONS:
        assert col in field_names


def test_create_dimensions_schema_base_fields_come_first():
    schema = spark_schemas.create_dimensions_schema(DIMENSIONS)
    field_names = [f.name for f in schema.fields]

    base_field_names = [f.name for f in spark_schemas.DIMENSIONS_SCHEMA.fields]
    assert field_names[:len(base_field_names)] == base_field_names


def test_create_dimensions_schema_dimension_fields_are_nullable_strings():
    schema = spark_schemas.create_dimensions_schema(DIMENSIONS)
    dimension_fields = {f.name: f for f in schema.fields}

    for col in DIMENSIONS:
        assert isinstance(dimension_fields[col].dataType, StringType)
        assert dimension_fields[col].nullable is True


def test_create_dimensions_schema_does_not_mutate_base_schema():
    """
    test to make sure that create_dimensions_schema(DIMENSIONS) does
    not accidentally mutate DIMENSIONS_SCHEMA in-place.
    """
    before = [f.name for f in spark_schemas.DIMENSIONS_SCHEMA.fields]
    spark_schemas.create_dimensions_schema(DIMENSIONS)
    after = [f.name for f in spark_schemas.DIMENSIONS_SCHEMA.fields]

    assert before == after


# --- select_from_schema ---

def test_select_from_schema_returns_correct_columns(spark):
    test_data = [("1", datetime.datetime(2024, 1, 1), "a", "extra")]
    df = spark.createDataFrame(test_data, ["id", "created_at", "label", "unwanted"])

    result = spark_schemas.select_from_schema(df, SCHEMA)

    assert result.columns == ["id", "created_at", "label"]
    assert "unwanted" not in result.columns


def test_select_from_schema_preserves_column_order(spark):
    test_data = [("a", datetime.datetime(2024, 1, 1), "x")]
    # Create df with columns in a different order
    df = spark.createDataFrame(test_data, ["label", "created_at", "id"])

    result = spark_schemas.select_from_schema(df, SCHEMA)

    assert result.columns == ["id", "created_at", "label"]


# --- validate_schema ---

def test_validate_schema_passes_for_matching_schema(spark):
    test_data = [("1", datetime.datetime(2024, 1, 1), "a")]
    df = spark.createDataFrame(test_data, ["id", "created_at", "label"])

    spark_schemas.validate_schema(df, SCHEMA)  # should not raise


def test_validate_schema_raises_on_wrong_type(spark):
    wrong_schema = StructType([
        StructField("id",         StringType()),
        StructField("created_at", StringType()),  # should be timestamp
        StructField("label",      StringType()),
    ])
    test_data = [("1", "2024-01-01", "a")]
    df = spark.createDataFrame(test_data, wrong_schema)

    with pytest.raises(TypeError, match="created_at"):
        spark_schemas.validate_schema(df, SCHEMA)


def test_validate_schema_raises_on_missing_column(spark):
    test_data = [("1", datetime.datetime(2024, 1, 1))]
    df = spark.createDataFrame(test_data, ["id", "created_at"])  # label missing

    with pytest.raises(TypeError, match="label"):
        spark_schemas.validate_schema(df, SCHEMA)


def test_validate_schema_raises_on_null_in_non_nullable_field(spark):
    nullable_schema = StructType([
        StructField("id",         StringType(),    nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("label",      StringType(),    nullable=True),
    ])
    test_data = [(None, datetime.datetime(2024, 1, 1), "a")]
    df = spark.createDataFrame(test_data, nullable_schema)

    with pytest.raises(TypeError, match="id"):
        spark_schemas.validate_schema(df, SCHEMA)


def test_validate_schema_allows_null_in_nullable_field(spark):
    nullable_schema = StructType([
        StructField("id",         StringType(),    nullable=False),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("label",      StringType(),    nullable=True),
    ])
    test_data = [("1", datetime.datetime(2024, 1, 1), None)]
    df = spark.createDataFrame(test_data, nullable_schema)

    spark_schemas.validate_schema(df, SCHEMA)  # should not raise


def test_validate_schema_reports_all_errors_at_once(spark):
    wrong_schema = StructType([
        StructField("id", IntegerType()),  # wrong type
        # created_at missing entirely
        StructField("label", StringType()),
    ])
    test_data = [(1, "a")]
    df = spark.createDataFrame(test_data, wrong_schema)

    with pytest.raises(TypeError) as exc_info:
        spark_schemas.validate_schema(df, SCHEMA)

    error_message = str(exc_info.value)
    assert "id" in error_message
    assert "created_at" in error_message


# --- METRIC_SCHEMA ---

def test_metric_schema_metric_value_is_integer_type():
    field = next(f for f in spark_schemas.METRIC_SCHEMA.fields if f.name == "metric_value")
    assert isinstance(field.dataType, IntegerType)


# --- get_metric_schema ---

@pytest.mark.parametrize("dtype_str,expected_type", [
    ("int",    IntegerType),
    ("float",  FloatType),
    ("string", StringType),
    ("bool",   BooleanType),
])
def test_get_metric_schema_metric_value_dtype(dtype_str, expected_type):
    schema = spark_schemas.get_metric_schema(dtype_str)
    field = next(f for f in schema.fields if f.name == "metric_value")
    assert isinstance(field.dataType, expected_type)


def test_get_metric_schema_preserves_all_other_fields():
    schema = spark_schemas.get_metric_schema("float")
    base_fields = {f.name: f for f in spark_schemas.METRIC_SCHEMA.fields}
    for field in schema.fields:
        if field.name == "metric_value":
            continue
        assert field.name in base_fields
        assert field.dataType == base_fields[field.name].dataType
        assert field.nullable == base_fields[field.name].nullable


def test_get_metric_schema_does_not_mutate_metric_schema():
    original_dtype = next(f for f in spark_schemas.METRIC_SCHEMA.fields if f.name == "metric_value").dataType
    spark_schemas.get_metric_schema("string")
    current_dtype = next(f for f in spark_schemas.METRIC_SCHEMA.fields if f.name == "metric_value").dataType
    assert type(current_dtype) == type(original_dtype)


def test_get_metric_schema_raises_on_invalid_dtype():
    with pytest.raises(ValueError, match="unsupported_type"):
        spark_schemas.get_metric_schema("unsupported_type")
