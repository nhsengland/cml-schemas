import pytest
import pandas as pd

from cml_schemas import pandas_schemas

DIMENSIONS = ["AgeAtBookingMotherAvg", "BirthweightTermGroup", "TotalDeliveries"]

TEST_SCHEMA = pandas_schemas.PandasSchema([
    pandas_schemas.PandasField("id",         "object",         nullable=False),
    pandas_schemas.PandasField("created_at", "datetime64[ns]", nullable=False),
    pandas_schemas.PandasField("label",      "object",         nullable=True),
])


# --- create_dimensions_schema ---

def test_create_dimensions_schema_contains_base_fields():
    schema = pandas_schemas.create_dimensions_schema(DIMENSIONS)
    field_names = [f.name for f in schema.fields]

    assert "dimension_id" in field_names
    assert "dimension_type_id" in field_names
    assert "dimension_count" in field_names


def test_create_dimensions_schema_contains_dimension_fields():
    schema = pandas_schemas.create_dimensions_schema(DIMENSIONS)
    field_names = [f.name for f in schema.fields]

    for col in DIMENSIONS:
        assert col in field_names


def test_create_dimensions_schema_base_fields_come_first():
    schema = pandas_schemas.create_dimensions_schema(DIMENSIONS)
    field_names = [f.name for f in schema.fields]

    base_field_names = [f.name for f in pandas_schemas.DIMENSIONS_SCHEMA.fields]
    assert field_names[:len(base_field_names)] == base_field_names


def test_create_dimensions_schema_dimension_fields_are_nullable_strings():
    schema = pandas_schemas.create_dimensions_schema(DIMENSIONS)
    dimension_fields = {f.name: f for f in schema.fields}

    for col in DIMENSIONS:
        assert dimension_fields[col].dtype == "object"
        assert dimension_fields[col].nullable is True


def test_create_dimensions_schema_does_not_mutate_base_schema():
    before = [f.name for f in pandas_schemas.DIMENSIONS_SCHEMA.fields]
    pandas_schemas.create_dimensions_schema(DIMENSIONS)
    after = [f.name for f in pandas_schemas.DIMENSIONS_SCHEMA.fields]

    assert before == after


# --- select_from_schema ---

def test_select_from_schema_returns_correct_columns():
    df = pd.DataFrame({
        "id":         ["1"],
        "created_at": pd.to_datetime(["2024-01-01"]),
        "label":      ["a"],
        "unwanted":   ["extra"],
    })
    result = pandas_schemas.select_from_schema(df, TEST_SCHEMA)

    assert list(result.columns) == ["id", "created_at", "label"]
    assert "unwanted" not in result.columns


def test_select_from_schema_preserves_column_order():
    # Create df with columns in a different order
    df = pd.DataFrame({
        "label":      ["a"],
        "created_at": pd.to_datetime(["2024-01-01"]),
        "id":         ["1"],
    })
    result = pandas_schemas.select_from_schema(df, TEST_SCHEMA)

    assert list(result.columns) == ["id", "created_at", "label"]


# --- validate_schema ---

def test_validate_schema_passes_for_matching_schema():
    df = pd.DataFrame({
        "id":         ["1"],
        "created_at": pd.to_datetime(["2024-01-01"]),
        "label":      ["a"],
    })
    pandas_schemas.validate_schema(df, TEST_SCHEMA)  # should not raise


def test_validate_schema_raises_on_wrong_type():
    df = pd.DataFrame({
        "id":         ["1"],
        "created_at": ["2024-01-01"],  # string, not datetime
        "label":      ["a"],
    })
    with pytest.raises(TypeError, match="created_at"):
        pandas_schemas.validate_schema(df, TEST_SCHEMA)


def test_validate_schema_raises_on_missing_column():
    df = pd.DataFrame({
        "id":         ["1"],
        "created_at": pd.to_datetime(["2024-01-01"]),
        # label missing
    })
    with pytest.raises(TypeError, match="label"):
        pandas_schemas.validate_schema(df, TEST_SCHEMA)


def test_validate_schema_raises_on_null_in_non_nullable_field():
    df = pd.DataFrame({
        "id":         [None],  # null in non-nullable field
        "created_at": pd.to_datetime(["2024-01-01"]),
        "label":      ["a"],
    })
    with pytest.raises(TypeError, match="id"):
        pandas_schemas.validate_schema(df, TEST_SCHEMA)


def test_validate_schema_allows_null_in_nullable_field():
    df = pd.DataFrame({
        "id":         ["1"],
        "created_at": pd.to_datetime(["2024-01-01"]),
        "label":      [None],  # null in nullable field
    })
    pandas_schemas.validate_schema(df, TEST_SCHEMA)  # should not raise


def test_validate_schema_reports_all_errors_at_once():
    df = pd.DataFrame({
        "id":    [1],   # wrong type (int64, should be object)
        # created_at missing entirely
        "label": ["a"],
    })
    with pytest.raises(TypeError) as exc_info:
        pandas_schemas.validate_schema(df, TEST_SCHEMA)

    error_message = str(exc_info.value)
    assert "id" in error_message
    assert "created_at" in error_message


# --- get_metric_schema ---

@pytest.mark.parametrize("dtype_str,expected_pandas_dtype", [
    ("int",    "int64"),
    ("float",  "float64"),
    ("string", "object"),
    ("bool",   "bool"),
])
def test_get_metric_schema_metric_value_dtype(dtype_str, expected_pandas_dtype):
    schema = pandas_schemas.get_metric_schema(dtype_str)
    field = next(f for f in schema.fields if f.name == "metric_value")
    assert field.dtype == expected_pandas_dtype


def test_get_metric_schema_preserves_all_other_fields():
    schema = pandas_schemas.get_metric_schema("float")
    base_fields = {f.name: f for f in pandas_schemas.METRIC_SCHEMA.fields}
    for field in schema.fields:
        if field.name == "metric_value":
            continue
        assert field.name in base_fields
        assert field.dtype == base_fields[field.name].dtype
        assert field.nullable == base_fields[field.name].nullable


def test_get_metric_schema_does_not_mutate_metric_schema():
    original_dtype = next(f for f in pandas_schemas.METRIC_SCHEMA.fields if f.name == "metric_value").dtype
    pandas_schemas.get_metric_schema("string")
    current_dtype = next(f for f in pandas_schemas.METRIC_SCHEMA.fields if f.name == "metric_value").dtype
    assert current_dtype == original_dtype


def test_get_metric_schema_raises_on_invalid_dtype():
    with pytest.raises(ValueError, match="unsupported_type"):
        pandas_schemas.get_metric_schema("unsupported_type")
