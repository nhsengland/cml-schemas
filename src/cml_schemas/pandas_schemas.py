import re
from dataclasses import dataclass

import pandas as pd


@dataclass
class PandasField:
    name: str
    dtype: str
    nullable: bool = True


class PandasSchema:
    def __init__(self, fields: list[PandasField]):
        self.fields = fields


METRIC_SCHEMA = PandasSchema([
    PandasField("datapoint_id",                     "object",         nullable=False),
    PandasField("metric_id",                        "object",         nullable=False),
    PandasField("dimension_id",                     "object",         nullable=True),
    PandasField("reporting_grain",                  "object",         nullable=False),
    PandasField("location_id",                      "object",         nullable=False),
    PandasField("location_type",                    "object",         nullable=False),
    PandasField("reporting_period_start_datetime",  "datetime64[ns]", nullable=False),
    PandasField("publication_datetime",             "datetime64[ns]", nullable=True),
    PandasField("metric_value",                     "int64",          nullable=False),
    PandasField("additional_metric_values",         "object",         nullable=True),
])


SOURCE_SCHEMA = PandasSchema([
    PandasField("source_name",             "object",         nullable=False),
    PandasField("project_id",              "object",         nullable=True),
    PandasField("source_platform",         "object",         nullable=True),
    PandasField("first_record_timestamp",  "datetime64[ns]", nullable=False),
    PandasField("last_record_timestamp",   "datetime64[ns]", nullable=False),
    PandasField("last_ingest_timestamp",   "datetime64[ns]", nullable=False),
    PandasField("version_release_date",    "datetime64[ns]", nullable=False),
    PandasField("version_release",         "int64",          nullable=False),
])


DIMENSIONS_SCHEMA = PandasSchema([
    PandasField("dimension_id",       "object", nullable=True),
    PandasField("dimension_type_id",  "object", nullable=True),
    PandasField("dimension_count",    "int64",  nullable=True),
])


RELATIONSHIPS_SCHEMA = PandasSchema([
    PandasField("metric_id",                "object", nullable=False),
    PandasField("metric_short_name",        "object", nullable=True),
    PandasField("child_metric_id",          "object", nullable=False),
    PandasField("child_metric_short_name",  "object", nullable=False),
    PandasField("relationship_type",        "object", nullable=False),
    PandasField("relationship_category",    "object", nullable=False),
    PandasField("relationship_description", "object", nullable=False),
])


METADATA_SCHEMA = PandasSchema([
    PandasField("metric_family_id",                             "object",         nullable=False),
    PandasField("metric_id",                                    "object",         nullable=False),
    PandasField("metric_status",                                "object",         nullable=False),
    PandasField("metric_title",                                 "object",         nullable=False),
    PandasField("metric_start_date",                            "datetime64[ns]", nullable=False),
    PandasField("metric_end_date",                              "datetime64[ns]", nullable=True),
    PandasField("available_metric_category",                    "object",         nullable=False),
    PandasField("available_reporting_grain",                    "object",         nullable=False),
    PandasField("available_location_types",                     "object",         nullable=False),
    PandasField("available_dimension_types",                    "object",         nullable=True),
    PandasField("business_area",                                "object",         nullable=False),
    PandasField("business_sub_area",                            "object",         nullable=False),
    PandasField("domain",                                       "object",         nullable=True),
    PandasField("source_name",                                  "object",         nullable=False),
    PandasField("project_id",                                   "object",         nullable=True),
    PandasField("source_platform",                              "object",         nullable=True),
    PandasField("metric_owner",                                 "object",         nullable=False),
    PandasField("metric_short_name",                            "object",         nullable=True),
    PandasField("metric_alias",                                 "object",         nullable=True),
    PandasField("metric_description",                           "object",         nullable=False),
    PandasField("calculation",                                  "object",         nullable=True),
    PandasField("metric_purpose",                               "object",         nullable=False),
    PandasField("unit",                                         "object",         nullable=False),
    PandasField("format",                                       "object",         nullable=False),
    PandasField("target",                                       "object",         nullable=True),
    PandasField("frequency_of_refresh",                         "object",         nullable=False),
    PandasField("aggregation",                                  "object",         nullable=False),
    PandasField("statistical_disclosure_control_flag",          "object",         nullable=False),
    PandasField("statistical_disclosure_control_description",   "object",         nullable=True),
    PandasField("interpretation",                               "object",         nullable=True),
    PandasField("inclusion_exclusion_rules",                    "object",         nullable=True),
    PandasField("usage",                                        "object",         nullable=True),
    PandasField("organisation_geog_type",                       "object",         nullable=True),
    PandasField("footnotes",                                    "object",         nullable=True),
    PandasField("notes",                                        "object",         nullable=True),
])


_METRIC_VALUE_DTYPE_MAP = {
    "int":    "int64",
    "float":  "float64",
    "string": "object",
    "bool":   "bool",
}


def _normalize_dtype(dtype_str: str) -> str:
    dtype_str = re.sub(r"datetime64\[.*?\]", "datetime64", dtype_str)
    # pandas 2.x uses "str" for string columns; normalize alongside "object"
    if dtype_str in ("str", "object"):
        return "string"
    return dtype_str


def get_metric_schema(metric_value_dtype: str) -> PandasSchema:
    """Return a copy of METRIC_SCHEMA with the ``metric_value`` field cast to the requested dtype.

    Args:
        metric_value_dtype: The desired dtype for the ``metric_value`` column.
            Must be one of ``"int"``, ``"float"``, ``"string"``, or ``"bool"``.

    Returns:
        A ``PandasSchema`` identical to ``METRIC_SCHEMA`` except that the
        ``metric_value`` field uses the pandas dtype corresponding to
        ``metric_value_dtype``.

    Raises:
        ValueError: If ``metric_value_dtype`` is not a supported dtype.
    """
    if metric_value_dtype not in _METRIC_VALUE_DTYPE_MAP:
        raise ValueError(f"Unsupported metric_value_dtype '{metric_value_dtype}'. Must be one of: {list(_METRIC_VALUE_DTYPE_MAP)}")
    dtype = _METRIC_VALUE_DTYPE_MAP[metric_value_dtype]
    fields = [
        PandasField(f.name, dtype if f.name == "metric_value" else f.dtype, nullable=f.nullable)
        for f in METRIC_SCHEMA.fields
    ]
    return PandasSchema(fields)


def create_dimensions_schema(dimensions: list[str]) -> PandasSchema:
    """Extend DIMENSIONS_SCHEMA with additional nullable string columns.

    Args:
        dimensions: Column names to append to the base ``DIMENSIONS_SCHEMA`` fields.

    Returns:
        A ``PandasSchema`` containing all fields from ``DIMENSIONS_SCHEMA`` followed
        by a nullable ``object`` field for each name in ``dimensions``.
    """
    dimension_fields = [PandasField(col, "object", nullable=True) for col in dimensions]
    return PandasSchema(DIMENSIONS_SCHEMA.fields + dimension_fields)


def select_from_schema(df: pd.DataFrame, schema: PandasSchema) -> pd.DataFrame:
    """Select only the columns defined in ``schema`` from ``df``, in schema order.

    Args:
        df: The source pandas DataFrame.
        schema: A ``PandasSchema`` whose field names define the columns to select.

    Returns:
        A DataFrame containing only the columns present in ``schema``, ordered
        as they appear in ``schema``.
    """
    return df[[field.name for field in schema.fields]]


def validate_schema(df: pd.DataFrame, schema: PandasSchema) -> None:
    """Assert that ``df`` contains all columns defined in ``schema`` with matching types
    and that non-nullable fields contain no null values.

    Args:
        df: The pandas DataFrame to validate.
        schema: The expected ``PandasSchema``.

    Raises:
        TypeError: If any expected column is missing, has a mismatched type, or
            violates a non-nullable constraint. The error message lists all violations found.
    """
    errors = []

    for field in schema.fields:
        col_name = field.name

        if col_name not in df.columns:
            errors.append(f"  - '{col_name}': column is missing")
            continue

        actual = _normalize_dtype(str(df[col_name].dtype))
        expected = _normalize_dtype(field.dtype)
        if actual != expected:
            errors.append(f"  - '{col_name}': expected {field.dtype}, got {str(df[col_name].dtype)}")
            continue

        if not field.nullable and df[col_name].isna().any():
            null_count = int(df[col_name].isna().sum())
            errors.append(f"  - '{col_name}': non-nullable field contains {null_count} null(s)")

    if errors:
        raise TypeError("Schema validation failed:\n" + "\n".join(errors))
