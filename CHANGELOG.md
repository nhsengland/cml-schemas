# Changelog

All notable changes to this project will be documented here.

This project adheres to [Semantic Versioning](https://semver.org/).

---

## [2.1.0] - 2026-05-07

**Supports CML Proforma version 2.0**

### Added
- `pandas_schemas` module: pandas equivalents of all CML schemas (`METRIC_SCHEMA`, `DIMENSIONS_SCHEMA`, `SOURCE_SCHEMA`, `METADATA_SCHEMA`, `RELATIONSHIPS_SCHEMA`) with matching helper functions (`get_metric_schema`, `create_dimensions_schema`, `select_from_schema`, `validate_schema`)

### Fixed
- Removed erroneous `dimension_names` field from `DIMENSIONS_SCHEMA` in Spark schemas

---

## [2.0.0] - 2026-04-10

**Supports CML Proforma version 2.0**

> **Breaking changes** — all schemas have changed. Pin your version.

### Added
- `SOURCE_SCHEMA`: new Spark `StructType` schema for CML source data, with fields `source_name`, `project_id`, `source_platform`, `first_record_timestamp`, `last_record_timestamp`, `last_ingest_timestamp`, `version_release_date`, `version_release`

### Changed

**`METRIC_SCHEMA`**
- Replaced `metric_dimension_id` (non-nullable) and `dimension_cohort_id` (nullable) with `dimension_id` (nullable) and `reporting_grain` (non-nullable)
- Removed `last_record_timestamp` and `last_ingest_timestamp` (moved to `SOURCE_SCHEMA`)
- Renamed `publication_date` → `publication_datetime`
- `metric_value` remains `IntegerType`; use `get_metric_schema("float")` for float pipelines

**`DIMENSIONS_SCHEMA`**
- Replaced `dimension_cohort_id` and `metric_id` with `dimension_id`, `dimension_type_id`, `dimension_names`, and `dimension_count` (all nullable)

**`RELATIONSHIPS_SCHEMA`**
- Renamed `parent_metric_id` → `metric_id`
- Added `metric_short_name` and `child_metric_short_name`
- Changed `relationship_description` to non-nullable

**`METADATA_SCHEMA`**
- Added `metric_family_id`, `metric_start_date`, `metric_end_date`, `available_reporting_grain`, `available_location_types`, `project_id`, `source_platform`, and `calculation`
- Renamed `metric_category` → `available_metric_category`
- Renamed `available_dimensions` → `available_dimension_types`
- Renamed `source` → `source_name`
- Removed `reporting_grain` (moved to `METRIC_SCHEMA`)
- Changed `metric_short_name` and `organisation_geog_type` to nullable

---

## [0.1.1] - Unreleased

**Supports CML Proforma version 1.0**

### Changed
- Renamed `cml_spark` module to `spark_schemas`
- Renamed `tests/` directory to `tests/unittests/`

---

## [0.1.0] - Initial release

**Supports CML Proforma version 1.0**

### Added
- `METRIC_SCHEMA`: Spark `StructType` schema for CML metric data
- `DIMENSIONS_SCHEMA`: base Spark `StructType` schema for CML dimension data
- `create_dimensions_schema(dimensions)`: builds a full dimensions schema from a list of dimension column names
- `select_from_schema(df, schema)`: selects and reorders DataFrame columns to match a schema
- `validate_schema(df, schema)`: validates a DataFrame's column names and types against a schema, collecting all errors before raising
