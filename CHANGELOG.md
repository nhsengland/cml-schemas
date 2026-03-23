# Changelog

All notable changes to this project will be documented here.

This project adheres to [Semantic Versioning](https://semver.org/).

---

## [0.1.1] - Unreleased

### Changed
- Renamed `cml_spark` module to `spark_schemas`
- Renamed `tests/` directory to `tests/unittests/`

---

## [0.1.0] - Initial release

### Added
- `METRIC_SCHEMA`: Spark `StructType` schema for CML metric data
- `DIMENSIONS_SCHEMA`: base Spark `StructType` schema for CML dimension data
- `create_dimensions_schema(dimensions)`: builds a full dimensions schema from a list of dimension column names
- `select_from_schema(df, schema)`: selects and reorders DataFrame columns to match a schema
- `validate_schema(df, schema)`: validates a DataFrame's column names and types against a schema, collecting all errors before raising
