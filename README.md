# Central Metrics Library Schemas (Python)

**Supports CML Proforma version 2.0**

A lightweight Python package providing validated **schemas for the Central Metrics Library (CML)** in multiple formats.

**Supported formats:** Apache Spark (`pyspark.sql.types.StructType`) and pandas (`pd.DataFrame`).

> The Central Metrics Library (CML) defines a common structure for **metrics** and the **metadata** that describes them, so analytical teams can produce, discover, and reuse metrics consistently across the NHS. This package implements those schemas for use in data pipelines.

***

## Why this exists

Today, metrics live in many places and many shapes—hard to find, easy to duplicate, and sometimes inconsistent. The CML aims to unify metric structures and metadata into a single, curated, service-managed library so analysts can source authoritative, consistently defined metrics, supported by appropriate security tagging and clear SME-owned definitions (purpose, methods, limitations, differences from similar measures). This repo hosts code-first schemas aligned to that aim.

***

## Status: **BETA**

The CML—and therefore these schemas—are in **beta** while we pilot with analytical teams and iterate on feedback. Expect **breaking changes** as the specification evolves. Please adopt **resilient coding practices** and pin schema versions where appropriate.

***

## What's in the box

**Schemas** for core CML entities (available in both Spark and pandas formats):

*   `METRIC_SCHEMA` — the measured value(s) and identifiers
*   `DIMENSIONS_SCHEMA` — base schema for dimensions used to slice metrics
*   `SOURCE_SCHEMA` — source system metadata
*   `METADATA_SCHEMA` — descriptive info: purpose, methodology, caveats, lineage, etc.
*   `RELATIONSHIPS_SCHEMA` — links between metrics and other artefacts

**Helper functions** (available in both `spark_schemas` and `pandas_schemas`):

*   `get_metric_schema(metric_value_dtype)` — returns a metric schema with a custom metric value type
*   `create_dimensions_schema(dimensions)` — builds a full dimensions schema from a list of dimension column names
*   `select_from_schema(df, schema)` — selects and reorders DataFrame columns to match a schema
*   `validate_schema(df, schema)` — validates a DataFrame's column names and types against a schema

These mirror the "draft standardised schema" referenced in the CML materials and will track the official spec as it matures.

***

## Installation

```bash
pip install cml-schemas
```

> Tip: Pin to a specific version (`cml-schemas==x.y.z`) to protect your pipelines from breaking changes during beta.

***

## Quick start (Spark)

### Use a built-in schema

```python
from cml_schemas import spark_schemas

# Create an empty, schema-correct DataFrame
empty_df = spark.createDataFrame([], schema=spark_schemas.METRIC_SCHEMA)
```

### Build a dimensions schema dynamically

```python
from cml_schemas import spark_schemas

dimensions = ["AgeGroup", "Region", "Ethnicity"]
schema = spark_schemas.create_dimensions_schema(dimensions)

empty_df = spark.createDataFrame([], schema=schema)
```

### Validate a DataFrame against a schema

```python
from cml_schemas import spark_schemas

# Raises TypeError with all mismatches listed if validation fails
spark_schemas.validate_schema(df, spark_schemas.METRIC_SCHEMA)
```

### Use a typed metric schema

`METRIC_SCHEMA` stores `metric_value` as `IntegerType` by default. CML rules also permit float metric values — if your pipeline produces floats, use `get_metric_schema()` to get a schema with the correct type enforced:

```python
from cml_schemas import spark_schemas

# metric_value as FloatType
float_schema = spark_schemas.get_metric_schema("float")

empty_df = spark.createDataFrame([], schema=float_schema)
```

All other fields are identical to `METRIC_SCHEMA`.

### Select and reorder columns to match a schema

```python
from cml_schemas import spark_schemas

# Selects only the columns defined in the schema, in schema order
df = spark_schemas.select_from_schema(df, spark_schemas.METRIC_SCHEMA)
```

***

## Quick start (pandas)

### Use a built-in schema

```python
from cml_schemas import pandas_schemas

# Validate an existing DataFrame against the metric schema
pandas_schemas.validate_schema(df, pandas_schemas.METRIC_SCHEMA)
```

### Build a dimensions schema dynamically

```python
from cml_schemas import pandas_schemas

dimensions = ["AgeGroup", "Region", "Ethnicity"]
schema = pandas_schemas.create_dimensions_schema(dimensions)
```

### Use a typed metric schema

```python
from cml_schemas import pandas_schemas

# metric_value as float64
float_schema = pandas_schemas.get_metric_schema("float")
```

Supported metric value types: `"int"`, `"float"`, `"string"`, `"bool"`.

### Select and reorder columns to match a schema

```python
from cml_schemas import pandas_schemas

# Selects only the columns defined in the schema, in schema order
df = pandas_schemas.select_from_schema(df, pandas_schemas.METRIC_SCHEMA)
```

***

## Principles for usage

*   **Spec-first**: Schemas track the CML Data Specification (draft during beta). When the official fields or formats change, this package revs a minor or major version, with changelog notes. We recommend locking to a specific version of this package to avoid breaking changes when the schema is updated.
*   **Build from tidy data where possible**: Aim to produce metrics by first producing outputs in tidy-data format and converting from there to the CML spec. See the [CML conversion helper functions](https://github.com/nhsengland/cml-conversion-helpers).
*   **RAP**: Aim to develop your pipelines in line with RAP (Reproducible Analytical Pipelines) principles — see the [RAP Community of Practice website](https://nhsdigital.github.io/rap-community-of-practice/) for guidance.

***

## How this maps to the CML artefacts

*   **CML Proforma & Spec**: Informs field names, types, nullability, and relationships for `metric`, `metadata`, `relationship`, `source`, `dimension`.
    Producers can continue to complete the proforma as documentation while using these programmatic schemas in code.
*   **Ownership & curation**: This repo does not own business definitions; SMEs own and maintain metric definitions. We only provide the technical shapes to carry those definitions consistently.
*   **Discovery & serving**: FDP National/Metadata Explore Hub will surface metrics/metadata to end users. This package helps you produce compliant data for that ecosystem.

***

## Versioning

> **Note:** package versions do **not** map to CML Proforma versions. See the top of this README for the currently supported proforma version, or [CHANGELOG.md](CHANGELOG.md) for the proforma version supported by each past release.

This package follows [Semantic Versioning](https://semver.org/):

*   **Major** (`x.0.0`) — breaking changes to schema field names, types, or nullability (expect these during beta as the CML spec evolves)
*   **Minor** (`0.x.0`) — new schemas or helper functions added in a backwards-compatible way
*   **Patch** (`0.0.x`) — bug fixes and non-breaking internal changes

Pin to a specific version in your pipelines (`cml-schemas==x.y.z`) to protect yourself from breaking changes.

***

## Contributing

We welcome issues and PRs, especially for:

*   Gaps or mismatches vs the CML spec (with references)
*   Additional runtime formats (e.g., JSON Schema, SQL DDL, Polars)
*   Validation and test data generators
*   Developer experience improvements

### Setting up your environment

We recommend using **GitHub Codespaces** — this repo's devcontainer will automatically install `pipx` and `poetry` for you.

Once your Codespace is ready (or if you're working locally with `pipx` and `poetry` already installed):

```bash
# Install dependencies and create the virtual environment
poetry install

# Activate the environment
poetry shell
```

To run the tests:

```bash
pytest
```

### Branching

Create a branch from `main` using a prefix that describes the type of change:

*   `feature/your-branch-name` — new functionality
*   `patch/your-branch-name` — bug fixes or minor tweaks
*   `chore/your-branch-name` — non-functional changes (docs, config, CI)

### Making changes

All changes must be made via a **pull request** on GitHub and require **at least one approval** before merging.

### Publishing to PyPI

1.  Bump the version in `pyproject.toml` following semver (see above)
2.  Update [CHANGELOG.md](CHANGELOG.md) with the new version and a summary of changes
3.  Open a PR, get it approved, and merge to `main`
4.  On GitHub, create a new **Release** targeting `main`, using the version number as the tag (e.g. `v2.1.0`) — this triggers the publish workflow automatically

> **One-time setup:** the publish workflow requires a PyPI API token stored as a repository secret. If the release does not trigger the package to be published, contact an owner of the package on PyPI - a token scoped to the `cml-schemas` project may need to be created in PyPI and added in GitHub under **Settings → Secrets and variables → Actions** as `PYPI_API_TOKEN`.


#### Test PyPI

**Want to do a test run first?** Publish to [Test PyPI](https://test.pypi.org/) manually:

```bash
poetry config repositories.test-pypi https://upload.pypi.org/legacy/
poetry config pypi-token.test-pypi <your-test-pypi-token>
poetry publish --build --repository test-pypi
```

Then, in a separate project to this one, verify the install from Test PyPI and make sure it's all working as you intended:

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ cml-schemas
```

***

## License

MIT

***

## Acknowledgements

This package is inspired by and aligned to the **Central Metrics Library** initiative, developed with analytical teams and Platform Modernisation to fit the developing **FDP National** platform.
