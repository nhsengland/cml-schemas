# Central Metrics Library Schemas (Python)

A lightweight Python package providing validated **schemas for the Central Metrics Library (CML)** in (eventually) multiple formats.

**Current focus:** Apache Spark (`pyspark.sql.types.StructType`).

> The Central Metrics Library (CML) defines a common structure for **metrics** and the **metadata** that describes them, so analytical teams can produce, discover, and reuse metrics consistently across the NHS. This package implements those schemas for use in data pipelines.

***

## Why this exists

Today, metrics live in many places and many shapes—hard to find, easy to duplicate, and sometimes inconsistent. The CML aims to unify metric structures and metadata into a single, curated, service-managed library so analysts can source authoritative, consistently defined metrics, supported by appropriate security tagging and clear SME-owned definitions (purpose, methods, limitations, differences from similar measures). This repo hosts code-first schemas aligned to that aim.

***

## Status: **BETA**

The CML—and therefore these schemas—are in **beta** while we pilot with analytical teams and iterate on feedback. Expect **breaking changes** as the specification evolves. Please adopt **resilient coding practices** and pin schema versions where appropriate.

***

## What’s in the box

*   **Spark schemas** for core CML entities (initial set):
    *   `metric` — the measured value(s) and identifiers
    *   `dimension` — dimensions and members used to slice metrics  
    
These mirror the "draft standardised schema" referenced in the CML materials and will track the official spec as it matures.

TODO:

*   `metadata` — descriptive info: purpose, methodology, caveats, lineage, etc.
*   `relationship` — links between metrics and other artefacts

***

## Installation

pip:

```bash
pip install cml-schemas
```

> Tip: Pin to a specific version (`cml-schemas==x.y.z`) 

***

## Quick start (Spark)

```python
## Sample code to be added
```

### Example: creating an empty, schema-correct DataFrame

```python
from cml_schemas.spark import get_schema
metric_schema = get_schema("metric")
empty = spark.createDataFrame([], schema=metric_schema)
```

***

## Principles for usage

*   **Spec-first**: Schemas track the CML Data Specification (draft during beta). When the official fields or formats change, this package revs a minor or major version, with changelog notes. We recommend locking to a specific version of this package to avoid breaking changes when the schema is updated!
*   **Build from tidy data where possible**: Aim to produce metrics by first producing outputs in tidy-data format and converting from there to the CML spec. See the [CML conversion helper functions](https://github.com/nhsengland/cml-conversion-helpers/tree/feature/cml-helper-functions).
*   **RAP**: Aim to develop your pipelines in line with RAP (Reproducible Analytical Pipelines) principles - see the [RAP Community of Practice website](https://nhsdigital.github.io/rap-community-of-practice/) for guidance.

***

## How this maps to the CML artefacts

*   **CML Proforma & Spec**: Informs field names, types, nullability, and relationships for `metric`, `metadata`, `relationship`, `dimension`.  
    Producers can continue to complete the proforma as documentation while using these programmatic schemas in code.
*   **Ownership & curation**: This repo does not own business definitions; SMEs own and maintain metric definitions. We only provide the technical shapes to carry those definitions consistently.
*   **Discovery & serving**: FDP National/Metadata Explore Hub will surface metrics/metadata to end users. This package helps you produce compliant data for that ecosystem.

***

## Using alongside the official materials

We'll add links to other guidance and documentation here as it becomes available.

***

## Versioning

*   **0.x**: Beta; spec and code may change.

During beta, **breaking changes can occur**—please pin versions and read the release notes. 

***

## Contributing

We welcome issues and PRs, especially for:

*   Gaps or mismatches vs the CML spec (with references)
*   Additional runtime formats (e.g., JSON Schema, SQL DDL)
*   Validation and test data generators
*   Developer experience improvements

***

## License

MIT

***

### Acknowledgements

This package is inspired by and aligned to the **Central Metrics Library** initiative, developed with analytical teams and Platform Modernisation to fit the developing **FDP National** platform.