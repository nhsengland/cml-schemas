from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cml_schemas.base import BaseSchemaManager

_registry: dict[str, type] = {}


def register(key: str):
    def decorator(cls):
        _registry[key] = cls
        return cls
    return decorator


def get_schema_manager(key: str) -> "BaseSchemaManager":
    if key not in _registry:
        raise KeyError(f"No schema manager registered for '{key}'. Available: {list(_registry.keys())}")
    return _registry[key]()
