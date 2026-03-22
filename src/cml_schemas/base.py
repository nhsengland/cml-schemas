from abc import ABC, abstractmethod


class BaseSchemaManager(ABC):

    @abstractmethod
    def create_dimensions_schema(self, dimensions: list[str]):
        pass

    @abstractmethod
    def select_from_schema(self, df, schema):
        pass

    @abstractmethod
    def validate_schema(self, df, schema) -> None:
        pass
