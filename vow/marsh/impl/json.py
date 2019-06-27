from typing import Any

from dataclasses import dataclass

from vow.marsh.base import Mapper, Fac, FieldsFac

JSON_FROM = 'json_from'
JSON_INTO = 'json_into'


class JsonAnyListMapper(Mapper):
    def serialize(self, obj: Any) -> Any:
        return [self.dependencies['value'].serialize(x) for x in obj]


@dataclass
class JsonAnyList(Fac):
    __mapper_cls__ = JsonAnyListMapper

    value: Fac

    def dependencies(self) -> FieldsFac:
        return {'value': self.value}


class JsonAnyDictMapper(Mapper):
    def serialize(self, obj: Any) -> Any:
        return {self.dependencies['key'].serialize(k): self.dependencies['value'].serialize(v) for k, v in obj.items()}


@dataclass
class JsonAnyDict(Fac):
    __mapper_cls__ = JsonAnyDictMapper

    key: Fac
    value: Fac

    def dependencies(self) -> FieldsFac:
        return {'key': self.key, 'value': self.value}


class JsonAnyOptionalMapper(Mapper):

    def serialize(self, obj: Any) -> Any:
        if obj is None:
            return None
        else:
            return self.dependencies['value'].serialize(obj)


@dataclass
class JsonAnyOptional(Fac):
    __mapper_cls__ = JsonAnyOptionalMapper
    value: Fac

    def dependencies(self) -> FieldsFac:
        return {'value': self.value}
