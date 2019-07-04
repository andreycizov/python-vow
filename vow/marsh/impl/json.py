from typing import Any

from dataclasses import dataclass

from vow.marsh.error import SerializationError, subserializer
from vow.marsh.base import Mapper, Fac, FieldsFac

JSON_FROM = 'json_from'
JSON_INTO = 'json_into'


class JsonAnyListMapper(Mapper):
    def serialize(self, obj: Any) -> Any:
        r = []
        for i, x in enumerate(obj):
            with subserializer(i):
                r.append(self.dependencies['value'].serialize(x))
        return r


@dataclass
class JsonAnyList(Fac):
    __mapper_cls__ = JsonAnyListMapper

    value: Fac

    def dependencies(self) -> FieldsFac:
        return {'value': self.value}


class JsonAnyDictMapper(Mapper):
    def serialize(self, obj: Any) -> Any:
        r = {}

        for k, v in obj.items():
            with subserializer('$key'):
                k = self.dependencies['key'].serialize(k)

            with subserializer('$value'):
                v = self.dependencies['value'].serialize(v)

            r[k] = v
        return r


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


class JsonAnyAnyMapper(Mapper):

    def serialize(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            r = {}
            for k, v in obj.items():
                with subserializer('$key'):
                    k = self.serialize(k)
                assert isinstance(k, str), k

                with subserializer('$value'):
                    v = self.serialize(v)

                r[k] = v
            return r
        elif isinstance(obj, list):
            r = []

            for i, v in enumerate(obj):
                with subserializer(i):
                    r.append(self.serialize(v))
            return r
        elif isinstance(obj, str):
            return obj
        elif isinstance(obj, float):
            return obj
        elif isinstance(obj, int):
            return obj
        elif obj is None:
            return obj
        else:
            raise SerializationError(val=obj, reason='not_json_value')


@dataclass
class JsonAnyAny(Fac):
    __mapper_cls__ = JsonAnyAnyMapper
