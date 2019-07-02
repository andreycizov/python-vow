from typing import Any

from dataclasses import dataclass

from vow.marsh.error import SerializationError
from vow.marsh.base import Mapper, Fac, FieldsFac

JSON_FROM = 'json_from'
JSON_INTO = 'json_into'


class JsonAnyListMapper(Mapper):
    def serialize(self, obj: Any) -> Any:
        r = []
        for i, x in enumerate(obj):
            try:
                r.append(self.dependencies['value'].serialize(x))
            except SerializationError as e:
                raise e.with_path(i)
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
            try:
                k = self.dependencies['key'].serialize(k)
            except SerializationError as e:
                raise e.with_path(k, '$key')

            try:
                v = self.dependencies['value'].serialize(v)
            except SerializationError as e:
                raise e.with_path(k, '$value')

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
                try:
                    k = self.serialize(k)
                except SerializationError as e:
                    raise e.with_path(k, '$key')
                assert isinstance(k, str), k

                try:
                    v = self.serialize(v)
                except SerializationError as e:
                    raise e.with_path(k, '$value')

                r[k] = v
            return r
        elif isinstance(obj, list):
            r = []

            for i, v in enumerate(obj):
                try:
                    r.append(self.serialize(v))
                except SerializationError as e:
                    raise e.with_path(i)
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


