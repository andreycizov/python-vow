from collections import OrderedDict
from typing import Any, List, Tuple

from dataclasses import dataclass
from datetime import timedelta, datetime

from vow.marsh.base import Mapper, Fac, FieldsFac, Fields
from vow.marsh.impl.json import JsonAnyOptional


class JsonIntoStructMapper(Mapper):

    def __init__(self, fields: List[Tuple[str, bool]], dependencies: Fields):
        self.fields = fields
        super().__init__(dependencies)

    def serialize(self, obj: Any) -> Any:
        r = OrderedDict()

        for k, _ in self.fields:
            v = self.dependencies[k]

            r[k] = v.serialize(getattr(obj, k))
        return r


@dataclass
class JsonIntoStruct(Fac):
    __mapper_cls__ = JsonIntoStructMapper

    fields: List[Tuple[str, Fac]]

    def create(self, dependencies: Fields) -> Mapper:
        return self.__mapper_cls__(
            dependencies=dependencies,
            fields=[(x, isinstance(y, JsonAnyOptional)) for x, y in self.fields]
        )

    def dependencies(self) -> FieldsFac:
        return {k: v for k, v in self.fields}


ISO8601 = '%Y-%m-%dT%H:%M:%S.%fZ'


class JsonIntoDateTimeMapper(Mapper):

    def __init__(self, format: str, dependencies: Fields):
        self.format = format
        super().__init__(dependencies)

    def serialize(self, obj: datetime) -> Any:
        return format(obj, self.format)


@dataclass
class JsonIntoDateTime(Fac):
    __mapper_cls__ = JsonIntoDateTimeMapper
    __mapper_args__ = 'format',
    format: str = ISO8601


class JsonIntoTimeDeltaMapper(Mapper):
    def serialize(self, obj: timedelta) -> Any:
        return obj.total_seconds()


class JsonIntoTimeDelta(Fac):
    __mapper_cls__ = JsonIntoTimeDeltaMapper
