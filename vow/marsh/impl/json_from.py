from datetime import datetime, timedelta
from typing import Type, Any

import pytz
from dataclasses import dataclass

from vow.marsh.impl.json_into import JsonIntoStruct, JsonIntoDateTime, \
    JsonIntoDateTimeMapper
from vow.marsh.base import Mapper, Fields, Fac


class JsonFromStructMapper(Mapper):
    def __init__(self, cls: Type, dependencies: 'Fields'):
        self.cls = cls
        super().__init__(dependencies)

    def serialize(self, obj: Any) -> Any:
        r = {}
        for k, v in self.dependencies.items():
            r[k] = v.serialize(obj[k])

        return self.cls(**r)


@dataclass
class JsonFromStruct(JsonIntoStruct):
    __mapper_cls__ = JsonFromStructMapper

    cls: Type

    def create(self, dependencies: Fields) -> Mapper:
        return JsonFromStructMapper(self.cls, dependencies)


class JsonFromDateTimeMapper(JsonIntoDateTimeMapper):

    def serialize(self, obj: Any) -> Any:
        return datetime.strptime(obj, self.format).replace(tzinfo=pytz.utc)


class JsonFromDateTime(JsonIntoDateTime):
    __mapper_cls__ = JsonFromDateTimeMapper


class JsonFromTimeDeltaMapper(Mapper):
    def serialize(self, obj: float) -> timedelta:
        return timedelta(seconds=obj)


class JsonFromTimeDelta(Fac):
    __mapper_cls__ = JsonFromTimeDeltaMapper
