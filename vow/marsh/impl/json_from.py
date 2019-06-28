from datetime import datetime, timedelta
from typing import Type, Any, List, Tuple

import pytz
from dataclasses import dataclass, MISSING
from yaml.serializer import SerializerError

from vow.marsh.impl.json_into import JsonIntoStruct, JsonIntoDateTime, \
    JsonIntoDateTimeMapper, JsonIntoStructMapper
from vow.marsh.base import Mapper, Fields, Fac


class JsonFromStructMapper(JsonIntoStructMapper):

    def __init__(self, cls: Type,
                 fields: List[Tuple[str, bool]], dependencies: Fields):
        self.cls = cls
        super().__init__(fields, dependencies)

    def serialize(self, obj: Any) -> Any:
        r = {}
        for k, is_nullable in self.fields:
            v = self.dependencies[k]
            obj_v = obj.get(k, MISSING)

            if is_nullable and obj_v == MISSING:
                r[k] = None
            elif obj_v == MISSING:
                raise SerializerError(f'{k}')
            else:
                r[k] = v.serialize(obj_v)

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
