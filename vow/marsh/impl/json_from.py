from datetime import datetime, timedelta
from typing import Type, Any, List, Tuple

import pytz
from dataclasses import dataclass, MISSING

from vow.marsh import SerializationError
from vow.marsh.impl.json import JsonAnyOptional
from vow.marsh.impl.json_into import JsonIntoStruct, JsonIntoDateTime, \
    JsonIntoDateTimeMapper, JsonIntoStructMapper, JsonIntoEnumMapper, JsonIntoEnum
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
                raise SerializationError(path=[k], reason='missing not optional')
            else:
                try:
                    r[k] = v.serialize(obj_v)
                except SerializationError as e:
                    raise e.with_path(k)

        return self.cls(**r)


@dataclass
class JsonFromStruct(JsonIntoStruct):
    __mapper_cls__ = JsonFromStructMapper

    cls: Type

    def create(self, dependencies: Fields) -> Mapper:
        return JsonFromStructMapper(
            self.cls,
            [(x, isinstance(y, JsonAnyOptional)) for x, y in self.fields],
            dependencies
        )


class JsonFromEnumMapper(JsonIntoEnumMapper):
    def serialize(self, obj: Any) -> Any:
        return self.enum(self.dependencies['value'].serialize(obj))


@dataclass
class JsonFromEnum(JsonIntoEnum):
    __mapper_cls__ = JsonFromEnumMapper


class JsonFromDateTimeMapper(JsonIntoDateTimeMapper):

    def serialize(self, obj: Any) -> Any:
        try:
            r = datetime.strptime(obj, self.format)
        except Exception as e:
            raise SerializationError(val=obj, reason=str(e))
        return r.replace(tzinfo=pytz.utc)


class JsonFromDateTime(JsonIntoDateTime):
    __mapper_cls__ = JsonFromDateTimeMapper


class JsonFromTimeDeltaMapper(Mapper):
    def serialize(self, obj: float) -> timedelta:
        try:
            obj = float(obj)
        except Exception as e:
            raise SerializationError(val=obj, reason=str(e))
        return timedelta(seconds=obj)


class JsonFromTimeDelta(Fac):
    __mapper_cls__ = JsonFromTimeDeltaMapper
