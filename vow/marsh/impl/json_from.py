from datetime import datetime, timedelta
from typing import Type, Any, List, Tuple

import pytz
from dataclasses import dataclass

from vow.marsh.error import SerializationError
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

            try:
                name, val = v.serialize(obj)
            except SerializationError as e:
                raise e.with_path(k)

            r[name] = val
        return self.cls(**r)


@dataclass
class JsonFromStruct(JsonIntoStruct):
    __mapper_cls__ = JsonFromStructMapper

    cls: Type

    def create(self, dependencies: Fields) -> Mapper:
        return self.__mapper_cls__(
            cls=self.cls,
            dependencies=dependencies,
            fields=[(str(i) if n is None else n, x) for i, (n, x, _) in enumerate(self.fields)]
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
