from typing import Any

from dataclasses import dataclass
from datetime import timedelta, datetime

from vow.marsh.error import SerializationError
from vow.marsh.base import Mapper, Fac, Fields

ISO8601 = '%Y-%m-%dT%H:%M:%S.%fZ'


class JsonIntoDateTimeMapper(Mapper):

    def __init__(self, format: str, dependencies: Fields):
        self.format = format
        super().__init__(dependencies)

    def serialize(self, obj: datetime) -> Any:
        if not isinstance(obj, datetime):
            raise SerializationError(val=obj)

        return format(obj, self.format)


@dataclass
class JsonIntoDateTime(Fac):
    __mapper_cls__ = JsonIntoDateTimeMapper
    __mapper_args__ = 'format',
    format: str = ISO8601


class JsonIntoTimeDeltaMapper(Mapper):
    def serialize(self, obj: timedelta) -> Any:
        if not isinstance(obj, timedelta):
            raise SerializationError(val=obj)

        return obj.total_seconds()


class JsonIntoTimeDelta(Fac):
    __mapper_cls__ = JsonIntoTimeDeltaMapper
