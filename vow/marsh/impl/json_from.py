from datetime import datetime, timedelta
from typing import Any

import pytz

from vow.marsh.error import SerializationError
from vow.marsh.impl.json_into import JsonIntoDateTime, \
    JsonIntoDateTimeMapper
from vow.marsh.base import Mapper, Fac


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
