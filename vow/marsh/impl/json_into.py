from collections import OrderedDict
from enum import Enum
from typing import Any, List, Tuple, Optional

from dataclasses import dataclass, field
from datetime import timedelta, datetime

from vow.marsh.error import SerializationError
from vow.marsh.base import Mapper, Fac, FieldsFac, Fields
from vow.marsh.impl.any import Passthrough


class JsonIntoStructMapper(Mapper):

    def __init__(self, fields: List[Tuple[str, bool]], dependencies: Fields):
        self.fields = fields
        super().__init__(dependencies)

    def serialize(self, obj: Any) -> Any:
        r = OrderedDict()

        for k, _ in self.fields:
            v = self.dependencies[k]

            try:
                name, val = v.serialize(obj)
            except SerializationError as e:
                raise e.with_path(k)

            r[name] = val
        return r


@dataclass
class JsonIntoStruct(Fac):
    __mapper_cls__ = JsonIntoStructMapper

    fields: List[Tuple[Optional[str], bool, Fac]]

    def create(self, dependencies: Fields) -> Mapper:
        return self.__mapper_cls__(
            dependencies=dependencies,
            fields=[(str(i) if n is None else n, x) for i, (n, x, _) in enumerate(self.fields)]
        )

    def dependencies(self) -> FieldsFac:
        return {str(i) if n is None else n: v for i, (n, _, v) in enumerate(self.fields)}


class JsonIntoEnumMapper(Mapper):

    def __init__(self, enum: Enum, dependencies: Fields):
        self.enum = enum
        super().__init__(dependencies)

    def serialize(self, obj: Any) -> Any:
        obj: Enum
        return self.dependencies['value'].serialize(obj.value)


@dataclass
class JsonIntoEnum(Fac):
    __mapper_cls__ = JsonIntoEnumMapper
    __mapper_args__ = 'enum',

    enum: Enum
    value: Fac = field(default_factory=lambda: Passthrough(str))

    def create(self, dependencies: Fields) -> Mapper:
        return self.__mapper_cls__(enum=self.enum, dependencies=dependencies)

    def dependencies(self) -> FieldsFac:
        return {'value': self.value}


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
