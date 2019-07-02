from typing import Any

from dataclasses import dataclass

from vow.marsh.error import SerializationError
from vow.marsh.impl.any_into import AnyIntoStructMapper, AnyIntoStruct, AnyIntoEnumMapper, AnyIntoEnum


class AnyFromStructMapper(AnyIntoStructMapper):

    def serialize(self, obj: Any) -> Any:
        r = {}
        for k, is_nullable in self.fields:
            v = self.dependencies[k]

            try:
                name, val = v.serialize(obj)
            except SerializationError as e:
                raise e.with_path(k)

            r[name] = val

        if self.cls:
            return self.cls(**r)
        else:
            return r


@dataclass
class AnyFromStruct(AnyIntoStruct):
    __mapper_cls__ = AnyFromStructMapper


class AnyFromEnumMapper(AnyIntoEnumMapper):
    def serialize(self, obj: Any) -> Any:
        try:
            obj2 = self.dependencies['value'].serialize(obj)
        except SerializationError as e:
            raise e.with_path('$value')

        try:
            return self.enum(obj2)
        except Exception as e:
            raise SerializationError(path=['$enum'], val=obj, exc=e, reason=f'invalid_enum_key', origin=self)


@dataclass
class AnyFromEnum(AnyIntoEnum):
    __mapper_cls__ = AnyFromEnumMapper