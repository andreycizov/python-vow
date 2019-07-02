import json
from typing import Any, List

from dataclasses import dataclass, field

from vow.marsh.base import FieldsFac, Mapper, Fac
from vow.marsh.error import subserializer, SerializationError


class BinaryIntoVarIntMapper(Mapper):

    def serialize(self, obj: Any) -> Any:
        if not isinstance(obj, int):
            raise SerializationError(val=obj, reason='not_int', origin=self)

        r = []

        while True:

            next_obj = obj >> 7

            if next_obj == 0:
                r.append(obj)
                break
            else:
                r.append(obj & 127 | 128)
                obj = next_obj

        return bytes(r)


class BinaryIntoVarInt(Fac):
    __mapper_cls__ = BinaryIntoVarIntMapper


class BinaryIntoJsonMapper(Mapper):

    def serialize(self, obj: Any) -> Any:
        with subserializer('$body'):
            val = self.dependencies['body'].serialize(obj)

        try:
            return json.dumps(val).encode()
        except Exception as e:
            raise SerializationError(val=obj, reason='json', exc=e, origin=self)


@dataclass
class BinaryIntoJson(Fac):
    __mapper_cls__ = BinaryIntoJsonMapper
    body: Fac

    def dependencies(self) -> FieldsFac:
        return {'body': self.body}


class BinaryIntoConcatMapper(Mapper):

    def serialize(self, obj: Any) -> Any:
        r = b''
        for x in range(len(self.dependencies)):
            with subserializer(str(x)):
                r += self.dependencies[str(x)].serialize(obj)

        return r


@dataclass
class BinaryIntoConcat(Fac):
    __mapper_cls__ = BinaryIntoConcatMapper

    items: List[Fac] = field(default_factory=list)

    def dependencies(self) -> FieldsFac:
        return {str(i): v for i, v in enumerate(self.items)}
