import json
from typing import Tuple, Any

from dataclasses import dataclass

from vow.marsh.base import FieldsFac, Mapper, Fac
from vow.marsh.error import subserializer, SerializationError
from vow.marsh.impl.binary import BinaryNext

BUFFER_NEEDED = 'buffer_overrun'


class BinaryFromVarIntMapper(Mapper):

    def serialize(self, obj: bytes) -> BinaryNext:
        if not isinstance(obj, bytes) and not isinstance(obj, memoryview):
            raise SerializationError(val=obj, reason='not_bytes', origin=self)

        r = 0
        i = 0
        curr = memoryview(obj)

        while True:
            if len(curr) == 0:
                raise SerializationError(val=obj, reason=BUFFER_NEEDED, origin=self)

            item, curr = curr[0], curr[1:]

            r = r | ((item & 127) << (7 * i))

            i += 1

            if item & 128 == 0:
                break

        return BinaryNext(r, curr)


class BinaryFromVarInt(Fac):
    __mapper_cls__ = BinaryFromVarIntMapper


class BinaryFromBytesMapper(Mapper):
    def serialize(self, obj: Tuple[int, bytes]) -> BinaryNext:
        with subserializer('$size'):
            size = self.dependencies['size'].serialize(obj)

        with subserializer('$body'):
            body = self.dependencies['body'].serialize(obj)

        if not isinstance(size, int):
            raise SerializationError(val=obj, reason='not_int', origin=self)

        if not isinstance(body, bytes) and not isinstance(body, memoryview):
            raise SerializationError(val=obj, reason='not_bytes', origin=self)

        if len(body) < size:
            raise SerializationError(val=obj, reason=BUFFER_NEEDED, origin=self)

        return BinaryNext(body[:size], body[size:])


@dataclass
class BinaryFromBytes(Fac):
    __mapper_cls__ = BinaryFromBytesMapper

    size: Fac
    body: Fac

    def dependencies(self) -> FieldsFac:
        return {'size': self.size, 'body': self.body}


class BinaryFromJsonMapper(Mapper):

    def serialize(self, obj: Any) -> Any:
        with subserializer('$body'):
            val = self.dependencies['body'].serialize(obj)

        try:
            return json.loads(bytes(val))
        except Exception as e:
            raise SerializationError(val=obj, reason='json', exc=e, origin=self)


@dataclass
class BinaryFromJson(Fac):
    __mapper_cls__ = BinaryFromJsonMapper
    body: Fac

    def dependencies(self) -> FieldsFac:
        return {'body': self.body}
