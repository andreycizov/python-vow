from typing import Tuple, Any

from vow.marsh import Fac, Mapper, SerializationError

BUFFER_NEEDED = 'buffer_overrun'


class BinaryFromVarIntMapper(Mapper):

    def serialize(self, obj: bytes) -> Tuple[int, bytes]:
        if not isinstance(obj, bytes):
            raise SerializationError(val=obj, reason='not_bytes')

        r = 0
        i = 0
        curr = obj

        while True:
            if len(curr) == 0:
                raise SerializationError(val=obj, reason=BUFFER_NEEDED)

            item, curr = curr[0], curr[1:]

            r = r | ((item & 127) << (7 * i))

            i += 1

            if item & 128 == 0:
                break

        return r, curr


class BinaryFromVarInt(Fac):
    __mapper_cls__ = BinaryFromVarIntMapper


class BinaryFromBytesMapper(Mapper):
    def serialize(self, obj: Tuple[int, bytes]) -> Tuple[bytes, bytes]:
        size, body = obj

        if not isinstance(size, int):
            raise SerializationError(val=obj, reason='not_int')

        if not isinstance(body, bytes):
            raise SerializationError(val=obj, reason='not_bytes')

        if len(body) < size:
            raise SerializationError(val=obj, reason=BUFFER_NEEDED)

        return body[:size], body[size:]


class BinaryFromBytes(Fac):
    __mapper_cls__ = BinaryFromBytesMapper
