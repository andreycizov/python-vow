from typing import Tuple

from vow.marsh import Fac, Mapper, SerializationError
from xrpc.trace import trc


class BinaryFromVarIntMapper(Mapper):

    def serialize(self, obj: bytes) -> Tuple[int, bytes]:
        if not isinstance(obj, bytes):
            raise SerializationError(val=obj, reason='not_bytes')

        r = 0
        i = 0
        curr = obj

        trc('1').debug('%s', obj)

        while True:
            if len(curr) == 0:
                raise SerializationError(val=obj, reason='buffer_overrun')

            item, curr = curr[0], curr[1:]

            trc('2').debug(f'{r} {item} {r << 7:b} {item:b} {item & 127:b}')

            r = r | ((item & 127) << (7 * i))

            i += 1

            trc('3').debug(f'{r}')

            if item & 128 == 0:
                break

        return r, curr


class BinaryFromVarInt(Fac):
    __mapper_cls__ = BinaryFromVarIntMapper
