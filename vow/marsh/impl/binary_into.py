from typing import Any

from vow.marsh import Fac, Mapper, SerializationError


class BinaryIntoVarIntMapper(Mapper):

    def serialize(self, obj: Any) -> Any:
        if not isinstance(obj, int):
            raise SerializationError(val=obj, reason='not_int')

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
