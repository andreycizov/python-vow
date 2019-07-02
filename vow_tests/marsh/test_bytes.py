import json
import unittest

from xrpc.trace import trc

from vow.marsh.error import SerializationError
from vow.marsh.impl.any import AnyAnyWith, AnyAnyItem, Passthrough, AnyAnyField, AnyAnyAttr, AnyAnyLen
from vow.marsh.impl.any_from import AnyFromStruct
from vow.marsh.impl.any_into import AnyIntoStruct
from vow.marsh.impl.binary import BINARY_FROM, BinaryNext, BINARY_INTO
from vow.marsh.impl.binary_from import BinaryFromVarInt, BinaryFromBytes, BinaryFromJson, BUFFER_NEEDED
from vow.marsh.impl.binary_into import BinaryIntoVarInt, BinaryIntoJson, BinaryIntoConcat
from vow.marsh.walker import Walker


def conv(x: bytes):
    r = []
    for y in x:
        r.append(f'{y:08b}')

    return r


class TestBytes(unittest.TestCase):
    def test_varint_0(self):
        mapper = BinaryIntoVarInt().create({})

        self.assertEqual(
            conv(bytes([0])),
            conv(mapper.serialize(0)),
        )

        self.assertEqual(
            conv(bytes([0b10101100, 0b00000010])),
            conv(mapper.serialize(300)),
        )

        self.assertEqual(
            conv(b'\x7f'),
            conv(mapper.serialize(127)),
        )

        self.assertEqual(
            conv(bytes([0b10000000, 1])),
            conv(mapper.serialize(128)),
        )

        self.assertEqual(
            conv(bytes([0b10000000, 0b10000000, 1])),
            conv(mapper.serialize(2 ** 14)),
        )

        self.assertEqual(
            conv(bytes([0b11111111, 0b01111111])),
            conv(mapper.serialize(2 ** 14 - 1)),
        )

    def test_varint_1(self):
        mapper = BinaryFromVarInt().create({})

        self.assertEqual(
            BinaryNext(0, b''),
            mapper.serialize(b'\x00'),
        )

        self.assertEqual(
            BinaryNext(300, b''),
            mapper.serialize(bytes([0b10101100, 0b00000010])),
        )

        self.assertEqual(
            BinaryNext(127, b''),
            mapper.serialize(b'\x7f'),
        )

        self.assertEqual(
            BinaryNext(128, b''),
            mapper.serialize(bytes([0b10000000, 1])),
        )

        self.assertEqual(
            BinaryNext(2 ** 14, b''),
            mapper.serialize(bytes([0b10000000, 0b10000000, 1])),
        )

        self.assertEqual(
            BinaryNext(2 ** 14 - 1, b''),
            mapper.serialize(bytes([0b11111111, 0b01111111])),
        )

        self.assertEqual(
            BinaryNext(2 ** 14 - 1, b'\x00'),
            mapper.serialize(bytes([0b11111111, 0b01111111, 0])),
        )

    def test_body_1(self):
        fac_from = AnyAnyWith(
            BinaryFromVarInt(),
            AnyAnyWith(
                BinaryFromBytes(
                    AnyAnyAttr('val', Passthrough(int)),
                    AnyAnyAttr('next', Passthrough())
                ),
                AnyFromStruct(
                    [
                        AnyAnyField(
                            'val',
                            BinaryFromJson(
                                AnyAnyAttr('val')
                            )
                        ),
                        AnyAnyField('next', AnyAnyAttr('next'))
                    ],
                    cls=BinaryNext
                )
            )
        )

        fac_into = AnyAnyWith(
            BinaryIntoJson(Passthrough()),
            BinaryIntoConcat([
                AnyAnyWith(
                    AnyAnyLen(Passthrough()),
                    BinaryIntoVarInt(),
                ),
                Passthrough()
            ])
        )

        bin_from, = Walker(BINARY_FROM).mappers(fac_from)
        bin_into, = Walker(BINARY_INTO).mappers(fac_into)

        x = bin_from.serialize(b'\x02{}abc')

        trc('0').debug(x)

        self.assertEqual(
            BinaryNext({}, b'abc'),
            x,
        )

        try:
            x = bin_from.serialize(b'\x55{}abc')
        except SerializationError as e:
            self.assertEqual(BUFFER_NEEDED, e.reason)

        self.assertEqual(
            b'\x04null',
            bin_into.serialize(None)
        )
