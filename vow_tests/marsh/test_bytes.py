import json
import unittest

from vow.marsh.impl.binary_from import BinaryFromVarInt
from vow.marsh.impl.binary_into import BinaryIntoVarInt


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
            (0, b''),
            mapper.serialize(b'\x00'),
        )

        self.assertEqual(
            (300, b''),
            mapper.serialize(bytes([0b10101100, 0b00000010])),
        )

        self.assertEqual(
            (127, b''),
            mapper.serialize(b'\x7f'),
        )

        self.assertEqual(
            (128, b''),
            mapper.serialize(bytes([0b10000000, 1])),
        )

        self.assertEqual(
            (2 ** 14, b''),
            mapper.serialize(bytes([0b10000000, 0b10000000, 1])),
        )

        self.assertEqual(
            (2 ** 14 - 1, b''),
            mapper.serialize(bytes([0b11111111, 0b01111111])),
        )

        self.assertEqual(
            (2 ** 14 - 1, b'\x00'),
            mapper.serialize(bytes([0b11111111, 0b01111111, 0])),
        )
