import unittest

from datetime import timedelta
from typing import Optional

from dataclasses import dataclass
from xrpc.trace import trc

from vow.marsh.walker import Walker
from vow.marsh.decl import infer
from vow.marsh.impl.any import Passthrough, Ref
from vow.marsh.impl.json import JSON_FROM, JSON_INTO
from vow.marsh.impl.json_from import JsonFromTimeDelta
from vow.marsh.impl.json_into import JsonIntoStruct


@infer(JSON_INTO)
@dataclass
class Amber:
    a: int
    b: 'Bulky'


@infer(JSON_INTO)
@dataclass
class Bulky:
    b: Optional[Amber] = None


class TestApi(unittest.TestCase):
    def test_resolve_atom(self):
        self.assertEqual(
            Passthrough(str),
            Walker(JSON_FROM).resolve(str)
        )
        self.assertEqual(
            JsonFromTimeDelta(),
            Walker(JSON_FROM).resolve(timedelta)
        )

    def test_resolve_struct(self):
        @infer(JSON_INTO)
        @dataclass
        class A:
            a: int

        @infer(JSON_INTO)
        @dataclass
        class B:
            a: int
            b: A

        self.assertEqual(
            Ref(__name__ + '.A'),
            Walker(JSON_INTO).resolve(A)
        )

        self.assertEqual(
            Ref(__name__ + '.B'),
            Walker(JSON_INTO).resolve(B),
        )

        self.assertEqual(
            JsonIntoStruct([
                ('a', Passthrough(int))
            ]),
            A.__serde__[JSON_INTO],
        )

        self.assertEqual(
            JsonIntoStruct([
                ('a', Passthrough(int)),
                ('b', Ref(__name__ + '.A')),
            ]),
            B.__serde__[JSON_INTO],
        )

    def test_resolve_forward_ref(self):
        @infer(JSON_INTO)
        @dataclass
        class A:
            a: int
            b: 'B'

        @infer(JSON_INTO)
        @dataclass
        class B:
            b: A

        self.assertEqual(
            JsonIntoStruct([
                ('a', Passthrough(int)),
                ('b', Ref(__name__ + '.B')),
            ]),
            A.__serde__[JSON_INTO],
        )

    def test_mapper(self):
        ctx = Walker(JSON_INTO)
        fac = ctx.resolve(Amber)
        mapper, = ctx.mappers(fac)

        self.assertEqual(
            {
                'a': 555,
                'b': {
                    'b': None
                }
            },
            mapper.serialize(Amber(555, Bulky()))
        )

        self.assertEqual(
            {
                'a': 'asd',
                'b': {
                    'b': None
                }
            },
            mapper.serialize(Amber('asd', Bulky()))
        )