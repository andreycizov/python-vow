import unittest

from datetime import timedelta
from typing import Optional

from dataclasses import dataclass

from vow.marsh import SerializationError
from xrpc.trace import trc

from vow.marsh.walker import Walker
from vow.marsh.decl import infer
from vow.marsh.impl.any import Passthrough, Ref
from vow.marsh.impl.json import JSON_FROM, JSON_INTO
from vow.marsh.impl.json_from import JsonFromTimeDelta
from vow.marsh.impl.any_into import AnyIntoStruct


@infer(JSON_INTO, JSON_FROM)
@dataclass
class Amber:
    a: int
    b: 'Bulky'


@infer(JSON_INTO, JSON_FROM)
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
            AnyIntoStruct([
                ('a', False, Passthrough(int))
            ]),
            A.__serde__[JSON_INTO],
        )

        self.assertEqual(
            AnyIntoStruct([
                ('a', False, Passthrough(int)),
                ('b', True, Ref(__name__ + '.A')),
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
            AnyIntoStruct([
                ('a', False, Passthrough(int)),
                ('b', True, Ref(__name__ + '.B')),
            ]),
            A.__serde__[JSON_INTO],
        )

    def test_mapper_into(self):
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

        try:
            self.assertEqual(
                {
                    'a': 'asd',
                    'b': {
                        'b': None
                    }
                },
                mapper.serialize(Amber('asd', Bulky()))
            )
        except SerializationError as e:
            self.assertEqual(['a', '$field', '$attr'], e.path)

    def test_mapper_from(self):
        ctx = Walker(JSON_FROM)
        fac = ctx.resolve(Amber)
        mapper, = ctx.mappers(fac)

        self.assertEqual(
            Amber(555, Bulky()),
            mapper.serialize({
                'a': 555,
                'b': {
                    'b': None
                }
            })
        )

        try:
            self.assertEqual(
                Amber('asd', Bulky()),
                mapper.serialize({
                    'a': 'asd',
                    'b': {
                        'b': None
                    }
                })
            )
        except SerializationError as e:
            self.assertEqual(['a', '$field', '$item'], e.path)
