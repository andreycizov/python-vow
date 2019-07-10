import unittest
from inspect import signature

from typing import AsyncIterable, Iterable

from dataclasses import dataclass, replace

from vow.marsh.decl import infer, get_serializers
from vow.marsh.impl.json import JSON_FROM, JSON_INTO
from vow.marsh.walker import Args
from vow.rpc.decl import rpc
from vow.rpc.wire import Header
from xrpc.trace import trc


@dataclass
class Tumblr:
    a: int = 5
    c: int = 7


class Methodist:
    @rpc()
    def meth1(self, a: int, b: str) -> Header:
        pass

    @rpc()
    def meth2(self, a: int, b: Tumblr) -> Tumblr:
        return replace(b, c=b.c + 1)


class TestDecl(unittest.TestCase):
    def test_decl_1(self):
        @rpc()
        @infer(JSON_INTO, JSON_FROM)
        async def func1(a: int, b: str) -> AsyncIterable[Header]:
            pass

        @rpc()
        @infer(JSON_INTO, JSON_FROM)
        def func2(a: int, b: str) -> Iterable[Header]:
            pass

        @rpc()
        @infer(JSON_INTO, JSON_FROM)
        def func3(a: int, b: str) -> Header:
            pass

        @rpc()
        @infer(JSON_INTO, JSON_FROM)
        def func4(a: int, b: str) -> AsyncIterable[Header]:
            pass

        def func5(a: int, b) -> AsyncIterable[Header]:
            pass

        @rpc()
        @infer(JSON_INTO, JSON_FROM)
        def func6(a: int, *b: int, d: int = 6, **kwargs: Header) -> AsyncIterable[Header]:
            pass

        # signature(func6).bind() -> raises TypeError

        def bind_pars(fun, *args, **kwargs):
            r = signature(fun).bind(*args, **kwargs)
            r.apply_defaults()
            return r.arguments

        args = bind_pars(func6, 1, 2, 3, k={'name': 'b', 'value': None}, d=5)

        map_argsm1, = get_serializers(JSON_FROM, Args(Methodist.meth1))
        map_args6, = get_serializers(JSON_FROM, Args(func6))

        trc().debug('%s', args)
        trc().debug('%s', map_args6.serialize(args))

        # trc().debug('%s %s', fac_args6, fac_ret6)
        trc().debug('%s', signature(func6).bind(**map_args6.serialize(args)))
