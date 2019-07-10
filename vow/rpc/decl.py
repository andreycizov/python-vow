import inspect

from collections import Callable
from dataclasses import dataclass, replace
from typing import Optional, List, Tuple, Any

from vow.marsh.base import Fac, Mapper
from vow.marsh.decl import get_mappers
from vow.marsh.walker import Kind, auto_callable_kind_reply


@dataclass
class MethodMeta:
    name: str
    is_async: bool
    kind: Kind
    signature: inspect.Signature
    input_into: Fac
    input_from: Fac
    output_into: Fac
    output_from: Fac

    def get_client(self) -> 'Method':
        a, b = get_mappers(self.input_into, self.output_from)
        return Method(
            self.name,
            self.is_async,
            self.kind,
            self.signature,
            input_mapper=a,
            output_mapper=b,
        )

    def get_server(self) -> 'Method':
        a, b = get_mappers(self.input_from, self.output_into)
        return Method(
            self.name,
            self.is_async,
            self.kind,
            self.signature,
            input_mapper=a,
            output_mapper=b,
        )


@dataclass
class MethodDef:
    method: MethodMeta
    callable: Callable


@dataclass
class Method:
    name: str
    is_async: bool
    kind: Kind
    signature: inspect.Signature
    input_mapper: Mapper
    output_mapper: Mapper


@dataclass
class rpc:
    is_method: bool = False
    name: Optional[str] = None
    kind: Optional[Kind] = None
    is_async: Optional[bool] = None
    signature: Optional[inspect.Signature] = None

    def to_method(self, ser_name: str, des_name: str) -> Method:
        return MethodMeta(

        )

    def __call__(self, fun):
        item: rpc = replace(self)

        if item.name is None:
            item = replace(item, name=fun.__qualname__)

        if item.kind is None:
            item = replace(item, kind=auto_callable_kind_reply(fun, is_method=item.is_method)[0])

        if item.is_async is None:
            is_async = inspect.iscoroutinefunction(fun)
            item = replace(item, is_async=is_async)

        if item.signature is None:
            signature = inspect.signature(fun)
            item = replace(item, signature=signature)

        setattr(fun, '__rpc__', item)
        return fun


def collect(obj) -> List[Tuple[Any, rpc]]:
    r: List[Tuple[rpc, Any]] = []
    for x in dir(obj):
        y = getattr(obj, x)
        if hasattr(y, '__rpc__'):
            r.append((y.__rpc__, y))

    return r
