import inspect

from dataclasses import dataclass, replace, field
from typing import Optional, List, Tuple, Any

from vow.marsh.base import Mapper
from vow.marsh.decl import get_mappers, get_factories_il, infer
from vow.marsh.impl.json import JSON_INTO, JSON_FROM
from vow.marsh.walker import Kind, auto_callable_kind_reply, Return, Args


@dataclass
class Method:
    name: str
    is_async: bool
    kind: Kind
    signature: inspect.Signature
    input_into: Mapper
    input_from: Mapper
    output_into: Mapper
    output_from: Mapper


@dataclass
class rpc:
    # we could say we only support classes as RPCs!

    is_method: bool = True
    name: Optional[str] = None
    kind: Optional[Kind] = None
    is_async: Optional[bool] = None
    signature: Optional[inspect.Signature] = None
    serializers: List[str] = field(default_factory=lambda: [JSON_FROM, JSON_INTO])

    def to_method(self, fun, ser_name: str, des_name: str) -> 'Method':
        # needs the callable itself to extract the serializers

        input_into, output_into, input_from, output_from = get_mappers(
            get_factories_il(ser_name, Args(fun)),
            get_factories_il(ser_name, Return(fun)),
            get_factories_il(des_name, Args(fun)),
            get_factories_il(des_name, Return(fun)),
        )

        return Method(
            name=self.name,
            is_async=self.is_async,
            kind=self.kind,
            signature=self.signature,
            input_into=input_into,
            output_into=output_into,
            input_from=input_from,
            output_from=output_from
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

        fun = infer(*self.serializers, is_method=self.is_method)(fun)

        return fun


def collect(obj) -> List[Tuple[Any, rpc]]:
    r: List[Tuple[rpc, Any]] = []
    for x in dir(obj):
        y = getattr(obj, x)
        if hasattr(y, '__rpc__'):
            r.append((y.__rpc__, y))

    return r
