import inspect
from importlib import import_module

import typing
from collections import Callable, OrderedDict
from dataclasses import dataclass
from typing_inspect import get_args

from vow.marsh.base import Fac
from vow.marsh.impl.any import AnyAnyField, AnyAnyItem, AnyAnyAttr
from vow.marsh.impl.any_from import AnyFromStruct
from vow.marsh.impl.any_into import AnyIntoStruct
from vow.marsh.impl.json import JSON_INTO, JSON_FROM
from vow.marsh.walker import Walker, Kind
from xrpc.trace import trc


@dataclass
class Method:
    name: str
    kind: Kind
    input: Fac
    output: Fac


@dataclass
class MethodCallable:
    method: Method
    callable: Callable


def _load_fun_parameters(fun, is_method=False):
    signature = inspect.signature(fun)

    if signature.return_annotation == inspect.Signature.empty:
        raise ValueError('Must supply return value')

    params = OrderedDict()

    trc().debug('%s %s %s %s %s',
                inspect.ismethod(fun),
                inspect.ismethoddescriptor(fun),
                inspect.ismemberdescriptor(fun),
                inspect.isgetsetdescriptor(fun),
                is_method
                )

    trc().debug('%s', dir(fun))
    trc('c').debug('%s', getattr(fun, '__class__', None))
    trc('c').debug('%s', getattr(fun, '__module__', None))
    trc('c').debug('%s', getattr(fun, '__qualname__', None))
    trc('c').debug('%s', getattr(fun, '__name__', None))

    for i, (_, param) in enumerate(signature.parameters.items()):
        param: inspect.Parameter

        if is_method and i == 0:
            continue

        if param.annotation == inspect.Parameter.empty:
            trc().debug('%s', signature)
            raise ValueError(f'No type for `{param.name}`')

        if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
            params[param.name] = param.annotation
        elif param.kind == inspect.Parameter.VAR_POSITIONAL:
            params[param.name] = typing.List[param.annotation]
        elif param.kind == inspect.Parameter.KEYWORD_ONLY:
            params[param.name] = param.annotation
        elif param.kind == inspect.Parameter.VAR_KEYWORD:
            params[param.name] = typing.Dict[str, param.annotation]
        else:
            raise ValueError(f'Not supported: {param}')

    trc().debug('%s', params)

    return params, signature.return_annotation


def auto_callable_fac(walker_args: Walker, walker_ret: Walker, fun, is_method=False):
    params, reply = _load_fun_parameters(fun, is_method)

    _, reply = auto_callable_kind_reply(fun, is_method=is_method)

    reply = walker_ret.resolve(reply)

    params_fac = []

    for name, param in params.items():
        item_factory = walker_args.resolve(param)

        if walker_args.name == JSON_INTO:
            params_fac.append(AnyAnyField(
                name,
                AnyAnyAttr(
                    name,
                    item_factory
                ),
            ))
        elif walker_args.name == JSON_FROM:
            params_fac.append(AnyAnyField(
                name,
                AnyAnyItem(
                    name,
                    item_factory
                ),
            ))
        else:
            raise NotImplementedError(walker_args.name)

    if walker_args.name == JSON_INTO:
        struct = AnyIntoStruct(params_fac)
    elif walker_args.name == JSON_FROM:
        struct = AnyFromStruct(params_fac)
    else:
        raise NotImplementedError(walker_args.name)

    return struct, reply


def auto_callable_kind_reply(fun, is_method=False):
    _, reply = _load_fun_parameters(fun, is_method)

    kind = Kind.Call

    # the issue of setting __serde__ on function
    # is that of it having 2 actual values:
    # - the arguments (into/from)
    # - return (into/from)

    if hasattr(reply, '__origin__'):
        if issubclass(reply.__origin__, typing.AsyncIterable) or issubclass(reply.__origin__, typing.Iterable):
            kind = Kind.Iterator
            vt, = get_args(reply)

            reply = vt

    return kind, reply
