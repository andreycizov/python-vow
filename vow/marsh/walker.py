import inspect
import sys
import typing
from collections import deque, OrderedDict
from datetime import datetime, timedelta
from enum import Enum
from itertools import count
from typing import Type, Any, List, Dict, Tuple, Union, Deque

from dataclasses import is_dataclass, dataclass, fields, Field, field, MISSING
from typing_inspect import is_optional_type, get_args, get_last_args

from vow.marsh.helper import is_serializable, DECL_ATTR, FIELD_FACTORY, FIELD_OVERRIDE, DECL_CALLABLE_ATTR

from vow.marsh.impl.any import This, Ref, AnyAnyAttr, AnyAnyItem, AnyAnyField
from vow.marsh.impl.json_from import JsonFromDateTime, JsonFromTimeDelta
from vow.marsh.impl.any_from import AnyFromStruct, AnyFromEnum
from vow.marsh.impl.json_into import JsonIntoDateTime, JsonIntoTimeDelta
from vow.marsh.impl.any_into import AnyIntoStruct, AnyIntoEnum
from vow.marsh.impl.json import JsonAnyList, JsonAnyDict, JsonAnyOptional, JSON_INTO, JSON_FROM
from vow.marsh.base import Fac, Mapper
from xrpc.trace import trc

if sys.version_info >= (3, 7):
    from typing import ForwardRef
else:
    from typing import _ForwardRef as ForwardRef


class Kind(Enum):
    Iterator = 1
    Call = 2


@dataclass
class CallableSerializers:
    kind: Kind
    parameters: Dict[str, Fac] = field(default_factory=dict)
    return_: Dict[str, Fac] = field(default_factory=dict)


@dataclass
class DeferredWrapper:
    type: Type


@dataclass
class Deferred:
    ctx: 'Walker'
    obj: Any

    def execute(self) -> Fac:
        return self.ctx.resolve(DeferredWrapper(self.obj))


@dataclass
class Serializers:
    items: Dict[str, Union[Deferred, Fac]] = field(default_factory=dict)

    def __getitem__(self, key) -> Fac:
        x = self.items[key]
        if isinstance(x, Deferred):
            self.items[key] = x.execute()
            x = self.items[key]

        return x

    def __setitem__(self, key, value: Union[Deferred, Fac]):
        assert key not in self.items, key
        self.items[key] = value

    @classmethod
    def assign(cls, obj) -> 'Serializers':
        if not hasattr(obj, DECL_ATTR):
            setattr(obj, DECL_ATTR, Serializers())

        return getattr(obj, DECL_ATTR)


@dataclass
class Return:
    type: Type


@dataclass
class Args:
    type: Any


VisitId = int
NodeId = int
DepId = str

VisitNodeDict = Dict[VisitId, NodeId]
NodeObjsDict = Dict[NodeId, Tuple[List[DepId], Fac, Dict[DepId, VisitId]]]


@dataclass
class Walker:
    """Automatically build serialization trees for given objects"""

    name: str
    frame: Any

    @property
    def globals(self):
        return self.frame.f_globals

    @property
    def locals(self):
        return self.frame.f_locals

    def __init__(self, name: str, frame=None):
        self.name = name
        self.frame = inspect.currentframe().f_back if frame is None else frame

    def resolve(self, cls: Type) -> Fac:
        """get a factory given an object cls"""

        # todo this needs to go, and we need to have proper instances of each separate `cls` mapper
        #

        def resolve_list(cls):
            vt, = get_args(cls)

            value = self.resolve(vt)

            if self.name.startswith('json'):
                return JsonAnyList(value)
            else:
                raise NotImplementedError(f'{self.name}')

        def resolve_dict(cls):
            kt, vt = get_args(cls)

            if self.name == 'json_into':
                assert issubclass(kt, str), (kt,)

            key = self.resolve(kt)
            value = self.resolve(vt)

            if self.name.startswith('json'):
                return JsonAnyDict(key, value)
            else:
                raise NotImplementedError(f'{self.name}')

        if isinstance(cls, DeferredWrapper):
            cls = cls.type
            assert is_dataclass(cls), cls
            r: List[Fac] = []

            for item in fields(cls):
                item: Field

                item_type = item.metadata.get(FIELD_OVERRIDE, item.type)
                item_factory = item.metadata.get(FIELD_FACTORY, {})

                if isinstance(item_factory, Fac):
                    pass
                elif isinstance(item_factory, dict):
                    item_factory = item_factory.get(self.name, MISSING)
                else:
                    assert False, repr(item_factory)

                if item_factory is MISSING:
                    item_factory = self.resolve(item_type)

                is_optional = item.default is not MISSING
                # todo support optional

                if self.name == 'json_from':
                    r.append(
                        AnyAnyField(
                            item.name,
                            AnyAnyItem(
                                item.name,
                                item_factory
                            )
                        )
                    )
                elif self.name == 'json_into':
                    r.append(
                        AnyAnyField(
                            item.name,
                            AnyAnyAttr(
                                item.name,
                                item_factory
                            )
                        )
                    )
                else:
                    raise NotImplementedError((self.name, None))

            if self.name == 'json_from':
                return AnyFromStruct(
                    r,
                    cls
                )
            elif self.name == 'json_into':
                return AnyIntoStruct(
                    r,
                    cls,
                )
            else:
                raise NotImplementedError((self.name, None))
        elif isinstance(cls, Args):
            dx: CallableSerializers = getattr(cls.type, DECL_CALLABLE_ATTR)
            return dx.parameters[self.name]
        elif isinstance(cls, Return):
            dx: CallableSerializers = getattr(cls.type, DECL_CALLABLE_ATTR)
            return dx.return_[self.name]
        elif is_serializable(cls):
            return Ref(self.name, cls.__module__ + '.' + cls.__name__)
        elif inspect.isclass(cls):
            if issubclass(cls, bool):
                return This(bool)
            elif issubclass(cls, float):
                return This(float)
            elif issubclass(cls, int):
                return This(int)
            elif issubclass(cls, str):
                return This(str)
            elif issubclass(cls, datetime):
                if self.name == 'json_from':
                    return JsonFromDateTime()
                elif self.name == 'json_into':
                    return JsonIntoDateTime()
                else:
                    raise NotImplementedError(f'{self.name}')
            elif issubclass(cls, timedelta):
                if self.name == 'json_from':
                    return JsonFromTimeDelta()
                elif self.name == 'json_into':
                    return JsonIntoTimeDelta()
                else:
                    raise NotImplementedError(f'{self.name}')
            elif issubclass(cls, List):
                return resolve_list(cls)
            elif issubclass(cls, Dict):
                return resolve_dict(cls)
            elif issubclass(cls, Enum):
                clss = set(item.value.__class__ for item in cls)

                assert len(clss) == 1, clss

                t = clss.pop()

                fac = self.resolve(t)

                if self.name == 'json_from':
                    return AnyFromEnum(cls, fac)
                elif self.name == 'json_into':
                    return AnyIntoEnum(cls, fac)
                else:
                    raise NotImplementedError(('class4', cls))
            elif is_dataclass(cls):
                return Ref(self.name, cls.__module__ + '.' + cls.__name__)
            else:
                raise NotImplementedError(('class2', cls))
        elif isinstance(cls, ForwardRef):
            if sys.version_info >= (3, 7):
                t = cls._evaluate(self.globals, self.locals)
            else:
                t = cls._eval_type(self.globals, self.locals)
            return self.resolve(t)
        elif isinstance(cls, str):
            code = compile(cls, '<string>', 'eval')
            t = eval(code, self.globals, self.locals)
            return self.resolve(t)
        else:
            if is_optional_type(cls):
                if sys.version_info >= (3, 7):
                    item, _ = get_args(cls)
                else:
                    item, _ = get_last_args(cls)

                value = self.resolve(item)

                if self.name.startswith('json'):
                    return JsonAnyOptional(value)
                else:
                    raise NotImplementedError(f'{self.name}')
            elif sys.version_info >= (3, 7) and hasattr(cls, '__origin__'):
                if cls.__origin__ is list:
                    return resolve_list(cls)
                elif cls.__origin__ is dict:
                    return resolve_dict(cls)
            else:
                raise NotImplementedError(('class', cls, cls.__class__))

    def _visit_objs(self, *roots: Fac) -> Tuple[NodeObjsDict, VisitNodeDict, List[NodeId]]:
        visit_node: VisitNodeDict = {}
        name_node: Dict[str, NodeId] = {}

        node_objs: NodeObjsDict = {}
        to_visit: Deque[Tuple[VisitId, List[DepId], Fac]] = deque()

        visit_ctr = count()
        node_ctr = count()

        def do_visit(visit_id: VisitId, path: List[DepId], obj: Fac):

            should_early = False

            if isinstance(obj, Ref):
                if obj.full_name not in name_node:
                    node_id = next(node_ctr)
                    name_node[obj.full_name] = node_id

                    obj = obj.resolve()
                else:
                    node_id = name_node[obj.full_name]
                    should_early = True
            else:
                node_id = next(node_ctr)

            visit_node[visit_id] = node_id

            if should_early:
                return

            deps = {}

            for k, v in obj.dependencies().items():
                dep_visit_id = next(visit_ctr)
                deps[k] = dep_visit_id

                to_visit.append((dep_visit_id, path + [k], v))

            node_objs[node_id] = path, obj, deps

        root_visit_ids = []

        for root in roots:
            root_visit_id = next(visit_ctr)
            root_visit_ids.append(root_visit_id)
            do_visit(root_visit_id, [], root)

        while len(to_visit):
            args = to_visit.popleft()
            do_visit(*args)

        node_objs = {k: (*v, {k2: visit_node[v2] for k2, v2 in deps.items()}) for k, (*v, deps) in node_objs.items()}

        root_nodes = [visit_node[visit_id] for visit_id in root_visit_ids]

        return node_objs, visit_node, root_nodes

    def mappers(self, *roots: Fac) -> List[Mapper]:
        node_objs, visit_node, root_nodes = self._visit_objs(*roots)

        node_deps_empty = {k: {} for k in node_objs.keys()}
        node_mapper = {k: fac.create(node_deps_empty[k]) for k, (_, fac, _) in node_objs.items()}

        for k, deps in node_deps_empty.items():
            _, _, deps_map = node_objs[k]

            for dep_k, dep_n in deps_map.items():
                deps[dep_k] = node_mapper[dep_n]

        root_mappers = [node_mapper[node_id] for node_id in root_nodes]

        return root_mappers


def _load_fun_parameters(fun, is_method=False):
    signature = inspect.signature(fun)

    if signature.return_annotation == inspect.Signature.empty:
        raise ValueError('Must supply return value')

    params = OrderedDict()

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