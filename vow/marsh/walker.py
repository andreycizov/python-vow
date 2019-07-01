import inspect
import sys
from collections import deque
from datetime import datetime, timedelta
from enum import Enum
from itertools import count
from typing import Type, Any, List, Dict, Tuple, Union, Deque

from dataclasses import is_dataclass, dataclass, fields, Field, field, MISSING
from typing_inspect import is_optional_type, get_args, get_last_args

from vow.marsh.helper import is_serializable, DECL_ATTR, FIELD_FACTORY, FIELD_OVERRIDE
from xrpc.trace import trc

from vow.marsh.impl.any import Passthrough, Ref
from vow.marsh.impl.json_from import JsonFromDateTime, JsonFromTimeDelta, \
    JsonFromStruct, JsonFromEnum
from vow.marsh.impl.json_into import JsonIntoDateTime, JsonIntoTimeDelta, \
    JsonIntoStruct, JsonIntoEnum
from vow.marsh.impl.json import JsonAnyList, JsonAnyDict, JsonAnyOptional
from vow.marsh.base import Fac, Mapper

if sys.version_info >= (3, 7):
    from typing import ForwardRef
else:
    from typing import _ForwardRef as ForwardRef


@dataclass
class Deferred:
    ctx: 'Walker'
    obj: Any

    def execute(self) -> Fac:
        return self.ctx.resolve(DataclassWrapper(self.obj))


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
class DataclassWrapper:
    type: Type


@dataclass
class Walker:
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
        if isinstance(cls, DataclassWrapper):
            cls = cls.type
            assert is_dataclass(cls), cls
            r: List[Tuple[str, Fac]] = []

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

                r.append((
                    item.name,
                    item_factory
                ))

            if self.name == 'json_from':
                return JsonFromStruct(
                    r,
                    cls
                )
            elif self.name == 'json_into':
                return JsonIntoStruct(
                    r,
                )
            else:
                raise NotImplementedError((self.name, None))
        elif is_serializable(cls):
            return Ref(cls.__module__ + '.' + cls.__name__)
        elif inspect.isclass(cls):
            if issubclass(cls, bool):
                return Passthrough(bool)
            elif issubclass(cls, float):
                return Passthrough(float)
            elif issubclass(cls, int):
                return Passthrough(int)
            elif issubclass(cls, str):
                return Passthrough(str)
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
                vt, = get_args(cls)

                value = self.resolve(vt)

                if self.name.startswith('json'):
                    return JsonAnyList(value)
                else:
                    raise NotImplementedError(f'{self.name}')
            elif issubclass(cls, Dict):
                kt, vt = get_args(cls)

                if self.name == 'json_into':
                    assert issubclass(kt, str), (kt,)

                key = self.resolve(kt)
                value = self.resolve(vt)

                if self.name.startswith('json'):
                    return JsonAnyDict(key, value)
                else:
                    raise NotImplementedError(f'{self.name}')
            elif issubclass(cls, Enum):
                clss = set(item.value.__class__ for item in cls)

                assert len(clss) == 1, clss

                t = clss.pop()

                fac = self.resolve(t)

                if self.name == 'json_from':
                    return JsonFromEnum(cls, fac)
                elif self.name == 'json_into':
                    return JsonIntoEnum(cls, fac)
                else:
                    raise NotImplementedError(('class4', cls))
            elif is_dataclass(cls):
                return Ref(cls.__module__ + '.' + cls.__name__)
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
            else:
                raise NotImplementedError(('class', cls, cls.__class__))

    def mappers(self, *roots: Fac) -> List[Mapper]:
        VisitId = int
        NodeId = int
        DepId = str

        visit_node: Dict[VisitId, NodeId] = {}
        name_node: Dict[str, NodeId] = {}

        node_objs: Dict[NodeId, Tuple[List[DepId], Fac, Dict[DepId, VisitId]]] = {}
        to_visit: Deque[Tuple[VisitId, List[DepId], Fac]] = deque()

        visit_ctr = count()
        node_ctr = count()

        def do_visit(visit_id: VisitId, path: List[DepId], obj: Fac):

            should_early = False

            if isinstance(obj, Ref):
                if obj.item not in name_node:
                    node_id = next(node_ctr)
                    name_node[obj.item] = node_id

                    obj = obj.resolve(self.name)
                else:
                    node_id = name_node[obj.item]
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

        node_deps_empty = {k: {} for k in node_objs.keys()}
        node_mapper = {k: fac.create(node_deps_empty[k]) for k, (_, fac, _) in node_objs.items()}

        for k, deps in node_deps_empty.items():
            _, _, deps_map = node_objs[k]

            for dep_k, dep_n in deps_map.items():
                deps[dep_k] = node_mapper[dep_n]

        root_nodes = [visit_node[visit_id] for visit_id in root_visit_ids]
        root_mappers = [node_mapper[node_id] for node_id in root_nodes]

        return root_mappers


