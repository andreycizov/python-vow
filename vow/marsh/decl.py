import inspect

from typing import List, Type, Any

from dataclasses import dataclass, is_dataclass, field

from vow.marsh.base import Mapper, Fac
from vow.marsh.helper import DECL_CALLABLE_ATTR
from vow.marsh.walker import Walker, Serializers, Deferred, CallableSerializers, auto_callable_fac, \
    auto_callable_kind_reply


@dataclass
class infer:
    ctxs: List[Walker] = field(default_factory=list)
    is_method: bool = False

    def __init__(self, *names: str, is_method=False):
        frame = inspect.currentframe().f_back
        self.ctxs = [Walker(name, frame=frame) for name in names]
        self.is_method = is_method

    def __call__(self, obj):
        if is_dataclass(obj):
            for ctx in self.ctxs:
                serde = Serializers.assign(obj)
                serde[ctx.name] = Deferred(ctx, obj)
        else:
            # assume the object is a callable
            if not hasattr(obj, DECL_CALLABLE_ATTR):
                kind, _ = auto_callable_kind_reply(obj, is_method=self.is_method)
                setattr(obj, DECL_CALLABLE_ATTR, CallableSerializers(kind))

            dx: CallableSerializers = getattr(obj, DECL_CALLABLE_ATTR)

            for ctx in self.ctxs:
                pars, rep = auto_callable_fac(ctx, ctx, obj, is_method=self.is_method)

                dx.parameters[ctx.name] = pars
                dx.return_[ctx.name] = rep

        return obj


def get_serializers(name: str, *clss: Any) -> List[Mapper]:
    walker = Walker(name)

    factories = [walker.resolve(cls) for cls in clss]

    walker = Walker(None)

    return walker.mappers(*factories)


def get_mappers(*facs: Fac) -> List[Mapper]:
    # todo specifically, walker is not required to have a name assigned to it.
    walker = Walker(None)

    return walker.mappers(*facs)


def get_factories_il(name: str, cls: Any) -> Fac:
    walker = Walker(name)
    return walker.resolve(cls)


def get_factories(name: str, *clss: Type) -> List[Fac]:
    walker = Walker(name)

    factories = [walker.resolve(cls) for cls in clss]

    node_objs, visit_node, root_nodes = walker._visit_objs(*factories)

    return [node_objs[x][1] for x in root_nodes]
