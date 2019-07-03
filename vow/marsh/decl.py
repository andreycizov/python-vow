import inspect
from typing import List, Type, Any, Callable

from dataclasses import dataclass, is_dataclass, field

from vow.marsh.base import Mapper, Fac
from vow.marsh.walker import Walker, Serializers, Deferred


@dataclass
class infer:
    ctxs: List[Walker] = field(default_factory=list)

    def __init__(self, *names: str):
        frame = inspect.currentframe().f_back
        self.ctxs = [Walker(name, frame=frame) for name in names]

    def __call__(self, obj):
        assert is_dataclass(obj), obj

        for ctx in self.ctxs:
            serde = Serializers.assign(obj)
            serde[ctx.name] = Deferred(ctx, obj)

        return obj


def get_serializers(name: str, *clss: Type) -> List[Mapper]:
    walker = Walker(name)

    factories = [walker.resolve(cls) for cls in clss]

    return walker.mappers(*factories)


def get_factories(name: str, *clss: Type) -> List[Fac]:
    walker = Walker(name)

    factories = [walker.resolve(cls) for cls in clss]

    node_objs, visit_node, root_nodes = walker._visit_objs(*factories)

    return [node_objs[x][1] for x in root_nodes]
