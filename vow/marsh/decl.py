import inspect
from typing import List

from dataclasses import dataclass, is_dataclass, field

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