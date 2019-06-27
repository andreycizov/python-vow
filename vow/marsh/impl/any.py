from importlib import import_module
from typing import Any, Type, Optional

from dataclasses import dataclass

from vow.marsh.error import SerializationError
from xrpc.trace import trc

from vow.marsh.helper import is_serializable
from vow.marsh.base import Mapper, Fac, FieldsFac, Fields


class PassthroughMapper(Mapper):

    def __init__(self, type: Optional[Type], dependencies: Fields):
        self.type = type
        super().__init__(dependencies)

    def serialize(self, obj: Any) -> Any:
        if self.type is not None:
            obj = self.type(obj)

        trc().debug('%s %s', self.type, obj)

        return obj

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.type})'

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.type == other.type


@dataclass()
class Passthrough(Fac):
    __mapper_cls__ = PassthroughMapper
    __mapper_args__ = 'type',

    type: Optional[Type] = None


@dataclass
class Ref(Fac):
    item: str

    def resolve(self, name) -> Fac:
        module, item = self.item.rsplit('.', 1)
        r = getattr(import_module(module), item)

        assert is_serializable(r), r
        return r.__serde__[name]

    def dependencies(self) -> FieldsFac:
        raise NotImplementedError('Ref.dependencies can not be called directly, must be handled outside')
