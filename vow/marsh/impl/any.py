from importlib import import_module
from typing import Any, Type, Optional, Dict

from dataclasses import dataclass

from vow.marsh.error import SerializationError
from xrpc.trace import trc

from vow.marsh.helper import is_serializable, DECL_ATTR
from vow.marsh.base import Mapper, Fac, FieldsFac, Fields


class PassthroughMapper(Mapper):

    def __init__(self, type: Optional[Type], dependencies: Fields):
        self.type = type
        super().__init__(dependencies)

    def serialize(self, obj: Any) -> Any:
        if self.type is not None:
            try:
                obj = self.type(obj)
            except Exception as e:
                raise SerializationError(val=obj, reason=str(e))

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


class AnyAnyAttrMapper(Mapper):

    def __init__(self, name: str, dependencies: Fields):
        self.name = name
        super().__init__(dependencies)

    def serialize(self, obj: Any) -> Any:
        try:
            r = getattr(obj, self.name)
        except AttributeError:
            raise SerializationError(val=obj, reason=f'`{self.name}`')

        try:
            return self.dependencies['type'].serialize(r)
        except SerializationError as e:
            raise e.with_path('$type')


@dataclass()
class AnyAnyAttr(Fac):
    __mapper_cls__ = AnyAnyAttrMapper
    __mapper_args__ = 'name',

    name: str
    type: Fac

    def dependencies(self) -> FieldsFac:
        return {'type': self.type}


class AnyAnyItemMapper(Mapper):

    def __init__(self, name: str, dependencies: Fields):
        self.name = name
        super().__init__(dependencies)

    def serialize(self, obj: Any) -> Any:
        try:
            r = obj[self.name]
        except KeyError:
            raise SerializationError(val=obj, reason=f'`{self.name}`')

        try:
            return self.dependencies['type'].serialize(r)
        except SerializationError as e:
            raise e.with_path('$type')


@dataclass()
class AnyAnyItem(Fac):
    __mapper_cls__ = AnyAnyItemMapper
    __mapper_args__ = 'name',

    name: str
    type: Fac

    def dependencies(self) -> FieldsFac:
        return {'type': self.type}


@dataclass
class Ref(Fac):
    item: str

    def resolve(self, name) -> Fac:
        module, item = self.item.rsplit('.', 1)
        r = getattr(import_module(module), item)

        assert is_serializable(r), r
        return getattr(r, DECL_ATTR)[name]

    def dependencies(self) -> FieldsFac:
        raise NotImplementedError('Ref.dependencies can not be called directly, must be handled outside')


@dataclass
class AnyAnyDisciminant(Fac):
    value: Fac
    mappers: Dict[Any, Fac]


