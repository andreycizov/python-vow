from importlib import import_module
from typing import Any, Type, Optional, Dict, Tuple, List

from dataclasses import dataclass
from xrpc.trace import trc

from vow.marsh.error import SerializationError
from vow.marsh.helper import is_serializable, DECL_ATTR
from vow.marsh.base import Mapper, Fac, FieldsFac, Fields


class AnyAnySelfMapper(Mapper):

    def serialize(self, obj: Any) -> Any:
        return self.dependencies['self'].serialize(obj)


class PassthroughMapper(Mapper):

    def __init__(self, type: Optional[Type], dependencies: Fields):
        self.type = type
        super().__init__(dependencies)

    def serialize(self, obj: Any) -> Any:
        if self.type is not None:
            try:
                obj = self.type(obj)
            except Exception as e:
                raise SerializationError(val=obj, exc=e, reason='unmappable', origin=self)

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
        except AttributeError as e:
            raise SerializationError(val=obj, reason=f'attr_missing', origin=self, exc=e)

        try:
            return self.dependencies['type'].serialize(r)
        except SerializationError as e:
            raise e.with_path('$attr')


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
        except KeyError as e:
            raise SerializationError(val=obj, reason='key_missing', exc=e, origin=self)

        trc('anyanyitemmap').debug('%s', r)

        try:
            return self.dependencies['type'].serialize(r)
        except SerializationError as e:
            raise e.with_path('$item')


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

    def __init__(self, item):
        if isinstance(item, str):
            self.item = item
        else:
            self.item = f'{item.__module__}.{item.__name__}'

    def resolve(self, name) -> Fac:
        module, item = self.item.rsplit('.', 1)
        r = getattr(import_module(module), item)

        assert is_serializable(r), (self.item, r)
        return getattr(r, DECL_ATTR)[name]

    def dependencies(self) -> FieldsFac:
        raise NotImplementedError('Ref.dependencies can not be called directly, must be handled outside')


class AnyAnyDiscriminantMapper(Mapper):

    def __init__(self, items: Dict[Any, str], dependencies: Fields):
        self.items = items
        super().__init__(dependencies)

    def serialize(self, obj: Any) -> Any:
        try:
            discriminant = self.dependencies['discriminant'].serialize(obj)
        except SerializationError as e:
            raise e.with_path('$discriminant')

        if discriminant not in self.items:
            raise SerializationError(val=obj, path=['$value'], origin=self,
                                     reason=f'`{discriminant}` is not in the map')

        try:
            value = self.dependencies['value'].serialize(obj)
        except SerializationError as e:
            raise e.with_path('$value')

        depk = self.items[discriminant]
        dep = self.dependencies[depk]

        try:
            return dep.serialize(value)
        except SerializationError as e:
            raise e.with_path('$sub', depk)


@dataclass
class AnyAnyDiscriminant(Fac):
    __mapper_cls__ = AnyAnyDiscriminantMapper
    discriminant: Fac
    value: Fac
    mappers: List[Tuple[Any, Fac]]

    def create(self, dependencies: Fields) -> Mapper:
        return self.__mapper_cls__(items={
            x: str(i) for i, (x, _) in enumerate(self.mappers)
        }, dependencies=dependencies)

    def dependencies(self) -> FieldsFac:
        r = {str(i): x for i, (_, x) in enumerate(self.mappers)}
        r['discriminant'] = self.discriminant
        r['value'] = self.value
        return r


@dataclass
class AnyAnyFieldMapper(Mapper):

    def __init__(self, name: str, dependencies: Fields):
        self.name = name
        super().__init__(dependencies)

    def serialize(self, obj: Any) -> Any:
        v = self.dependencies['item']

        try:
            obj = v.serialize(obj)
        except SerializationError as e:
            raise e.with_path('$field')

        return self.name, obj


@dataclass
class AnyAnyField(Fac):
    __mapper_cls__ = AnyAnyFieldMapper
    __mapper_args__ = 'name',

    name: str
    serializer: Fac

    def dependencies(self) -> FieldsFac:
        return {'item': self.serializer}


class AnyAnyLookupMapper(Mapper):

    def __init__(self, lookup: Dict[Any, Any], dependencies: Fields):
        self.lookup = lookup
        super().__init__(dependencies)

    def serialize(self, obj: Any) -> Any:
        try:
            obj2 = self.dependencies['value'].serialize(obj)
        except SerializationError as e:
            raise e.with_path('$value')

        if obj2 not in self.lookup:
            raise SerializationError(val=obj, exc=KeyError(obj2), reason='key_missing')

        return self.lookup[obj2]


@dataclass
class AnyAnyLookup(Fac):
    __mapper_cls__ = AnyAnyLookupMapper
    __mapper_args__ = 'lookup',

    value: Fac
    lookup: Dict[Any, Any]

    def dependencies(self) -> FieldsFac:
        return {'value': self.value}


class AnyAnyWithMapper(Mapper):

    def serialize(self, obj: Any) -> Any:
        try:
            obj = self.dependencies['value'].serialize(obj)
        except SerializationError as e:
            raise e.with_path('$value')

        try:
            r = self.dependencies['child'].serialize(obj)
        except SerializationError as e:
            raise e.with_path('$child')

        return r


@dataclass
class AnyAnyWith(Fac):
    __mapper_cls__ = AnyAnyWithMapper

    value: Fac
    child: Fac

    def dependencies(self) -> FieldsFac:
        return {'value': self.value, 'child': self.child}
