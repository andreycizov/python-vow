from collections import OrderedDict
from enum import Enum
from typing import Type, List, Tuple, Any, Optional

from dataclasses import dataclass, field

from vow.marsh.error import SerializationError, subserializer
from vow.marsh.base import Fields, FieldsFac, Mapper, Fac
from vow.marsh.impl.any import Passthrough, FieldValue


class AnyIntoStructMapper(Mapper):

    def __init__(self, cls: Type, dependencies: Fields):
        self.cls = cls
        super().__init__(dependencies)

    def serialize(self, obj: Any) -> Any:
        if self.cls and not isinstance(obj, self.cls):
            raise SerializationError(val=obj, reason='not_instance', origin=self)

        r = OrderedDict()

        for idx, v in self.dependencies.items():
            with subserializer(idx):
                item = v.serialize(obj)

                if not isinstance(item, FieldValue):
                    raise SerializationError(val=item, reason='unsupported_field_defn', origin=self)

            if item.has_value:
                r[item.name] = item.value
        return r


@dataclass
class AnyIntoStruct(Fac):
    __mapper_cls__ = AnyIntoStructMapper

    fields: List[Fac]
    cls: Optional[Type] = None

    def create(self, dependencies: Fields) -> Mapper:
        return self.__mapper_cls__(
            cls=self.cls,
            dependencies=dependencies,
        )

    def dependencies(self) -> FieldsFac:
        return {str(i): v for i, v in enumerate(self.fields)}


class AnyIntoEnumMapper(Mapper):

    def __init__(self, enum: Type[Enum], dependencies: Fields):
        self.enum = enum
        super().__init__(dependencies)

    def serialize(self, obj: Any) -> Any:
        try:
            obj = self.dependencies['value'].serialize(obj)
        except SerializationError as e:
            raise e.with_path('value')

        if not isinstance(obj, self.enum):
            raise SerializationError(val=obj, reason='enum_not_enum', origin=self)

        try:
            obj = obj.value
        except AttributeError as e:
            raise SerializationError(val=obj, exc=e, reason='invalid_enum', origin=self)

        return obj


@dataclass
class AnyIntoEnum(Fac):
    __mapper_cls__ = AnyIntoEnumMapper
    __mapper_args__ = 'enum',

    enum: Type[Enum]
    value: Fac = field(default_factory=lambda: Passthrough(str))

    def create(self, dependencies: Fields) -> Mapper:
        return self.__mapper_cls__(enum=self.enum, dependencies=dependencies)

    def dependencies(self) -> FieldsFac:
        return {'value': self.value}
