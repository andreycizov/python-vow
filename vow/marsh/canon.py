import typing

from dataclasses import dataclass, field


# we can always extend this type

class Type:
    def walk(self) -> typing.Iterable['Type']:
        return iter([])


class Material(Type):
    pass


class Immaterial(Type):
    pass


@dataclass
class Named:
    """(possibly) self-referential type"""
    name: str


@dataclass
class Ref(Immaterial):
    """can only reference Named types"""
    name: str


@dataclass
class Var(Immaterial):
    name: str


@dataclass
class Date(Material):
    pass


@dataclass
class TimeDelta(Material):
    pass


@dataclass
class DateTime(Material):
    pass


@dataclass
class Int(Material):
    pass


@dataclass
class String(Material):
    pass


@dataclass
class Tuple(Material):
    items: typing.List[Type]


@dataclass
class List(Material):
    value: Type


@dataclass
class Dict(Material):
    key: Type
    value: Type


@dataclass
class Struct(Named, Material):
    fields: typing.Dict[str, Type]


@dataclass
class Custom(Named, Material):
    """Any custom class simply copies the structure of one of the types"""
    subtype: Type


@dataclass
class Tuple(Material):
    values: typing.List[Type] = field(default_factory=list)

    def __init__(self, *args: Type):
        self.values = list(args)
