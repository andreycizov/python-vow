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
class Ref(Immaterial):
    """can only reference Named types"""
    name: str


@dataclass
class Var(Immaterial):
    name: str


@dataclass
class Int(Material):
    pass


@dataclass
class String(Material):
    pass


@dataclass
class Tuple(Material):
    items: typing.List[Type] = field(default_factory=list)


@dataclass
class List(Material):
    value: Type


@dataclass
class Dict(Material):
    key: Type
    value: Type


@dataclass
class Struct(Material):
    fields: typing.Dict[str, Type] = field(default_factory=dict)


@dataclass
class Class(Material):
    """Any custom class simply copies the structure of one of the types"""
    name: str

