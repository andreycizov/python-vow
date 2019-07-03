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
    """
    we can reference any types
    referencing happens by providing the path in the relevant serialization format

    e.g. :
      - if these objects are used inside a Python module - then we need to import `name`
         and the import of that name must be of `Type`
      - if these objects are provided as part of an API return - then we'll need to find them
         in the JSON object returned by the server
    """
    name: str


@dataclass
class Var(Immaterial):
    name: str


@dataclass
class Atom(Material):
    name: str


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
class Struct(Material):
    fields: typing.Dict[str, Type]


@dataclass
class Dataclass(Struct):
    name: str


@dataclass
class Class(Material):
    """Any custom class simply copies the structure of one of the types"""
    name: str
