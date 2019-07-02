from typing import TypeVar, Any, Dict

T = TypeVar('T')

FieldsFac = Dict[str, 'Fac']
Fields = Dict[str, 'Mapper']


class Mapper:
    def __init__(self, dependencies: Fields):
        self.dependencies: Fields = dependencies

    def serialize(self, obj: Any) -> Any:
        raise NotImplementedError('')

    def __call__(self, obj: Any) -> Any:
        return self.serialize(obj)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}()'

    def __eq__(self, other):
        return self.__class__ == other.__class__


class Fac:
    __mapper_cls__ = Mapper
    __mapper_args__ = tuple()

    def dependencies(self) -> FieldsFac:
        return {}

    def create(self, dependencies: Fields) -> Mapper:
        try:
            return self.__mapper_cls__(
                dependencies=dependencies,
                **{k: getattr(self, k) for k in self.__mapper_args__}
            )
        except Exception as e:
            raise ValueError(f'{self.__mapper_cls__}, {self.__mapper_args__}, {e}')

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}()'

    def __eq__(self, other):
        return self.__class__ == other.__class__
