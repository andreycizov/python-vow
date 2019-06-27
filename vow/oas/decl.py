from typing import Optional, List, Any

from dataclasses import field, dataclass

from vow.oas.helper import asdict_shallow
from vow.oas.obj.media import Media, XmlMedia, JsonMedia, FormMedia
from vow.oas.obj.op import PathMethod, PathURL, Placement, Parameter, ParameterSerializerStyle
from vow.oas.obj.schema import Schema

ATTR = '__oas__'


def get_declarative(obj) -> Optional['Declarative']:
    return getattr(obj, ATTR, None)


@dataclass
class Declarative:
    path: Optional[PathURL] = None
    method: Optional[PathMethod] = 'get'
    operation_id: Optional[str] = None

    response: Optional['response'] = field(default_factory=lambda: response())
    body: Optional['body'] = None
    parameters: List['parameter'] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)


class DeclarativeDecorator:
    def map(self, it, x: Declarative) -> Declarative:
        raise NotImplementedError()

    def __call__(self, it):
        new = getattr(it, ATTR, Declarative())

        setattr(it, ATTR, self.map(it, new))

        return it


@dataclass
class path(DeclarativeDecorator):
    path: str
    method: str
    operation_id: str
    tags: List[str] = field(default_factory=list)

    def map(self, it, x: Declarative) -> Declarative:
        for k, v in asdict_shallow(self).items():
            setattr(x, k, v)

        return x


@dataclass
class Parametrises:
    name: str
    description: Optional[str] = None
    schema: Optional[Schema] = None
    required: bool = True
    enum: Optional[List[Any]] = None


@dataclass
class parameter(DeclarativeDecorator, Parametrises):
    placement: Placement = Placement.Query

    def map(self, it, x: Declarative) -> Declarative:
        # ss = self._map_schema(it, x)
        x.parameters.append(self)
        return x

    def to_parameter(self) -> Parameter:
        return Parameter(
            placement=self.placement,
            name=self.name,
            description=self.description,
            required=self.required,
            serializer=ParameterSerializerStyle.default_for(self.placement, self.schema),
            # serialize the value given as the parameter
            enum=self.enum,
        )


@dataclass
class body(DeclarativeDecorator, Parametrises):
    media: List[Media] = field(default=(JsonMedia(), XmlMedia(), FormMedia()))

    def map(self, it, x: Declarative) -> Declarative:
        x.body = self
        return x


@dataclass
class response(DeclarativeDecorator):
    schema: Optional[Schema] = None
    media: List[Media] = field(default=(JsonMedia(), XmlMedia()))

    def map(self, it, x: Declarative) -> Declarative:
        x.response = self
        return x
