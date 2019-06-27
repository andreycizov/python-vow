from collections import OrderedDict
from enum import Enum
from typing import Optional, List, Dict, Any, Union, Tuple

from dataclasses import dataclass, field, replace

from vow.oas.data import JsonAny
from vow.oas.gen import Generated, update_deprecated, update_summarydesc
from vow.oas.obj.media import Media
from vow.oas.obj.schema import Schema


@dataclass
class External:
    url: str
    description: Optional[str] = None


@dataclass
class ExternalVar:
    default: str
    enum: Optional[List[str]] = None
    description: Optional[str] = None


class Placement(Enum):
    Path = 'path'
    Query = 'query'
    Header = 'header'
    Cookie = 'cookie'


class Style(Enum):
    Simple = 'simple'
    Label = 'label'
    Matrix = 'matrix'

    Form = 'form'
    SpaceDelimited = 'spaceDelimited'
    PipeDelimited = 'pipeDelimited'
    DeepObject = 'deepObject'


PLACEMENT_STYLE_DEFAULT: Dict[Placement, Tuple[Style, bool]] = {
    Placement.Path: (Style.Simple, False),
    Placement.Query: (Style.Form, True),
    Placement.Header: (Style.Simple, False),
    Placement.Cookie: (Style.Form, True),
}

PLACEMENT_STYLE_ALLOWED: Dict[Placement, List[Style]] = {
    Placement.Path: [Style.Simple, Style.Label, Style.Matrix],
    Placement.Query: [Style.Form, Style.SpaceDelimited, Style.PipeDelimited, Style.DeepObject],
    Placement.Header: [Style.Simple],
    Placement.Cookie: [Style.Form],
}


@dataclass
class SchemaMedia:
    media: Media
    schema: Schema
    examples: Dict[str, JsonAny] = field(default_factory=dict)


@dataclass
class ParameterSerializerStyle:
    schema: Schema
    style: Style
    explode: bool = False

    @classmethod
    def default_for(cls, placement: Placement, schema: Schema):
        return ParameterSerializerStyle(
            schema,
            *PLACEMENT_STYLE_DEFAULT[placement]
        )


ParameterSerializer = Union[SchemaMedia, ParameterSerializerStyle]


@dataclass
class Parameter(Generated):
    placement: Placement
    name: str
    serializer: ParameterSerializer
    required: bool = True
    description: Optional[str] = None
    enum: Optional[List[JsonAny]] = None

    def serialize(self) -> JsonAny:
        r = OrderedDict()

        r['in'] = self.placement.value
        r['name'] = self.name

        if self.placement == Placement.Path:
            r['required'] = True

        if self.required:
            r['required'] = True

        if isinstance(self.serializer, ParameterSerializerStyle):
            r['schema'] = self.serializer.schema.serialize()

            style, explode = PLACEMENT_STYLE_DEFAULT[self.placement]

            if self.serializer.style != style:
                r['style'] = self.serializer.style.value

            if self.serializer.explode != explode:
                r['explode'] = self.serializer.explode
        elif isinstance(self.serializer, SchemaMedia):
            rr = {
                'schema': self.serializer.schema.serialize(),
            }

            if self.serializer.examples:
                rr['examples'] = self.serializer.examples

            r['content'] = {
                self.serializer.media.content_type: rr
            }
        else:
            raise NotImplementedError('')

        if self.enum:
            r['schema']['enum'] = self.enum

        return r


ResponseCode = str


def response_media_list_serialize(obj: List[SchemaMedia]):
    rr = {}

    for v in obj:
        rrr = {
            'schema': v.schema.serialize(),
        }

        if v.examples:
            rrr['examples'] = v.examples

        rr[v.media.content_type] = rrr

    return rr


@dataclass
class OperationRequestBody:
    name: str
    content: List[SchemaMedia] = field(default_factory=list)
    description: Optional[str] = None
    required: bool = False


@dataclass
class LinkParameter:
    name: str
    value: JsonAny


@dataclass
class Link(Generated):
    name: str
    operation_id: str
    parameters: List[LinkParameter] = field(default_factory=list)
    description: Optional[str] = None

    def serialize(self) -> JsonAny:
        r = OrderedDict()

        r['operationId'] = self.operation_id

        if self.parameters:
            r['parameters'] = {}

            for x in self.parameters:
                r['parameters'][x.name] = x.value

        update_summarydesc(self, r)

        return r


@dataclass
class Response(Generated):
    content: List[SchemaMedia]
    description: Optional[str] = None
    links: List[Link] = field(default_factory=list)

    def serialize(self) -> JsonAny:
        r = OrderedDict()

        update_summarydesc(self, r)

        r['content'] = response_media_list_serialize(self.content)

        if self.links:
            r['links'] = {}

            for x in self.links:
                r['links'][x.name] = x.serialize()

        return r


@dataclass
class Operation(Generated):
    operation_id: Optional[str] = None
    parameters: List[Parameter] = field(default_factory=list)
    request_body: Optional[OperationRequestBody] = None
    responses: Dict[ResponseCode, Response] = field(default_factory=dict)
    summary: Optional[str] = None
    description: Optional[str] = None
    external_docs: Optional[External] = None
    deprecated: bool = False
    servers: List[External] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    method_link: Optional[Any] = None

    def serialize(self) -> JsonAny:
        r = OrderedDict()

        if self.tags:
            r['tags'] = self.tags

        update_summarydesc(self, r)
        update_deprecated(self, r)

        r['operationId'] = self.operation_id

        if self.parameters:
            rr = []

            for x in self.parameters:
                rr.append(x.serialize())

            r['parameters'] = rr

        rr = {}

        for k, v in self.responses.items():
            rr[k] = v.serialize()

        r['responses'] = rr

        if self.request_body:
            rr = {}

            if self.request_body.required is True:
                rr['required'] = self.request_body.required

            rr['content'] = response_media_list_serialize(self.request_body.content)
            update_summarydesc(self.request_body, rr)

            r['requestBody'] = rr

        return r


def _split_path_parameters(x: str):
    start = 0

    while True:
        idx_a = x.find('{', start)

        if idx_a == -1:
            break

        idx_b = x.find('}', idx_a)

        if idx_b == -1:
            break

        yield idx_a + 1, idx_b

        start = idx_b


def split_path_parameters(path: str):
    return [path[a:b] for a, b in _split_path_parameters(path)]


PathURL = str
PathMethod = str


@dataclass
class Path(Generated):
    path: PathURL
    operations: Dict[PathMethod, Operation]

    summary: Optional[str] = None
    description: Optional[str] = None

    servers: Optional['Servers'] = None

    def merge(self, path: 'Path') -> 'Path':
        new_path = replace(self)
        new_path.operations = dict(new_path.operations)

        keys_a = new_path.operations.keys()
        keys_b = path.operations.keys()

        assert keys_a & keys_b == set(), keys_a & keys_b

        new_path.operations.update(path.operations)

        return new_path

    def serialize(self) -> JsonAny:
        r = OrderedDict()

        update_summarydesc(self, r)

        for method, op in self.operations.items():
            r[method.lower()] = op.serialize()

        return r


@dataclass
class Server(External, Generated):
    variables: Dict[str, ExternalVar] = field(default_factory=dict)

    def serialize(self) -> JsonAny:
        r = OrderedDict()
        r['url'] = self.url
        if self.description is not None:
            r['description'] = self.description

        if self.variables:
            r['variables'] = {}

            for k, v in self.variables.items():
                rr = {'default': v.default}

                if v.description is not None:
                    rr['description'] = v.description

                if v.enum is not None:
                    r['enum'] = v.enum

                r['variables'][k] = rr

        return r


@dataclass
class Servers:
    items: List[Server]
