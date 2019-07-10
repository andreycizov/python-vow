import json

from bottle import Bottle, request, response
from dataclasses import replace, dataclass

from vow.api.bottle import bottle_envelope
from vow.middle.obj import Method, Endpoint, Endpoints, Methods
from vow.oas.obj.op import Operation, _split_path_parameters, Placement, Style, \
    Parameter, ParameterSerializerStyle, SchemaMedia
from vow.oas.obj.root import OAS
from vow.oas.obj.schema import Schema, ArraySchema, ObjectSchema, RefSchema
from xrpc.trace import trc


def walk_schema(item: Schema, fn):
    if isinstance(item, ArraySchema):
        if item.items:
            item.items = _map(item.items, fn, False)
    elif isinstance(item, ObjectSchema):
        for k, v in item.properties.items():
            v.schema = _map(v.schema, fn, False)

        if item.additional:
            item.additional = _map(item.additional, fn, False)


def walk_parameter(item: Parameter, fn):
    if isinstance(item.serializer, ParameterSerializerStyle):
        item.serializer.schema = _map(item.serializer.schema, fn, True)
    elif isinstance(item.serializer, SchemaMedia):
        item.serializer.schema = _map(item.serializer.schema, fn, True)
    else:
        raise NotImplementedError(item.serializer)


def _map(schema: Schema, fn, is_outer):
    walk_schema(schema, fn)

    cont, schema = fn(schema, is_outer)

    return schema


def walk_oas(item: OAS, fn):
    for path in item.paths:
        for method, op in path.operations.items():
            for parameter in op.parameters:
                walk_parameter(parameter, fn)
                # parameter.schema = _map(parameter.schema, fn, True)

            if op.request_body:
                for x in op.request_body.content:
                    x.schema = _map(x.schema, fn, True)

            for method, y in op.responses.items():
                for x in y.content:
                    x.schema = _map(x.schema, fn, True)


def simplify_schemas(oas: OAS):
    oas = replace(oas)
    schemas = {}
    schemas_actual = {}

    def ep(x, is_outer):
        if isinstance(x, ObjectSchema):
            if x.name:
                if x.name not in schemas:
                    schemas[x.name] = RefSchema(f'/components/schemas/{x.name}')
                    schemas_actual[x.name] = x
                return True, schemas[x.name]

        return True, x

    walk_oas(oas, ep)
    oas.components.schemas = schemas_actual
    return oas


class Empty:
    def __repr__(self):
        return 'EMPTY'


EMPTY = Empty()


@dataclass
class BottleEndpoint:
    operation: Operation
    endpoint: Endpoint
    method: Method

    def parse_response(self):
        envelope = bottle_envelope(self.operation, request)
        trc('0').debug('%s', envelope)
        return_ = self.method.call(self.endpoint, envelope)
        trc('1').debug('%s', return_)

        return return_

    def __call__(self, **kwargs):
        return_ = self.parse_response()

        response.status = 200
        response.content_type = 'application/json'

        return json.dumps(return_)


def _convert_path_parameters(path):
    first = True
    b = None
    old_b = None
    for a, b in _split_path_parameters(path):
        a -= 1
        b += 1

        if first:
            if a > 0:
                yield False, path[:a]
            first = False
        elif old_b and old_b < a:
            yield False, path[old_b:a]

        yield True, path[a + 1:b - 1]
        old_b = b

    if b is not None and b < len(path):
        yield False, path[b - 1:]

    if first:
        yield False, path[:]


def convert_path_parameters(path):
    r = ''
    for flag, item in _convert_path_parameters(path):
        if flag:
            r += f'<{item}>'
        else:
            r += item
    return r


def bottle_connect(oas: OAS, endpoints: Endpoints, methods: Methods, app: Bottle):
    for val in oas.paths:
        for method, operation in val.operations.items():
            path = convert_path_parameters(val.path)
            trc().debug('%s', path)
            app.route(path, method=method, callback=BottleEndpoint(
                operation,
                endpoints[operation.operation_id],
                methods[operation.operation_id]
            ))
