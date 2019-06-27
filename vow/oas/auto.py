import inspect
from datetime import datetime, date, timedelta
from functools import reduce
from itertools import groupby
from typing import List, Dict, TypeVar, Union, Tuple

from dataclasses import fields, Field, is_dataclass, MISSING, replace
from typing_inspect import get_last_args, is_optional_type, get_args

from vow.middle.obj import Method, Endpoint, Argument, Endpoints, Methods, Arguments
from vow.oas.decl import get_declarative, parameter, body
from vow.oas.helper import load_fun_parameters, EMPTY, asdict_shallow
from vow.oas.obj import schema
from vow.oas.obj.op import Path, Operation, Parameter, OperationRequestBody, \
    SchemaMedia, ResponseCode, Response, split_path_parameters, Placement
from vow.oas.obj.schema import ObjectSchema, Defaulted
from vow.marsh.impl.json import JSON_INTO
from vow.marsh.walker import Walker
from xrpc.trace import trc


def auto_any(cls) -> schema.Schema:
    if inspect.isclass(cls):
        if issubclass(cls, bool):
            return schema.BooleanSchema()
        elif issubclass(cls, float):
            return schema.NumberSchema()
        elif issubclass(cls, int):
            return schema.IntSchema()
        elif issubclass(cls, str):
            return schema.StringSchema()
        elif issubclass(cls, datetime):
            return schema.StringSchema(format='date-time')
        elif issubclass(cls, date):
            return schema.StringSchema(format='date')
        elif issubclass(cls, timedelta):
            return schema.NumberSchema(format='double')
        elif issubclass(cls, List):
            vt, = get_args(cls)

            subschema = auto_any(vt)

            return schema.ArraySchema(
                items=subschema
            )
        elif issubclass(cls, Dict):
            kt, vt = get_args(cls)

            if not issubclass(kt, str):
                raise NotImplementedError(('class3', cls, kt))

            subschema = auto_any(vt)

            return ObjectSchema(
                additional=subschema,
            )
        elif is_dataclass(cls):
            return auto_dataclass(cls)
        else:
            raise NotImplementedError(('class2', cls))
    else:
        if is_optional_type(cls):
            item, _ = get_last_args(cls)

            trc('1').debug('%s %s', item, _)
            r = auto_any(item)

            if isinstance(r, schema.Nullable):
                r.nullable = True
            else:
                raise NotImplementedError('can not be nullable')

            trc('2').debug('%s', r)
            return r
        else:
            raise NotImplementedError(('class', cls))


def auto_dataclass(cls):
    r = schema.ObjectSchema(
        name=cls.__name__,
        extensions={
            'py-dataclass': cls.__module__ + '.' + cls.__name__
        }
    )

    flds: List[Field] = fields(cls)

    for field in flds:
        item = auto_any(field.type)

        if isinstance(item, schema.Defaulted):

            default = field.default
            default_set = None

            if default == MISSING:
                if field.default_factory != MISSING:
                    default = field.default_factory()

            if default != MISSING:
                if default is None:
                    if isinstance(item, schema.Nullable):
                        item.nullable = True
                    else:
                        raise NotImplementedError(f'{field.name} {item} [1]')
                else:
                    ctx = Walker(JSON_INTO)
                    fac = ctx.resolve(field.type)
                    mapper, = ctx.mappers(fac)

                    default_set = mapper.serialize(default)

            item.default = default_set

        r.properties[field.name] = schema.ObjectSchemaProperty(
            item
        )
    return r


T = TypeVar('T', bound=Union[parameter, body])


def auto_callable_param_schema(params, par: T) -> T:
    schema = par.schema
    if schema is None:
        default, annot = params[par.name]
        if annot is None:
            raise ValueError(f'Schemaless definitions need a type annotation `{par.name}`')
        schema = auto_any(annot)

        if default != EMPTY:
            ctx = Walker(JSON_INTO)
            fac = ctx.resolve(par)
            mapper, = ctx.mappers(fac)

            schema.default = mapper.serialize(default)
    return replace(par, schema=schema)


def auto_callable(fun, is_bound=False) -> Tuple[Path, Endpoint, Method]:
    r = get_declarative(fun)

    if r:
        params, resp = load_fun_parameters(fun, is_bound=is_bound)

        assert resp != EMPTY, f'Return value is empty for `{fun}`'

        parameters = []
        endpoint_parameters = []
        endpoint_body = None
        endpoint_body_name = None
        endpoint_response = resp

        parameters_path = split_path_parameters(r.path)
        parameters_missed = list(params.keys())

        for p in r.parameters:
            try:
                b = auto_callable_param_schema(params, p)
            except KeyError:
                raise ValueError(f'`{p.name}` parameters is mussing from {fun}')
            parameters.append(b.to_parameter())
            parameters_missed.remove(p.name)

            default, annot = params[p.name]

            endpoint_parameters.append(Argument(
                p.name,
                default=default if default != EMPTY else MISSING,
                type=annot
            ))

        request_body = None
        if r.body:
            b = r.body
            b = auto_callable_param_schema(params, b)

            request_body = OperationRequestBody(
                name=b.name,
                content=[SchemaMedia(x, b.schema) for x in b.media],
                required=b.required,
            )

            parameters_missed.remove(r.body.name)

            default, annot = params[b.name]

            endpoint_body_name = b.name
            endpoint_body = annot

        if parameters_missed:
            for p in parameters_missed:
                placement = Placement.Path if p in parameters_path else Placement.Query
                try:
                    b = auto_callable_param_schema(params, parameter(p, placement=placement))
                except KeyError:
                    raise ValueError(f'`{p.name}` parameters is missing from {fun}')
                parameters.append(
                    b.to_parameter()
                )

                default, annot = params[b.name]

                endpoint_parameters.append(Argument(
                    b.name,
                    default=default if default != EMPTY else MISSING,
                    type=annot
                ))

        responses = {}  # type: Dict[ResponseCode, Response]
        if r.response:
            schema2 = r.response.schema

            if schema2 is None:
                schema2 = auto_any(resp)

            responses['200'] = Response(
                [SchemaMedia(
                    x, schema2
                ) for x in r.response.media],
                description='OK'
            )

        operation = Operation(
            operation_id=r.operation_id,
            parameters=parameters,
            request_body=request_body,
            tags=r.tags,
            responses=responses,
            method_link=fun,
        )
        path = Path(
            r.path,
            operations={
                r.method: operation
            }
        )
        endpoint = Endpoint(
            operation_id=r.operation_id,
            url=r.path,
            method=r.method,
            arguments=Arguments(endpoint_parameters),
            body=endpoint_body,
            body_name=endpoint_body_name,
            response=endpoint_response

        )
        method = Method(
            operation_id=r.operation_id,
            callable=fun,
        )
        return path, endpoint, method
    else:
        raise NotImplementedError(f'{fun}')


def auto_actor(obj) -> Tuple[List[Path], Endpoints, Methods]:
    r = []  # type: List[Path]
    r2 = []  # type: List[Endpoint]
    r3 = []  # type: List[Method]
    for x in dir(obj):
        y = getattr(obj, x)

        if get_declarative(y):
            path, endpoint, method = auto_callable(y, is_bound=True)
            r.append(path)
            r2.append(endpoint)
            r3.append(method)

    r = sorted(r, key=lambda x: x.path)

    r = [reduce(lambda a, b: a.merge(b), rs) for _, rs in groupby(r, key=lambda x: x.path)]

    return r, Endpoints(r2), Methods(r3)
