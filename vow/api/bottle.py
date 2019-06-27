import json
from base64 import b64decode
from typing import List, Optional, Tuple

from bottle import BaseRequest
from xrpc.trace import trc

from vow.oas.data import JsonAny
from vow.oas.envelope import RequestEnvelope, RequestError, ParameterMissing, \
    ContentType, \
    NoBody, RequestEnvelopeParameter
from vow.oas.obj.op import Operation, Placement, SchemaMedia, ParameterSerializerStyle
from vow.oas.obj.schema import StringSchema, ObjectSchema
from vow.oas.serializer import SERIALIZER


class Empty:
    def __repr__(self):
        return 'EMPTY'


EMPTY = Empty()


def bottle_envelope(operation: Operation, request: BaseRequest) -> RequestEnvelope:
    parameters: List[RequestEnvelopeParameter] = []
    body = None

    errors: List[RequestError] = []

    for item in operation.parameters:
        val = EMPTY

        try:
            if item.placement == Placement.Path:
                val = request.url_args[item.name]
            elif item.placement == Placement.Cookie:
                val = request.cookies.get(item.name, EMPTY)
            elif item.placement == Placement.Header:
                val = request.headers.get(item.name, EMPTY)
            elif item.placement == Placement.Query:
                val = request.query.get(item.name, EMPTY)
            else:
                raise NotImplementedError(f'{item.placement}')
        except KeyError:
            pass

        if val == EMPTY:
            if item.required:
                errors.append(ParameterMissing(item))

            continue

        if isinstance(item.serializer, ParameterSerializerStyle):
            ser_key = item.placement, item.serializer.style, item.serializer.explode
            _, fun_des = SERIALIZER[ser_key].get(item.serializer.schema.__class__, SERIALIZER[ser_key].get(None))

            val = fun_des(item.serializer.schema, val)

            parameters.append(RequestEnvelopeParameter(item.name, item.placement, val))
        elif isinstance(item.serializer, SchemaMedia):
            trc().warning('item ignored')
        else:
            raise NotImplementedError(f'{item.serializer}')

    try:
        if operation.request_body:
            content_type = request.headers['content-type'].split(';')

            body_bytes = request.body.read()

            for match_content_type in operation.request_body.content:
                if match_content_type.media.content_type in content_type:
                    schema_media = match_content_type
                    break
            else:
                errors.append(ContentType())
                raise ValueError()

            if isinstance(match_content_type.schema, StringSchema):
                if match_content_type.schema.format == 'binary':
                    body = body_bytes
                elif match_content_type.schema.format == 'base64':
                    body_bytes = b64decode(body_bytes)
                    body = body_bytes

            if body is None:
                if schema_media.media.content_type == 'application/json':
                    body = json.loads(body_bytes)
                elif schema_media.media.content_type == 'application/xml':
                    raise NotImplementedError('Need XML unmarshaller')
                elif schema_media.media.content_type == 'multipart/form-data':
                    assert isinstance(schema_media.schema, ObjectSchema), schema_media.schema

                    print(body)
                elif schema_media.media.content_type == 'application/x-www-form-urlencoded':
                    print(body)
                else:
                    raise NotImplementedError(f'{schema_media.media.content_type}')

        elif request.body:
            if request.body.read(1) != b'':
                errors.append(NoBody())
    except ValueError:
        pass

    return RequestEnvelope(
        request.url,
        request.method.lower(),
        errors,
        parameters,
        body
    )
