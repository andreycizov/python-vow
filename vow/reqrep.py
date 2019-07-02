import json
from enum import Enum
from typing import Optional, Any, Tuple, Union

from dataclasses import dataclass, field

from vow.marsh import infer, Fac, SerializationError
from vow.marsh.base import FieldsFac, Mapper
from vow.marsh.helper import FIELD_FACTORY
from vow.marsh.impl.any import Passthrough, AnyAnyDiscriminant, Ref, AnyAnyItem, AnyAnySelfMapper, AnyAnyField, \
    AnyAnyAttr, AnyAnyLookupMapper, AnyAnyWith, AnyAnyLookup
from vow.marsh.impl.binary_from import BinaryFromVarInt, BinaryFromBytes
from vow.marsh.impl.binary_into import BinaryIntoVarInt
from vow.marsh.impl.json import JSON_FROM, JSON_INTO, JsonAnyAny, JsonAnyOptional
from vow.marsh.impl.any_from import AnyFromStruct, AnyFromEnum
from vow.marsh.impl.any_into import AnyIntoStruct, AnyIntoEnum
from vow.marsh.walker import Walker
from vow.oas.data import JsonAny


class Type(Enum):
    # client sends the service request
    Service = 'service'
    Header = 'header'
    Begin = 'begin'

    Accepted = 'accepted'
    Denied = 'denied'

    # initiates the request
    Request = 'request'
    # error
    Error = 'error'
    # cancels the request
    Cancel = 'cancel'
    # a start means "an iteration begins"
    Start = 'start'
    # if request has many items to reply with, then each reply is an iteration
    Step = 'step'
    StepAck = 'stepa'
    # an end either means "end of iteration" or
    End = 'end'


# basically anything that contains JsonAnyAny as a field
# is an envelope.

class BinaryFromFrameMapper(Mapper):

    def serialize(self, obj: bytes) -> Tuple['Frame', bytes]:
        size, obj = self.dependencies['varint'].serialize(obj)
        body, obj = self.dependencies['bytes'].serialize((size, obj))

        try:
            body = json.loads(body)
        except json.JSONDecodeError as e:
            raise SerializationError(val=body, reason=f'json:{e}')

        return Frame(body), obj


class BinaryFromFrame(Fac):
    __mapper_cls__ = BinaryFromFrameMapper

    def dependencies(self) -> FieldsFac:
        return {'varint': BinaryFromVarInt(), 'bytes': BinaryFromBytes()}


class BinaryIntoFrameMapper(Mapper):

    def serialize(self, obj: 'Frame') -> bytes:
        try:
            body = json.dumps(obj.body).encode('utf-8')
        except Exception as e:
            raise SerializationError(val=obj, reason=f'json:{e}')

        size = self.dependencies['varint'].serialize(len(body))

        return size + body


class BinaryIntoFrame(Fac):
    __mapper_cls__ = BinaryIntoFrameMapper

    def dependencies(self) -> FieldsFac:
        return {'varint': BinaryIntoVarInt()}


@dataclass
class Frame:
    body: JsonAny = field(default=None)


class JsonIntoPacket(Fac):
    __mapper_cls__ = AnyAnySelfMapper

    def dependencies(self) -> FieldsFac:
        r = dict()

        r['self'] = AnyIntoStruct(
            [
                ('type', False, AnyAnyField(
                    'type',
                    AnyIntoEnum(
                        Type,
                        AnyAnyLookup(
                            AnyAnyWith(
                                AnyAnyAttr('body', Passthrough()),
                                AnyAnyAttr('__class__', Passthrough()),
                            ),
                            {v: k for k, v in PACKET_TYPE_MAP}
                        )
                    ),

                    # AnyAnyAttr('type', JsonIntoEnum(Type, Passthrough(str)))
                )),
                ('stream', False, AnyAnyField(
                    'stream',
                    AnyAnyAttr('stream', JsonAnyOptional(Passthrough(int)))
                )),
                ('body', False, AnyAnyField(
                    'body',
                    AnyAnyDiscriminant(
                        AnyAnyWith(
                            AnyAnyAttr('body', Passthrough()),
                            AnyAnyAttr('__class__', Passthrough()),
                        ),
                        AnyAnyAttr('body', Passthrough()),
                        [(v, Ref(v)) for _, v in PACKET_TYPE_MAP]
                    )
                )),
            ],
            Packet
        )

        return r


class JsonFromPacket(Fac):
    __mapper_cls__ = AnyAnySelfMapper

    def dependencies(self) -> FieldsFac:
        r = dict()

        r['self'] = AnyFromStruct(
            [
                # ('type', False, AnyAnyField(
                #     'type',
                #     AnyAnyItem('type', JsonFromEnum(Type, Passthrough(str)))
                # )),
                ('stream', False, AnyAnyField(
                    'stream',
                    AnyAnyItem('stream', JsonAnyOptional(Passthrough(int)))
                )),
                ('body', False, AnyAnyField(
                    'body',
                    AnyAnyDiscriminant(
                        AnyAnyItem('type', AnyFromEnum(Type, Passthrough(str))),
                        AnyAnyItem('body', JsonAnyAny()),
                        [(k, Ref(v)) for k, v in PACKET_TYPE_MAP]
                    )
                )),
            ],
            Packet
        )

        return r


@dataclass
class Packet:
    __serde__ = {
        JSON_INTO: JsonIntoPacket(),
        JSON_FROM: JsonFromPacket(),
    }
    stream: Optional[str]
    body: Union[
        'Service',
        'Header',
        'Begin',
        'Accepted',
        'Denied',
        'Request',
        'Error',
        'Cancel',
        'Start',
        'Step',
        'StepAck',
        'End',
    ]


@infer(JSON_INTO, JSON_FROM)
@dataclass
class Service:
    name: str
    version: str = '0.1.0'
    proto: str = '0.1.0'


@infer(JSON_INTO, JSON_FROM)
@dataclass
class Header:
    name: str
    value: JsonAny = field(default=None, metadata={FIELD_FACTORY: JsonAnyAny()})


@infer(JSON_INTO, JSON_FROM)
@dataclass
class Begin:
    pass


@infer(JSON_INTO, JSON_FROM)
@dataclass
class Accepted:
    pass


@infer(JSON_INTO, JSON_FROM)
@dataclass
class Denied:
    reason: str
    value: JsonAny = field(default=None, metadata={FIELD_FACTORY: JsonAnyAny()})


@infer(JSON_INTO, JSON_FROM)
@dataclass
class Request:
    method: str
    body: JsonAny = field(default=None, metadata={FIELD_FACTORY: JsonAnyAny()})


@infer(JSON_INTO, JSON_FROM)
@dataclass
class Error:
    type: str
    body: JsonAny = field(default=None, metadata={FIELD_FACTORY: JsonAnyAny()})


@infer(JSON_INTO, JSON_FROM)
@dataclass
class Cancel:
    reason: Optional[str] = None


@infer(JSON_INTO, JSON_FROM)
@dataclass
class Start:
    max_inflight: int
    body: JsonAny = field(default=None, metadata={FIELD_FACTORY: JsonAnyAny()})


@infer(JSON_INTO, JSON_FROM)
@dataclass
class Step:
    index: int
    body: JsonAny = field(default=None, metadata={FIELD_FACTORY: JsonAnyAny()})


@infer(JSON_INTO, JSON_FROM)
@dataclass
class StepAck:
    index: int
    max_inflight: Optional[int] = None


@infer(JSON_INTO, JSON_FROM)
@dataclass
class End:
    body: JsonAny = field(default=None, metadata={FIELD_FACTORY: JsonAnyAny()})


PACKET_TYPE_MAP = [
    (Type.Service, Service),
    (Type.Header, Header),

    (Type.Begin, Begin),
    (Type.Accepted, Accepted),
    (Type.Denied, Denied),

    (Type.Request, Request),
    (Type.Error, Error),
    (Type.Cancel, Cancel),
    (Type.Start, Start),
    (Type.Step, Step),
    (Type.StepAck, StepAck),
    (Type.End, End),
]
