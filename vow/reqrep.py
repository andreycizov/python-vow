import json
from enum import Enum
from typing import Optional, Any, Tuple

from dataclasses import dataclass, field

from vow.marsh import infer, Fac, SerializationError
from vow.marsh.base import FieldsFac, Mapper
from vow.marsh.helper import FIELD_FACTORY
from vow.marsh.impl.binary_from import BinaryFromVarInt, BinaryFromBytes
from vow.marsh.impl.binary_into import BinaryIntoVarInt
from vow.marsh.impl.json import JSON_FROM, JSON_INTO, JsonAnyAny
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
    Iteration = 'cont'
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
    body: JsonAny = field(default=None, metadata={FIELD_FACTORY: JsonAnyAny()})


@infer(JSON_INTO, JSON_FROM)
@dataclass
class Packet:
    type: Type
    stream: Optional[str] = None
    body: JsonAny = field(default=None, metadata={FIELD_FACTORY: JsonAnyAny()})


@infer(JSON_INTO, JSON_FROM)
@dataclass
class Service:
    name: str
    version: Optional[str] = None


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


WALKER_JSON_INTO = Walker(JSON_INTO)
WALKER_JSON_FROM = Walker(JSON_FROM)
PACKET_MAPPER_INTO, = WALKER_JSON_INTO.mappers(WALKER_JSON_INTO.resolve(Packet))
PACKET_MAPPER_FROM, = WALKER_JSON_FROM.mappers(WALKER_JSON_FROM.resolve(Packet))
