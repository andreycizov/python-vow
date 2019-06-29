from enum import Enum
from typing import Optional

from dataclasses import dataclass, field

from vow.marsh import infer
from vow.marsh.helper import FIELD_FACTORY
from vow.marsh.impl.json import JSON_FROM, JSON_INTO, JsonAnyAny
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
    End = 'reply'


# basically anything that contains JsonAnyAny as a field
# is an envelope.

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
