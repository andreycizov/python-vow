from typing import Dict, List, Tuple, Optional

from dataclasses import dataclass

from vow.oas.data import JsonAny
from vow.oas.obj.op import Placement, Parameter


@dataclass
class RequestError:
    pass


@dataclass
class ParameterMissing(RequestError):
    parameter: Parameter


@dataclass
class ContentType(RequestError):
    pass


@dataclass
class NoBody(RequestError):
    pass


@dataclass
class RequestEnvelopeParameter:
    name: str
    placement: Placement
    value: JsonAny


@dataclass
class RequestEnvelope:
    url: str
    method: str
    errors: List[RequestError]
    parameters: List[RequestEnvelopeParameter]
    body: JsonAny


@dataclass
class ResponseEnvelope:
    code: int
    content_type: str
    body: Optional[JsonAny]
