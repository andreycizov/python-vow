from enum import Enum

from dataclasses import dataclass

from vow.oas.data import JsonAny


class Type(Enum):
    # client sends the service request
    Service = 'service'
    # initiates the request
    Request = 'request'
    # replies to a request
    Reply = 'reply'
    Iteration = 'cont'
    IterationFinish = 'finish'


@dataclass
class Packet:
    type: Type
    id: str
    body: JsonAny
