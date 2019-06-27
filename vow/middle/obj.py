from collections import deque
from typing import Optional, Any, Callable, List, Type

from dataclasses import dataclass, field, MISSING
from xrpc.trace import trc

from vow.oas.data import JsonAny
from vow.oas.envelope import RequestEnvelope
from vow.marsh.base import Mapper
from vow.marsh.walker import Walker


@dataclass
class Argument:
    name: str
    type: Type
    default: Optional[Any] = MISSING
    mapper: Optional[Mapper] = None


@dataclass
class Arguments:
    items: List[Argument] = field(default_factory=list)

    def __iter__(self):
        return iter(self.items)

    def __getitem__(self, item):
        return [x for x in self.items if x.name == item][0]


@dataclass
class Endpoint:
    """
    defines a single Method in OAS
    subsequently this needs to know it's own respective types
    so that it could later on serialize/deserialize into OAS

    this can be auto-generated
     - from OAS (into very basic python types)
     - from method definitions themselves

    """
    operation_id: str
    url: str
    method: str = 'get'

    arguments: Arguments = field(default_factory=Arguments)

    body_name: Optional[str] = None
    body: Optional[Type] = None
    body_mapper: Optional[Mapper] = None
    response: Optional[Type] = None
    response_mapper: Optional[Mapper] = None


@dataclass
class Endpoints:
    items: List[Endpoint]

    def __getitem__(self, item):
        return [x for x in self.items if x.operation_id == item][0]

    def mappify(self, name_req: str, name_rep: str) -> 'Endpoints':
        walker_req = Walker(name_req)

        factories_req = []

        for endpoint in self.items:
            for arg in endpoint.arguments:
                factories_req.append(walker_req.resolve(arg.type))

            if endpoint.body:
                factories_req.append(walker_req.resolve(endpoint.body))

        mappers_req = iter(walker_req.mappers(*factories_req))

        for endpoint in self.items:
            for arg in endpoint.arguments:
                arg.mapper = next(mappers_req)

            if endpoint.body:
                endpoint.body_mapper = next(mappers_req)

        walker_rep = Walker(name_rep)

        factories_rep = []

        for endpoint in self.items:
            if endpoint.response:
                factories_rep.append(walker_rep.resolve(endpoint.response))

        mappers_rep = iter(walker_rep.mappers(*factories_rep))

        for endpoint in self.items:
            if endpoint.response:
                endpoint.response_mapper = next(mappers_rep)

        return self


@dataclass
class Method:
    operation_id: str
    callable: Any

    def call(self, endpoint: Endpoint, envelope: RequestEnvelope) -> JsonAny:
        kwargs = {}
        arguments_missed = set([x.name for x in endpoint.arguments])
        trc('4').debug('%s', endpoint)
        for p in envelope.parameters:
            arguments_missed.remove(p.name)
            kwargs[p.name] = endpoint.arguments[p.name].mapper.serialize(p.value)

        trc('1').debug('%s', endpoint.body)

        if endpoint.body:
            kwargs[endpoint.body_name] = endpoint.body_mapper.serialize(envelope.body)

        for x in arguments_missed:
            assert endpoint.arguments[x].default is not MISSING
            kwargs[x] = endpoint.arguments[x].default

        trc('0').debug('%s', kwargs)
        ret = self.callable(**kwargs)

        trc('2').debug('%s', endpoint.response)
        if endpoint.response:
            return endpoint.response_mapper.serialize(ret)


@dataclass
class Methods:
    items: List[Method]

    def __getitem__(self, item):
        return [x for x in self.items if x.operation_id == item][0]
