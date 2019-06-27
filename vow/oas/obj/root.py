from collections import OrderedDict
from typing import List, Optional, Dict

from dataclasses import dataclass, field

from vow.oas.data import JsonAny
from vow.oas.gen import Generated
from vow.oas.obj.op import Server, Path
from vow.oas.obj.schema import Schema


@dataclass
class Contact:
    name: str
    url: Optional[str] = None
    email: Optional[str] = None

    def serialize(self) -> JsonAny:
        r = OrderedDict()

        r['name'] = self.name

        if self.url is not None:
            r['url'] = self.url

        if self.email is not None:
            r['email'] = self.email

        return r


@dataclass
class License(Generated):
    name: str
    url: Optional[str]

    def serialize(self) -> JsonAny:
        r = OrderedDict()

        r['name'] = self.name

        if self.url is not None:
            r['url'] = self.url

        return r


@dataclass
class Info(Generated):
    title: str = 'Exemplary'
    version: str = '1.0.0'
    contact: Optional[Contact] = None
    license: Optional[License] = None
    description: Optional[str] = None
    terms_of_service: Optional[str] = None

    def serialize(self) -> JsonAny:
        r = OrderedDict()

        r['title'] = self.title
        r['version'] = self.version

        if self.contact:
            r['contact'] = self.contact.serialize()

        if self.license:
            r['license'] = self.license.serialize()

        if self.description is not None:
            r['description'] = self.description

        if self.terms_of_service is not None:
            r['termsOfService'] = self.terms_of_service

        return r


@dataclass
class Components(Generated):
    schemas: Dict[str, Schema] = field(default_factory=dict)

    def serialize(self) -> JsonAny:
        r = OrderedDict()

        if self.schemas:
            r['schemas'] = {}

            for k, v in sorted(self.schemas.items()):
                r['schemas'][k] = v.serialize()

        return r


@dataclass
class OAS(Generated):
    info: Info = field(default_factory=Info)
    servers: List[Server] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    paths: List[Path] = field(default_factory=list)
    components: Components = field(default_factory=Components)
    version: str = '3.0.1'

    def serialize(self) -> JsonAny:
        r = OrderedDict()

        r['openapi'] = self.version

        r['info'] = self.info.serialize()

        for server in self.servers:
            if 'servers' not in r:
                r['servers'] = []

            r['servers'].append(server.serialize())

        if self.tags:
            r['tags'] = self.tags

        r['paths'] = {}

        for path in self.paths:
            r['paths'][path.path] = path.serialize()

        rr = self.components.serialize()

        if rr:
            r['components'] = rr

        return r
