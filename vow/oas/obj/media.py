import json
from typing import Dict

from dataclasses import dataclass

from vow.oas.data import JsonAny
from vow.oas.obj.schema import Schema


class Media:
    # todo convert all media to simple text-based media
    def serialize(self, schema: Schema, obj: JsonAny) -> bytes:
        raise NotImplementedError(f'{self.__class__.__name__}')

    def deserialize(self, schema: Schema, obj: bytes) -> JsonAny:
        raise NotImplementedError(f'{self.__class__.__name__}')

    @property
    def content_type(self):
        raise NotImplementedError(f'{self.__class__.__name__}')

    def __repr__(self):
        return f'{self.__class__.__name__}'


class JsonMedia(Media):

    def deserialize(self, schema: Schema, obj: bytes) -> JsonAny:
        return json.loads(obj)

    @property
    def content_type(self):
        return 'application/json'


class XmlMedia(Media):

    @property
    def content_type(self):
        return 'application/xml'


class FormMedia(Media):
    @property
    def content_type(self):
        return 'application/x-www-form-urlencoded'


class PlainTextMedia(Media):

    @property
    def content_type(self):
        return 'text/plain'


@dataclass
class TextMediaEncoding:
    allow_reserved: bool = False


class TextMedia(Media):
    encoding: Dict[str, TextMediaEncoding]

    @property
    def content_type(self):
        return 'text/plain'
