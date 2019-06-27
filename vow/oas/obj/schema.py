from collections import OrderedDict
from typing import Optional, Dict, List, Any

from dataclasses import dataclass, field

from vow.oas.data import JsonAny
from vow.oas.gen import Generated
from xrpc.trace import trc


def update_nullable(obj: 'Nullable', r):
    if obj.nullable is True:
        r['nullable'] = True


def update_default(obj: 'Defaulted', r):
    if obj.default is not None:
        val = obj.default
        r['default'] = val


def update_format(obj: 'Formatted', r):
    if obj.format is not None:
        r['format'] = obj.format


class Schema(Generated):
    pass


@dataclass
class Nullable:
    nullable: bool = False


@dataclass
class Formatted:
    format: Optional[str] = None


@dataclass
class Defaulted:
    default: Optional[JsonAny] = None


@dataclass
class NumberSchema(Schema, Formatted, Nullable, Defaulted):
    minimum: Optional[float] = None
    maximum: Optional[float] = None

    def serialize(self) -> JsonAny:
        r = OrderedDict()
        r['type'] = 'number'

        if self.maximum is not None:
            r['maximum'] = self.maximum

        if self.minimum is not None:
            r['minimum'] = self.minimum

        update_nullable(self, r)
        update_format(self, r)
        update_default(self, r)
        return r


@dataclass
class IntSchema(Schema, Formatted, Nullable, Defaulted):
    multiple_of: Optional[int] = None
    minimum: Optional[int] = None
    maximum: Optional[int] = None

    def serialize(self) -> JsonAny:
        r = OrderedDict()
        r['type'] = 'integer'

        if self.maximum is not None:
            r['maximum'] = self.maximum

        if self.minimum is not None:
            r['minimum'] = self.minimum

        update_nullable(self, r)
        update_format(self, r)
        update_default(self, r)

        return r


@dataclass
class StringSchema(Schema, Formatted, Nullable, Defaulted):
    pattern: Optional[str] = None
    enum: Optional[List[str]] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None

    def serialize(self) -> JsonAny:
        r = OrderedDict()
        r['type'] = 'string'

        if self.pattern:
            r['pattern'] = self.pattern

        if self.enum is not None:
            r['enum'] = self.enum

        if self.min_length is not None:
            r['minLength'] = self.min_length

        if self.max_length is not None:
            r['maxLength'] = self.max_length

        update_nullable(self, r)
        update_format(self, r)
        update_default(self, r)

        return r


@dataclass
class BooleanSchema(Schema, Nullable, Defaulted):

    def serialize(self) -> JsonAny:
        r = OrderedDict()
        r['type'] = 'boolean'

        update_nullable(self, r)
        update_default(self, r)

        return r


@dataclass
class ArraySchema(Schema, Nullable, Defaulted):
    items: Optional[Schema] = None
    min_items: Optional[int] = None
    max_items: Optional[int] = None

    def serialize(self) -> JsonAny:
        r = OrderedDict()
        r['type'] = 'array'

        update_nullable(self, r)
        update_default(self, r)

        if self.min_items is not None:
            r['minItems'] = self.min_items

        if self.max_items is not None:
            r['maxItems'] = self.max_items

        if self.items:
            r['items'] = self.items.serialize()

        return r


@dataclass
class ObjectSchemaProperty:
    schema: Schema
    read_only: bool = False
    write_only: bool = False

    def __repr__(self) -> str:
        return f'Prop({repr(self.schema)}, R={self.read_only} W={self.write_only})'


@dataclass
class ObjectSchema(Schema, Nullable, Defaulted):
    name: Optional[str] = None

    properties: Dict[str, ObjectSchemaProperty] = field(default_factory=dict)
    required: List[str] = field(default_factory=list)
    min_properties: Optional[int] = None
    max_properties: Optional[int] = None
    additional: Optional[Schema] = None

    extensions: Dict[str, JsonAny] = field(default_factory=dict)

    def serialize(self) -> JsonAny:
        r = OrderedDict()

        r['type'] = 'object'

        if self.additional:
            r['additionalProperties'] = self.additional.serialize()

        if len(self.properties):
            ppts = {}
            for name, prop in self.properties.items():
                ppt = {}

                ppt.update(prop.schema.serialize())

                if prop.read_only:
                    ppt['readOnly'] = True

                if prop.write_only:
                    ppt['writeOnly'] = True

                ppts[name] = ppt

            r['properties'] = ppts
        update_nullable(self, r)
        update_default(self, r)

        if self.extensions:

            for k, v in self.extensions.items():
                r['x-' + k] = v

        return r


@dataclass
class RefSchema(Schema):
    path: str

    def serialize(self) -> JsonAny:
        return {
            '$ref': f'#{self.path}'
        }

