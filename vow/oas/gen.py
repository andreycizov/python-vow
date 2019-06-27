from collections import OrderedDict
from typing import TypeVar

import yaml

from vow.oas.data import JsonAny

T = TypeVar('T', bound='Generated')


class Generated:
    def serialize(self) -> JsonAny:
        raise NotImplementedError(f'`{self.__class__.__name__}`')

    # def deserialize(self: T, x: SchemaAny) -> T:
    #     raise NotImplementedError('')


def update_deprecated(obj, r):
    if getattr(obj, 'deprecated', False) is not False:
        r['deprecated'] = obj.deprecated


def update_summarydesc(obj, r):
    if getattr(obj, 'description', None) is not None:
        r['description'] = obj.description

    if getattr(obj, 'summary', None) is not None:
        r['summary'] = obj.summary


_mapping_tag = yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG


def dict_representer(dumper, data: OrderedDict):
    return dumper.represent_dict(data.items())


def dict_constructor(loader, node):
    return OrderedDict(loader.construct_pairs(node))


yaml.add_representer(OrderedDict, dict_representer)


# yaml.add_constructor(_mapping_tag, dict_constructor)


def generate_yaml(obj):
    return yaml.dump_all([obj], canonical=False, width=32423432, indent=10000, default_flow_style=False)
