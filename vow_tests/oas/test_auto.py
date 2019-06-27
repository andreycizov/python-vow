import logging
import unittest
from datetime import datetime
from pprint import pformat
from typing import Optional, List

from dataclasses import dataclass

from vow.oas.auto import auto_dataclass, auto_actor, auto_any
from vow.oas.decl import path, parameter, body
from vow.oas.gen import generate_yaml
from vow.oas.obj.root import OAS
from xrpc.trace import trc


@dataclass
class Morbid:
    a: int
    b: Optional[int]


class Executioner:

    @path('/', 'post', 'loadEntities', tags=['beta', 'gamma', 'theta'])
    @parameter('name')
    @body('glob')
    def load_entities(self, name: datetime, glob: str = 'abc', **kwargs) -> str:
        pass

    @path('/{id}', 'post', 'loadEntities3', tags=['beta', 'gamma', 'theta'])
    @body('glob')
    def load_entities2(self, id: datetime, glob: str = 'abc', **kwargs) -> str:
        pass


class AutoTest(unittest.TestCase):

    def test_auto_0(self):
        r = auto_dataclass(Morbid)

        trc().debug('%s', pformat(r))

    def test_auto_2(self):
        r = auto_any(List[int])

        trc().debug('%s', pformat(r))

        self.assertFalse(True)

    def test_gen_2(self):
        r = auto_actor(Executioner)

        r = OAS(paths=r).serialize()
        trc('0').debug('%s', pformat(r))
        r = generate_yaml(r)
        trc('1').debug('%s', '\n' + r)
