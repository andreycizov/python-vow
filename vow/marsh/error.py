import sys
from contextlib import contextmanager
from typing import List, Optional, Any

from dataclasses import MISSING, dataclass, replace, field, fields

from vow.marsh.base import Mapper

BUFFER_NEEDED = 'buffer_overrun'


@dataclass
class SerializationError(Exception):
    val: Any = MISSING
    path: List[str] = field(default_factory=list)
    reason: Optional[str] = None
    origin: Optional[Mapper] = None
    exc: Optional[Exception] = None

    def __repr__(self):
        flds = [(x.name, getattr(self, x.name)) for x in fields(self)]
        flds = [(k, v if v != MISSING else '<none>') for k, v in flds]
        flds = [f'{k}={v}' for k, v in flds]
        flds = ', '.join(flds)

        r = f'{self.__class__.__name__}({flds})'

        return r

    def __str__(self) -> str:
        return repr(self)

    def with_path(self, *path):
        return replace(self, path=list(path) + self.path)


class subserializer:
    def __init__(self, *path):
        self.path = path

    def __enter__(self):
        pass

    def __exit__(self, exc_type, e, exc_tb):
        if exc_type is not None:
            if issubclass(exc_type, SerializationError):
                raise e.with_path(*self.path).with_traceback(exc_tb) from None

# @contextmanager
# def subserializer(*path):

#    return Subserializer(list(path))
#     try:
#         yield
#     except SerializationError as e:
#         exc_info = sys.exc_info()
#         raise e.with_path(*path).with_traceback(exc_info[2]) from None
