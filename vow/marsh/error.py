from typing import List, Optional, Any

from dataclasses import MISSING, dataclass, replace, field, fields

from vow.marsh import Mapper


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
