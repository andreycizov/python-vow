from typing import Any, Union

from dataclasses import dataclass

BINARY_FROM = 'bin_from'
BINARY_INTO = 'bin_into'


@dataclass
class BinaryNext:
    val: Any
    next: Union[bytes, memoryview]

    def __repr__(self):
        bts = bytes(self.next)

        if len(bts) > 20:
            bts = f'<{repr(bts[:20])}...{len(bts)}>'
        else:
            bts = repr(bts)

        return f'{self.__class__.__name__}({repr(self.val)}, {bts})'
