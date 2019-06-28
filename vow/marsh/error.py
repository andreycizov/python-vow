from dataclasses import MISSING


class SerializationError(Exception):
    def __init__(self, val=MISSING, path=None, reason=None, **kwargs):
        if path is None:
            path = []
        self.val = val
        self.path = path
        self.reason = reason
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.path}, val=`{repr(self.val)}`)'

    def with_path(self, *path):
        return SerializationError(path=path + self.path, val=self.val, reason=self.reason)
