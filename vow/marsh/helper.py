from typing import Type


def is_serializable(cls: Type):
    return hasattr(cls, '__serde__')