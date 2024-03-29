from typing import Type

DECL_ATTR = '__serde__'
DECL_CALLABLE_ATTR = '__serde_callable__'
FIELD_OVERRIDE = '__marsh_override__'
FIELD_FACTORY = '__marsh_factory__'


def is_serializable(cls: Type):
    return hasattr(cls, DECL_ATTR)
