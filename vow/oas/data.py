from typing import Union, List, Dict

JsonStr = str
JsonFloat = float
JsonInt = int
JsonBool = bool
JsonBytes = bytes
JsonNull = None.__class__

JsonAtom = Union[JsonBytes, JsonStr, JsonFloat, JsonInt, JsonNull, JsonBool]

JsonList = List['JsonAny']
JsonDict = Dict[JsonStr, 'JsonAny']

JsonComplex = Union[JsonList, JsonDict]

JsonAny = Union[JsonAtom, JsonComplex]
