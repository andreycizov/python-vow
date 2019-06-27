from vow.oas import data
from vow.oas.obj.op import Style, Placement
from vow.oas.obj import schema


def simple_any_any_ser(cls: schema.Schema, value: str):
    return str(value)


def simple_any_any_des(cls: schema.Schema, value: str):
    return MAP[cls.__class__](value)


def simple_any_array_ser(cls: schema.Schema, value: data.JsonList) -> str:
    return ','.join([str(x) for x in value])


def simple_any_array_des(cls: schema.ArraySchema, value: str) -> data.JsonList:
    return [simple_any_any_des(cls.items, x) for x in value.split(',')]


def simple_any_dict_ser(cls: schema.Schema, value: data.JsonDict) -> str:
    return ','.join([str(k) + ',' + str(v) for k, v in value.items()])


# def simple_

# there's also the format concept!

MAP = {
    schema.NumberSchema: data.JsonFloat,
    schema.IntSchema: data.JsonInt,
    schema.StringSchema: data.JsonStr,
    schema.BooleanSchema: data.JsonBool,
    schema.ArraySchema: data.JsonList,
    schema.ObjectSchema: data.JsonDict,
}

SERIALIZER = {
    (Placement.Path, Style.Simple, False): {
        None: (simple_any_any_ser, simple_any_any_des),
        schema.ArraySchema: (simple_any_array_ser, simple_any_any_des),
        schema.ObjectSchema: (simple_any_dict_ser, None),
    },
    (Placement.Path, Style.Simple, False): {
        None: (simple_any_any_ser, simple_any_any_des),
        schema.ArraySchema: (simple_any_array_ser, simple_any_any_des),
        schema.ObjectSchema: (simple_any_dict_ser, None),
    },
    (Placement.Query, Style.Form, True): {
        None: (simple_any_any_ser, simple_any_any_des),
        schema.ArraySchema: (None, None),
        schema.ObjectSchema: (None, None),
    },
    (Placement.Path, Style.Form, True): {
        None: (simple_any_any_ser, simple_any_any_des),
        schema.ArraySchema: (None, None),
        schema.ObjectSchema: (None, None),
    },
    (Placement.Path, Style.Form, False): {
        None: (simple_any_any_ser, simple_any_any_des),
        schema.ArraySchema: (None, None),
        schema.ObjectSchema: (None, None),
    },
}