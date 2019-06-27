import inspect
from itertools import zip_longest

from dataclasses import fields, Field

from xrpc.trace import trc


class Empty:
    def __repr__(self):
        return 'EMPTY'


EMPTY = Empty()


def load_fun_parameters(it, is_bound=False):
    r = inspect.getfullargspec(it)

    args = list(reversed(list(r.args)))

    if is_bound:
        args = args[:-1]

    defs = list(r.defaults) if r.defaults else {}
    annot = r.annotations if r.annotations else {}

    name_default = {
        k: (v, annot.get(k, EMPTY)) for k, v in zip_longest(args, defs, fillvalue=EMPTY)
    }

    return name_default, annot.get('return', EMPTY)


def asdict_shallow(obj):
    r = {}
    for x in fields(obj):
        x: Field

        r[x.name] = getattr(obj, x.name)
    return r