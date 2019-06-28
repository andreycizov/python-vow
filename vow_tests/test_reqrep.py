import unittest
from collections import OrderedDict
from pprint import pprint

from xrpc.trace import trc

from vow.marsh.impl.json import JSON_FROM, JSON_INTO
from vow.marsh.walker import Walker
from vow.reqrep import Packet, Type


class TestRequests(unittest.TestCase):
    def test_api_1(self):
        pkt = Packet(Type.Service, None, {'a': [{'a': 'c'}]})

        wlkr1 = Walker(JSON_FROM)
        wlkr2 = Walker(JSON_INTO)

        fac1 = wlkr1.resolve(Packet)
        fac2 = wlkr2.resolve(Packet)

        ser1, = wlkr1.mappers(fac1)
        ser2, = wlkr2.mappers(fac2)

        out1 = ser2.serialize(pkt)
        out2 = ser1.serialize(out1)

        self.assertEqual(OrderedDict([('type', 'service'),
                                      ('stream', None),
                                      ('body', {'a': [{'a': 'c'}]})]), out1)

        trc().debug()

        self.assertEqual(pkt, out2)