import unittest
from collections import OrderedDict

from xrpc.trace import trc

from vow.marsh.impl.binary import BINARY_FROM, BINARY_INTO
from vow.marsh.impl.json import JSON_FROM, JSON_INTO
from vow.marsh.walker import Walker
from vow.wire import Packet, Type, Service


class TestRequests(unittest.TestCase):
    def test_api_1(self):
        pkt = Packet(None, Service('ratelimiter'))

        wlkr1 = Walker(JSON_FROM)
        wlkr2 = Walker(JSON_INTO)

        fac1 = wlkr1.resolve(Packet)
        fac2 = wlkr2.resolve(Packet)

        json_from, = wlkr1.mappers(fac1)
        json_into, = wlkr2.mappers(fac2)

        out1 = json_into.serialize(pkt)

        trc('test_api_1').debug('%s', out1)

        out2 = json_from.serialize(out1)

        trc('test_api_2').debug('%s', out2)

        self.assertEqual(OrderedDict([('type', 'service'),
                                      ('stream', None),
                                      ('body', {'version': '0.1.0', 'name': 'ratelimiter', 'proto': '0.1.0'})]), out1)

        self.assertEqual(pkt, out2)

    def test_api_2(self):
        pkt = Packet(None, Service('ratelimiter'))

        wlkr1 = Walker(BINARY_FROM)
        wlkr2 = Walker(BINARY_INTO)

        fac1 = wlkr1.resolve(Packet)
        fac2 = wlkr2.resolve(Packet)

        json_from, = wlkr1.mappers(fac1)
        json_into, = wlkr2.mappers(fac2)

        out1 = json_into.serialize(pkt)

        trc('test_api_1').debug('%s', out1)

        out2 = json_from.serialize(out1)

        trc('test_api_2').debug('%s', out2)

        self.assertEqual(out1[0], len(out1[1:]))

        self.assertEqual(pkt, out2.val)
