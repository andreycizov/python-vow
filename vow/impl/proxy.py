import asyncio
import inspect
from dataclasses import dataclass, field

from argparse import ArgumentParser
from asyncio import StreamReader, StreamWriter
from typing import Generic, TypeVar, List, Dict, Tuple, Optional

from vow.marsh.base import Mapper
from vow.marsh.decl import get_serializers
from vow.marsh.error import SerializationError, BUFFER_NEEDED
from vow.marsh.impl.binary import BINARY_INTO, BINARY_FROM, BinaryNext
from vow.rpc.wire import Packet, Service, Header, Begin, Denied, Accepted, PacketType, Request, Error, End
from xrpc.logging import logging_parser, cli_main
from xrpc.trace import trc

PACKET_MAPPER_INTO, = get_serializers(BINARY_INTO, Packet)
PACKET_MAPPER_FROM, = get_serializers(BINARY_FROM, Packet)

T = TypeVar('T')


@dataclass
class FrameReader(Generic[T]):
    reader: StreamReader
    mapper: Mapper
    read_size: int = 2048
    buffer: bytes = field(default_factory=bytes)

    async def read(self) -> T:
        while True:
            try:
                r: BinaryNext = self.mapper(self.buffer)

                self.buffer = bytes(r.next)

                return r.val
            except SerializationError as e:
                if e.reason == BUFFER_NEEDED:
                    read = await self.reader.read(self.read_size)

                    if len(read) == 0:
                        raise ConnectionAbortedError()

                    self.buffer += read

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


@dataclass
class FrameWriter(Generic[T]):
    writer: StreamWriter
    mapper: Mapper

    async def write(self, item: T):
        self.writer.write(self.mapper(item))

    async def sync(self):
        await self.writer.drain()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.writer.close()


class ProtocolError(Exception):
    pass


class CallError(Exception):
    pass


async def handle_client(reader: StreamReader, writer: StreamWriter):
    reader: FrameReader[Packet] = FrameReader(reader, PACKET_MAPPER_FROM)
    writer: FrameWriter[Packet] = FrameWriter(writer, PACKET_MAPPER_INTO)

    async with reader, writer:
        try:
            service = await reader.read()

            if service.stream is not None:
                raise ProtocolError(f'invalid stream: {service.stream}')

            if isinstance(service.body, Service):
                pass
            else:
                raise ProtocolError(f'{service}')

            headers: List[Header] = []
            while True:
                header = await reader.read()

                if header.stream is not None:
                    raise ProtocolError(f'invalid stream: {service.stream}')

                if isinstance(header.body, Header):
                    headers.append(header.body)
                elif isinstance(header.body, Begin):
                    break
                else:
                    raise ProtocolError(f'{header}')

            if service.body.name == 'rate_limiter':
                await writer.write(Packet(None, Accepted()))
            else:
                await writer.write(Packet(None, Denied('service unknown', None)))
                await writer.sync()
                return

            trc().debug('')

            while True:
                packet = await reader.read()

                trc().debug('%s', packet)

                if packet.stream is None:
                    raise ProtocolError(f'{packet}')

                if isinstance(packet.body, Request):
                    await writer.write(Packet(packet.stream, End(packet.body.body)))
                else:
                    await writer.write(Packet(packet.stream, Error('invalid')))

        except ProtocolError:
            trc('disco').exception("Exception while communicating")
        except ConnectionAbortedError:
            trc('disco').debug("")

        # todo this is still not enough
        # todo writer may stall on trying to close the client that never reads


async def handle_clients(queue: asyncio.Queue, queue_out: asyncio.Queue):
    while True:
        trc('1').debug('w')
        reader, writer = await queue.get()

        trc('0').debug('connected')

        task = asyncio.create_task(handle_client(reader, writer))

        await queue_out.put(task)


async def handle_client_exits(queue: asyncio.Queue):
    """makes sure that all of the exceptions in client threads are propagated upwards"""
    # todo move this to handle_clients - should be enough
    tasks = set()
    tasks_fut = None
    while True:
        queue_get_fut = asyncio.create_task(queue.get())
        both_futs = {queue_get_fut}

        if tasks_fut:
            both_futs.add(tasks_fut)

        trc('4').debug('%s', both_futs)

        done, pending = await asyncio.wait(both_futs, return_when=asyncio.FIRST_COMPLETED)

        trc('0').debug('%s', tasks_fut)

        if queue_get_fut in done:
            task: asyncio.Task = await queue_get_fut

            trc('1').debug('%s', task)

            tasks.add(task)

            if tasks_fut:
                tasks_fut.cancel()

            tasks_fut = asyncio.create_task(asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED))
        if tasks_fut is not None and tasks_fut in done:
            done, pending = await tasks_fut

            trc('2').debug('%s', done)
            for x in done:
                await x
                tasks.remove(x)

            if len(tasks):
                tasks_fut = asyncio.create_task(asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED))
            else:
                tasks_fut = None


async def main_server(addr):
    host, port = addr

    queue = asyncio.Queue()
    queue_out = asyncio.Queue()

    async def _handle_client(reader, writer):
        await queue.put((reader, writer))

    task1 = asyncio.create_task(handle_clients(queue, queue_out))
    task2 = asyncio.create_task(handle_client_exits(queue_out))

    server = await asyncio.start_server(
        _handle_client, host, port)

    addr = server.sockets[0].getsockname()

    trc().debug('%s', addr)

    async with server:
        await asyncio.gather(
            server.serve_forever(),
            task1,
            task2
        )


API_VERSION = '0.1.0'


@dataclass
class ClientChannel(Generic[T]):
    stream_id: str
    client: Optional['Client']
    buffer: List[T] = field(default_factory=list)

    async def read(self) -> T:
        return await self.client.reader_mailboxes[self.stream_id].get()

    async def write(self, obj: T):
        self.buffer.append(obj)

    async def sync(self):
        await self.client.writer_mailbox.put([
            (self.stream_id, x) for x in self.buffer
        ])

        self.buffer = []

    async def close(self):
        del self.client.reader_mailboxes[self.stream_id]

        self.client = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if len(self.buffer):
            await self.sync()

        await self.close()


@dataclass
class Client:
    reader: FrameReader[Packet]
    writer: FrameWriter[Packet]

    chan_ctr: int = 0

    writer_mailbox: asyncio.Queue = field(default_factory=asyncio.Queue)
    reader_mailboxes: Dict[str, asyncio.Queue] = field(default_factory=dict)

    sender_task: Optional[asyncio.Task] = None
    receiver_task: Optional[asyncio.Task] = None

    def __post_init__(self):
        self._background_start()

    def channel(self) -> ClientChannel:
        r = ClientChannel(str(self.chan_ctr), self)
        self.chan_ctr += 1
        self.reader_mailboxes[r.stream_id] = asyncio.Queue()
        return r

    async def sender_coro(self):
        try:
            while True:
                items: List[Tuple[str, PacketType]] = await self.writer_mailbox.get()
                for stream_id, packet_body in items:
                    trc().debug('%s %s', stream_id, packet_body)
                    await self.writer.write(Packet(stream_id, packet_body))

                await self.writer.sync()
        except asyncio.CancelledError:
            trc('cancelled').debug('')
        except:
            trc().exception('')

    async def receiver_coro(self):
        try:
            while True:
                packet = await self.reader.read()

                if packet.stream is not None:
                    mb = self.reader_mailboxes.get(packet.stream)
                    if mb is None:
                        raise KeyError(f'{packet.stream}')
                    await mb.put(packet.body)
                else:
                    raise ProtocolError(f'post-nego:{packet}')
        except asyncio.CancelledError:
            trc('cancelled').debug('')
        except:
            trc().exception('')

    @classmethod
    async def connect(cls, host, port, service, version, headers: Dict[str, str] = None, proto=API_VERSION) -> 'Client':
        if headers is None:
            headers = {}

        # todo we need to add a timeout here (or somewhere else)
        reader, writer = await asyncio.open_connection(host, port)

        reader: FrameReader[Packet] = FrameReader(reader, PACKET_MAPPER_FROM)
        writer: FrameWriter[Packet] = FrameWriter(writer, PACKET_MAPPER_INTO)

        await writer.write(Packet(None, Service(name=service, version=version, proto=proto)))

        for k, v in headers.items():
            await writer.write(Packet(None, Header(k, v)))
        await writer.write(Packet(None, Begin()))
        await writer.sync()

        item = await reader.read()

        if isinstance(item.body, Accepted):
            trc().debug('connected to %s %s %s', host, port, service)
        elif isinstance(item.body, Denied):
            trc().debug('denied to %s %s %s', host, port, service)
            await writer.__aexit__(None, None, None)
            raise ConnectionAbortedError(f'{item.body}')
        else:
            raise ProtocolError(f'{item}')

        return Client(reader, writer)

    def _background_start(self):
        self.sender_task = asyncio.create_task(self.sender_coro())
        self.receiver_task = asyncio.create_task(self.receiver_coro())

    async def __aenter__(self):
        trc().debug('')
        """
        - create a channel for reading data
        - return a ClientChanneler
        :return:
        """

        return self

    async def __aexit__(self, *args):
        trc().debug('')
        self.sender_task.cancel()
        self.receiver_task.cancel()

        self.sender_task = None
        self.receiver_task = None

        await self.reader.__aexit__(*args)
        await self.writer.__aexit__(*args)


async def main_client(addr):
    try:
        trc().debug('%s', 'created')
        await asyncio.sleep(0.33)

        trc().debug('%s', 'connecting')

        async with await Client.connect(
                *addr, 'rate_limiter', '0.1.0',
                {
                    'Authorize': 'Bearer: 123123'
                }
        ) as client:
            client: Client

            chan1 = client.channel()
            chan2 = client.channel()

            xx = await asyncio.gather(chan1.write(Request('get')), chan2.write(Request('put', {'a': 'b'})))
            await asyncio.gather(chan1.sync(), chan2.sync())

            trc('xx').debug('%s', xx)

            yy = await asyncio.gather(chan1.read(), chan2.read())

            trc('yy').debug('%s', yy)

            await asyncio.sleep(1)

            yy = await asyncio.gather(chan1.close(), chan2.close())

            pass
    except:
        trc().exception('$%')


async def main_async():
    addr = ('127.0.0.1', 8888)
    task1 = asyncio.create_task(main_server(addr))
    task2 = asyncio.create_task(main_client(addr))

    await asyncio.gather(task1, task2)


def main(**kwargs):
    async def googly():
        pass

    r = inspect.getfullargspec(googly)

    trc('brute').debug('%s', r)

    asyncio.run(main_async())


def parser():
    parser = ArgumentParser()

    logging_parser(parser)

    return parser


if __name__ == '__main__':
    cli_main(main, parser())
