import asyncio
import inspect
import struct
from dataclasses import dataclass, field

from argparse import ArgumentParser
from asyncio import StreamReader, StreamWriter
from typing import AsyncIterable, Any, Generic, TypeVar, List

from vow.marsh.base import Mapper
from vow.marsh.decl import get_serializers
from vow.marsh.error import SerializationError, BUFFER_NEEDED
from vow.marsh.impl.binary import BINARY_INTO, BINARY_FROM, BinaryNext
from vow.marsh.impl.json import JSON_INTO
from vow.marsh.walker import Walker
from vow.oas.data import JsonAny
from vow.reqrep import Packet, Type, Service, Header, Begin, Denied, Accepted
from xrpc.logging import logging_parser, cli_main
from xrpc.trace import trc

RPC_PACKET_PACKER = struct.Struct('!L')


async def frame_decoder(reader: StreamReader, per_read=2048) -> AsyncIterable[bytes]:
    buffer = bytes()
    while True:
        while True:
            if len(buffer) >= RPC_PACKET_PACKER.size:
                curr_size, = RPC_PACKET_PACKER.unpack(buffer[:RPC_PACKET_PACKER.size])

                buffer = buffer[RPC_PACKET_PACKER.size:]

                trc('size').debug('%s', curr_size)
                break
            read = await reader.read(per_read)
            if len(read) == 0:
                raise ConnectionAbortedError()
            buffer += read
        while True:
            if len(buffer) >= curr_size:
                yield buffer[:curr_size]

                buffer = buffer[curr_size:]
                trc('buffer').debug('%s', curr_size)
                break

            read = await reader.read(per_read)
            if len(read) == 0:
                raise ConnectionAbortedError()
            buffer += read


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


async def handle_client(reader: StreamReader, writer: StreamWriter):
    reader: FrameReader[Packet] = FrameReader(reader, PACKET_MAPPER_FROM)
    writer: FrameWriter[Packet] = FrameWriter(writer, PACKET_MAPPER_INTO)

    async with reader, writer:
        try:
            service = await reader.read()

            if isinstance(service.body, Service):
                pass
            else:
                raise ProtocolError(f'{service}')

            assert isinstance(service.body, Service), service.body

            headers: List[Header] = []
            while True:
                header = await reader.read()

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


async def main_client(addr):
    try:
        host, port = addr
        trc().debug('%s', 'created')
        await asyncio.sleep(0.33)

        trc().debug('%s', 'connecting')

        reader, writer = await asyncio.open_connection(host, port)

        reader: FrameReader[Packet] = FrameReader(reader, PACKET_MAPPER_FROM)
        writer: FrameWriter[Packet] = FrameWriter(writer, PACKET_MAPPER_INTO)

        trc().debug('%s', 'connecting')

        await writer.write(Packet(None, Begin()))
        await writer.write(Packet(None, Service('rate_limiter2')))
        await writer.write(Packet(None, Header('Authorize', 'Bearer: 2342356')))
        await writer.write(Packet(None, Begin()))
        await writer.sync()

        item = await reader.read()

        trc().debug('%s', item)

        await asyncio.sleep(5)
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
