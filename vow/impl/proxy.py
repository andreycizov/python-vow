import asyncio
import inspect
import json
import struct
from argparse import ArgumentParser
from asyncio import StreamReader
from typing import Optional, Iterable, AsyncIterable

from vow.marsh.impl.json import JSON_INTO
from vow.marsh.walker import Walker
from vow.oas.data import JsonAny
from vow.reqrep import Packet, Type
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


async def json_frame_decoder(iter: AsyncIterable[bytes]) -> AsyncIterable[JsonAny]:
    async for x in iter:
        yield Packet.unpack(x)


async def handle_client(reader, writer):
    try:
        async for frame in json_frame_decoder(frame_decoder(reader)):
            trc('frame').debug('%r', frame)
    except ConnectionAbortedError:
        trc('disco').debug("")


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
    host, port = addr
    trc().debug('%s', 'created')
    await asyncio.sleep(0.33)

    fac = Walker(JSON_INTO)

    mapper, = fac.mappers(fac.resolve(Packet))

    trc().debug('%s', 'connecting')

    (reader, writer) = await asyncio.open_connection(host, port)

    trc().debug('%s', 'connecting')

    writer.write(Packet(Type.Header).pack())

    await asyncio.sleep(5)
    await writer.drain()
    writer.close()
    trc().debug('%s', 'exit')


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
