import asyncio
from time import sleep
from functools import partial
import multiprocessing
from queue import Empty as EmptyQException

import pytest

from datachunks import chunks, achunks, ChunkingFeeder, AsyncChunkingFeeder


def sync_chunks_tc(num: int, chunk_size: int, expected_answer):
    assert list(chunks(range(num), chunk_size)) == expected_answer


async def arange(num):
    for res in range(num):
        yield res


def async_chunks_tc(num: int, chunk_size: int, expected_answer):

    async def do_it_async():
        assert [el async for el in achunks(arange(num), chunk_size)] == expected_answer

    asyncio.run(do_it_async())


def do_test_chunks(testing_func):
    # Empty source
    testing_func(0, 5, [])

    # Ordinary cases
    testing_func(10, 5, [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]])
    testing_func(12, 5, [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9], [10, 11]])
    testing_func(2, 5, [[0, 1], ])

    # Weird but correct chunk sizes
    # 1. Chunk sizes < 1 work like chunk_size == 1
    testing_func(3, 0, [[0, ], [1, ], [2, ], ])
    testing_func(3, -100, [[0, ], [1, ], [2, ], ])
    # 2. Floating-point chunk sizes work like nearest bigger integer
    testing_func(10, 4.1, [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]])
    testing_func(12, 4.1, [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9], [10, 11]])


def test_chunks():
    do_test_chunks(sync_chunks_tc)

    with pytest.raises(ValueError) as exc:
        list(chunks(arange(1), 1))
    assert 'Use `achunks` function' in str(exc.value)


def test_achunks():
    do_test_chunks(async_chunks_tc)

    with pytest.raises(ValueError) as exc:

        async def do_it_async():
            return [el async for el in achunks(range(1), 1)]

        asyncio.run(do_it_async())
    assert 'Use `chunks` function' in str(exc.value)


def sync_consumer(consumed, chunk_size: int, pause_func, chunk: list):
    assert isinstance(chunk, list)
    assert 0 < len(chunk) <= chunk_size
    if pause_func:
        if isinstance(pause_func, (int, float)):
            sleep(pause_func)
        elif callable(pause_func):
            sleep(pause_func(chunk))
    if isinstance(consumed, list):
        consumed += chunk
    elif isinstance(consumed, dict) and 'queue' in consumed:
        for val in chunk:
            consumed['queue'].put(val)


async def async_consumer(consumed: list, chunk_size: int, pause_func, chunk: list):
    assert isinstance(chunk, list)
    assert 0 < len(chunk) <= chunk_size
    if pause_func:
        if isinstance(pause_func, (int, float)):
            await asyncio.sleep(pause_func)
        elif callable(pause_func):
            await asyncio.sleep(pause_func(chunk))
    consumed += chunk


def do_test_ceck_params_feeders(class_to_test, right_consumer, wrong_consumer):
    # Test initialization params check
    with pytest.raises(ValueError) as exc:
        class_to_test(1, 1)
    assert '"callback" parameter' in str(exc.value)

    with pytest.raises(ValueError) as exc:
        class_to_test(right_consumer, 'haha')
    assert '"chunk_size" parameter' in str(exc.value)

    with pytest.raises(ValueError) as exc:
        class_to_test(right_consumer, 1, workers_num='haha')
    assert '"workers_num" parameter' in str(exc.value)

    with pytest.raises(ValueError) as exc:
        class_to_test(partial(wrong_consumer, [], 3, None), 3)
    assert '"callback" parameter' in str(exc.value)


class recursive_feeding_tester():
    def __init__(self):
        self.produced = []
        self.consumed = []
        self.feeder = None

    def sync_consumer(self, chunk):
        # Produces 100 additional items on value 13
        self.consumed += chunk
        if 13 in chunk:
            for val in range(100, 200):
                self.produced.append(val)
                self.feeder.put(val)

    async def async_consumer(self, chunk):
        # Produces 100 additional items on value 13
        self.consumed += chunk
        if 13 in chunk:
            for val in range(100, 200):
                self.produced.append(val)
                await self.feeder.aput(val)

    def sync_do_test(self):
        with ChunkingFeeder(self.sync_consumer, 3, workers_num=2) as self.feeder:
            for val in range(14):
                self.produced.append(val)
                self.feeder.put(val)
        # Test feeding after exiting context
        self.produced.append(42)
        self.feeder.put(42)
        assert sorted(self.produced) == sorted(self.consumed)

    async def async_do_test(self):
        async with AsyncChunkingFeeder(self.async_consumer, 3, workers_num=2) as self.feeder:
            for val in range(14):
                self.produced.append(val)
                await self.feeder.aput(val)
        # Test feeding after exiting context
        self.produced.append(42)
        await self.feeder.aput(42)
        assert sorted(self.produced) == sorted(self.consumed)


def test_chunkingfeeder():
    do_test_ceck_params_feeders(ChunkingFeeder, sync_consumer, async_consumer)

    # Simple case with one consumer
    produced = []
    consumed = []
    with ChunkingFeeder(
            partial(sync_consumer, consumed, 3, None),
            3) as feeder:
        for val in range(10):
            produced.append(val)
            feeder.put(val)
    assert sorted(produced) == sorted(consumed)

    # Feeding before entering context
    feeder = ChunkingFeeder(partial(sync_consumer, [], 3, None), 3)
    with pytest.raises(TypeError) as exc:
        feeder.put(1)
    assert '"with" scope' in str(exc.value)

    # Multple threaded consumers, and also feeding after exiting context
    produced_even = []
    consumed_even = []
    produced_odd = []
    consumed_odd = []
    with ChunkingFeeder(
            partial(sync_consumer, consumed_even, 3, 0.005),
            3, workers_num=2) as feeder_even, \
         ChunkingFeeder(
            partial(sync_consumer, consumed_odd, 3, 0.006),
            3, workers_num=2) as feeder_odd:
        for val in range(1000):
            if val % 2 == 0:
                produced_even.append(val)
                feeder_even.put(val)
            else:
                produced_odd.append(val)
                feeder_odd.put(val)
    produced_even.append(10000)
    feeder_even.put(10000)
    produced_odd.append(10001)
    feeder_odd.put(10001)
    assert sorted(produced_even) == sorted(consumed_even)
    assert sorted(produced_odd) == sorted(consumed_odd)

    # Muliprocessed consumers, and also feeding after exiting context
    produced = []
    consumed = []
    mp_manager = multiprocessing.Manager()
    consumed_q = mp_manager.Queue()

    with ChunkingFeeder(
            partial(sync_consumer, {'queue': consumed_q}, 3, 0.002),
            3, workers_num=2, multiprocessing=True) as feeder:
        for val in range(200):
            produced.append(val)
            feeder.put(val)
    produced.append(10000)
    feeder.put(10000)
    while True:
        try:
            val = consumed_q.get_nowait()
            consumed.append(val)
        except EmptyQException:
            break
    assert sorted(produced) == sorted(consumed)

    # Test recursive feeding
    recursive_feeding_tester().sync_do_test()


def test_asyncchunkingfeeder():
    do_test_ceck_params_feeders(AsyncChunkingFeeder, async_consumer, sync_consumer)

    async def do_it_async():
        # Simple case with one consumer
        produced = []
        consumed = []
        async with AsyncChunkingFeeder(
                partial(async_consumer, consumed, 3, None),
                3) as feeder:
            for val in range(10):
                produced.append(val)
                await feeder.aput(val)
        assert sorted(produced) == sorted(consumed)

        # Feeding before entering context
        feeder = AsyncChunkingFeeder(partial(async_consumer, [], 3, None), 3)
        with pytest.raises(TypeError) as exc:
            await feeder.aput(1)
        assert '"with" scope' in str(exc.value)

    asyncio.run(do_it_async())

    # Test recursive feeding
    rfeeder = recursive_feeding_tester()
    asyncio.run(rfeeder.async_do_test())
