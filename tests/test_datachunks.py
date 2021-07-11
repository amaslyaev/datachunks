import asyncio
from time import sleep
from functools import partial
import multiprocessing
from queue import Empty as EmptyQException
from platform import python_version

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


def sync_consumer(is_mp: bool, consumed, chunk_size: int, pause_func, fail_on: int, chunk: list):
    assert isinstance(chunk, list)
    assert 0 < len(chunk) <= chunk_size
    if pause_func:
        if isinstance(pause_func, (int, float)):
            sleep(pause_func)
        elif callable(pause_func):
            sleep(pause_func(chunk))
    if fail_on is not None:
        _ = [1 / (val - fail_on) for val in chunk]
    if is_mp:
        for val in chunk:
            consumed.put(val)
    else:
        consumed += chunk


async def async_consumer(consumed: list, chunk_size: int, pause_func, fail_on: int, chunk: list):
    assert isinstance(chunk, list)
    assert 0 < len(chunk) <= chunk_size
    if pause_func:
        if isinstance(pause_func, (int, float)):
            await asyncio.sleep(pause_func)
        elif callable(pause_func):
            await asyncio.sleep(pause_func(chunk))
    if fail_on is not None:
        _ = [1 / (val - fail_on) for val in chunk]
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

    if not python_version().startswith('3.7.'):  # 3.7 cannot recognize cor.function after partial
        with pytest.raises(ValueError) as exc:
            class_to_test(partial(wrong_consumer, [], 3, None), 3)
        assert '"callback" parameter' in str(exc.value)


def test_chunkingfeeder_params():
    do_test_ceck_params_feeders(ChunkingFeeder, sync_consumer, async_consumer)


def do_sync_test(is_mp: bool, workers_num: int, max_num: int,
                 chunk_size, pause: float, fail_on: int):
    produced = []
    consumed = []
    if is_mp:
        mp_manager = multiprocessing.Manager()
        consumed_q = mp_manager.Queue()
        consumed_param = consumed_q
    else:
        consumed_param = consumed

    with ChunkingFeeder(
            partial(sync_consumer, is_mp, consumed_param, chunk_size, pause, fail_on),
            chunk_size, workers_num=workers_num, multiprocessing=is_mp) as feeder:
        for val in range(max_num):
            produced.append(val)
            feeder.put(val)

    produced.append(10000)
    feeder.put(10000)

    if is_mp:
        while True:
            try:
                val = consumed_q.get_nowait()
                consumed.append(val)
            except EmptyQException:
                break

    assert sorted(produced) == sorted(consumed)


def test_chunkingfeeder_simple_case():
    # Fully synchronous, one consumer, no deliberate fails
    do_sync_test(False, 0, 10, 3, 0.001, None)


def test_chunkingfeeder_out_of_context():
    # Feeding before entering context
    feeder = ChunkingFeeder(partial(sync_consumer, [], 3, None), 3)
    with pytest.raises(TypeError) as exc:
        feeder.put(1)
    assert '"with" scope' in str(exc.value)


def test_chunkingfeeder_threads():
    # 2 threads, one consumer, no deliberate fails
    do_sync_test(False, 2, 20, 3, 0, None)
    do_sync_test(False, 2, 20, 3, 0.001, None)
    do_sync_test(False, 2, 200, 3, 0.001, None)


def test_chunkingfeeder_mp():
    # 2 processes, one consumer, no deliberate fails
    do_sync_test(True, 2, 20, 3, 0, None)
    do_sync_test(True, 2, 20, 3, 0.001, None)
    do_sync_test(True, 2, 200, 3, 0.001, None)


def test_chunkingfeeder_fail_in_thread():
    # 2 threads, one consumer, deliberate fail on val==7
    with pytest.raises(ZeroDivisionError):
        do_sync_test(False, 2, 200, 3, 0.001, 7)
    # On join thread...
    with pytest.raises(ZeroDivisionError):
        do_sync_test(False, 2, 10, 3, 0.001, 7)


def test_chunkingfeeder_fail_in_mp():
    # 2 processes, one consumer, deliberate fail on val==7
    with pytest.raises(ZeroDivisionError):
        do_sync_test(True, 2, 200, 3, 0.001, 7)
    # On join thread...
    with pytest.raises(ZeroDivisionError):
        do_sync_test(True, 2, 10, 3, 0.001, 7)


def test_chunkingfeeder_two_consumers():
    # Multple threaded consumers, and also feeding after exiting context
    produced_even = []
    consumed_even = []
    produced_odd = []
    consumed_odd = []
    with ChunkingFeeder(
            partial(sync_consumer, False, consumed_even, 3, 0.005, None),
            3, workers_num=2) as feeder_even, \
         ChunkingFeeder(
            partial(sync_consumer, False, consumed_odd, 3, 0.006, None),
            3, workers_num=2) as feeder_odd:
        for val in range(1000):
            if val % 2 == 0:
                produced_even.append(val)
                feeder_even.put(val)
            else:
                produced_odd.append(val)
                feeder_odd.put(val)
    assert sorted(produced_even) == sorted(consumed_even)
    assert sorted(produced_odd) == sorted(consumed_odd)


def test_asyncchunkingfeeder_params():
    do_test_ceck_params_feeders(AsyncChunkingFeeder, async_consumer, sync_consumer)


def test_asyncchunkingfeeder_simple_case():
    async def do_it_async():
        # Simple case with one consumer
        produced = []
        consumed = []
        async with AsyncChunkingFeeder(
                partial(async_consumer, consumed, 3, None, None),
                3) as feeder:
            for val in range(10):
                produced.append(val)
                await feeder.aput(val)
        produced.append(42)
        await feeder.aput(42)
        assert sorted(produced) == sorted(consumed)

    asyncio.run(do_it_async())


def test_asyncchunkingfeeder_out_of_context():
    async def do_it_async():
        # Feeding before entering context
        feeder = AsyncChunkingFeeder(partial(async_consumer, [], 3, None, None), 3)
        with pytest.raises(TypeError) as exc:
            await feeder.aput(1)
        assert '"with" scope' in str(exc.value)

    asyncio.run(do_it_async())
