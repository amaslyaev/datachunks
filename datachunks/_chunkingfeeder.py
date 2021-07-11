"""
Provides something like a "chunks" function, but works in opposite direction: instead of pulling
chunks we push values to be chunked, and this thing feeds a specified callback function with chunks
of specified size. This approach gives a possibility to build much more interesting data processing
schemas than linear pipelines that are only possible using "chunks" function.
"""

import asyncio
from threading import Thread, Lock
from concurrent import futures
from queue import Queue as TQueue, Empty as EmptyQException
from multiprocessing import Queue as MPQueue
from platform import python_version


class ChunkingFeederBase:
    """
    Base class for ChunkingFeeder and AsyncChunkingFeeder
    """
    def __init__(self, callback, chunk_size: int, *, workers_num: int = 0,
                 max_out_q_size_per_worker: int = 10):
        """
        :param callback: function with one positional parameter that will recieve lists of entries.
            For asynchronous version this callback function must be "async".
        :param chunk_size: desired size of chunks.
        :param workers_num: number of workers.
        :param max_out_q_size_per_worker: length of chunks buffer.
        """
        if not callable(callback):
            raise ValueError(
                'The callable object was expected in the "callback" parameter '
                f'({type(callback).__name__} given)')
        if not isinstance(chunk_size, (int, float)):
            raise ValueError(
                f'Expected number in the "chunk_size" parameter ({type(callback).__name__} given)')
        if not isinstance(workers_num, int) or workers_num < 0:
            raise ValueError(
                'Expected non-negative integer in the "workers_num" parameter '
                f'({str(workers_num)[:20]} given)')
        self.curr_chunk = []
        self.callback = callback
        self.chunk_size = max(1, chunk_size)
        self.started = False
        self.finished = False
        self._lock = Lock()
        self._callback_lock_flag = False
        self._workers_num = workers_num
        self._max_out_q_size_per_worker = max_out_q_size_per_worker


class ChunkingFeeder(ChunkingFeederBase):
    """
    Synchronous, and also multithreaded/multiprocessed version of chunking feeder.
    """
    def __init__(self, callback, chunk_size: int, *, workers_num: int = 0,
                 max_out_q_size_per_worker: int = 10, multiprocessing: bool = False):
        """
        :param multiprocessing: set to True to use multiprocessing instead of multithreading.
        """
        if asyncio.iscoroutinefunction(callback):
            raise ValueError(
                'Async function was not expected in the "callback" parameter. '
                'Use AsyncChunkingFeeder instead')
        super().__init__(
            callback, chunk_size, workers_num=workers_num,
            max_out_q_size_per_worker=max_out_q_size_per_worker)
        self._multiprocessing = multiprocessing
        self._out_q = None
        self._feeder_thread = None

    def __enter__(self):
        if self._workers_num > 0:
            if self._multiprocessing:
                self._out_q = MPQueue(self._workers_num * (self._max_out_q_size_per_worker or 0))
            else:
                self._out_q = TQueue(self._workers_num * (self._max_out_q_size_per_worker or 0))
            self._feeder_thread = Thread(target=self._thread_feeder)
            self._feeder_thread.start()
        self._exc = None
        self.started = True
        return self

    def _thread_feeder(self):
        if self._multiprocessing:
            pool_class = futures.ProcessPoolExecutor
        else:
            pool_class = futures.ThreadPoolExecutor
        with pool_class(max_workers=self._workers_num) as workers_pool:
            active_futures = set()
            panic = False
            while True:
                if self.finished:
                    try:
                        chunk = self._out_q.get_nowait()
                    except EmptyQException:
                        if not active_futures:
                            return
                        chunk = None
                else:
                    chunk = self._out_q.get()
                if panic:
                    active_futures = set()
                    continue
                if (active_futures
                        and (len(active_futures) >= self._workers_num
                             or (self.finished and chunk is None))):
                    complete_futures, active_futures = futures.wait(
                        active_futures, return_when=futures.FIRST_COMPLETED)
                else:
                    complete_futures = set()
                for future in list(active_futures):
                    if future.done():
                        complete_futures.add(future)
                        active_futures.remove(future)
                # Get results from complete futures to re-raise exceptions if any
                for complete_future in complete_futures:
                    try:
                        _ = complete_future.result()
                    except Exception as exc:
                        with self._lock:
                            self._exc = exc
                        panic = True
                        continue
                if chunk is not None:
                    active_futures.add(workers_pool.submit(self.callback, chunk))

    def _apply_curr_chunk(self, finalize: bool):
        while True:
            with self._lock:
                if self._exc is not None:
                    exc = self._exc
                    self._exc = None
                    raise exc
            chunk_to_process = None
            with self._lock:
                if (self.curr_chunk and not self._callback_lock_flag
                        and (finalize or self.finished
                             or len(self.curr_chunk) >= self.chunk_size)):
                    self._callback_lock_flag = True
                    chunk_to_process, self.curr_chunk = \
                        self.curr_chunk[:self.chunk_size], self.curr_chunk[self.chunk_size:]
                else:
                    break
            if self._workers_num == 0 or self.finished:
                self.callback(chunk_to_process)
            else:
                self._out_q.put(chunk_to_process)
            with self._lock:
                self._callback_lock_flag = False

    def put(self, value):
        """
        :param value: - a value to put to the chunk that will be passed to a chunks consumer
        """
        if not self.started:
            raise TypeError('Use ChunkingFeeder object only inside a "with" scope')
        with self._lock:
            self.curr_chunk.append(value)
        self._apply_curr_chunk(False)

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self._apply_curr_chunk(True)
        self.finished = True
        if self._workers_num > 0:
            self._out_q.put(None)
            self._feeder_thread.join()
            with self._lock:
                if self._exc is not None:
                    raise self._exc


class AsyncChunkingFeeder(ChunkingFeederBase):
    """
    Asynchronous version of chunking feeder.
    """
    def __init__(self, callback, chunk_size: int, *, workers_num: int = 1):
        if not asyncio.iscoroutinefunction(callback) and not python_version().startswith('3.7.'):
            raise ValueError(
                'Async function was expected in the "callback" parameter')
        super().__init__(callback, chunk_size, workers_num=workers_num)
        self._awaiting = set()

    async def __aenter__(self):
        self.started = True
        return self

    async def _apply_curr_chunk(self, finalize: bool):
        while True:
            chunk_to_process = None
            with self._lock:
                if (self.curr_chunk and not self._callback_lock_flag
                        and (finalize or self.finished
                             or len(self.curr_chunk) >= self.chunk_size)):
                    self._callback_lock_flag = True
                    chunk_to_process, self.curr_chunk = \
                        self.curr_chunk[:self.chunk_size], self.curr_chunk[self.chunk_size:]
                else:
                    if not finalize or not self._awaiting:
                        break
            if self._workers_num == 0 or self.finished:
                if chunk_to_process:
                    await self.callback(chunk_to_process)
            else:
                if self._awaiting and (len(self._awaiting) >= self._workers_num or finalize):
                    complete_futures, self._awaiting = await asyncio.wait(
                        self._awaiting, return_when=asyncio.FIRST_COMPLETED)
                    # Get results from complete futures to re-raise exceptions if any
                    for complete_future in complete_futures:
                        _ = complete_future.result()
                if chunk_to_process:
                    self._awaiting.add(asyncio.get_running_loop().create_task(
                        self.callback(chunk_to_process)))
            with self._lock:
                self._callback_lock_flag = False

    async def aput(self, value):
        """
        :param value: - a value to put to the chunk that will be passed to a chunks consumer
        """
        if not self.started:
            raise TypeError('Use ChunkingFeeder object only inside a "with" scope')
        with self._lock:
            self.curr_chunk.append(value)
        await self._apply_curr_chunk(False)

    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            await self._apply_curr_chunk(True)
        self.finished = True
