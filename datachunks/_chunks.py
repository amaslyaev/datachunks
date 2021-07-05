"""
Synchronous (chunks) and asynchronous (achunks) generators to split iterables by chunks
of given size.
"""

from collections.abc import AsyncIterable


def chunks(iterable, chunk_size):
    """
    Splits given iterable by chunks of given size. Very usefull when we need to split read
    or write operations to butches of reasonable size.
    :param iterable: something interable
    :param chunk_size: desirable size if chunks to be produced
    :yield: lists of elements extracted from iterable
    """
    if isinstance(iterable, AsyncIterable):
        raise ValueError(
            'First parameter is async. iterable. Use `achunks` function instead of `chunks`')
    curr_chunk = []
    for val in iterable:
        if curr_chunk and len(curr_chunk) >= chunk_size:
            yield curr_chunk
            curr_chunk = []
        curr_chunk.append(val)
    if curr_chunk:
        yield curr_chunk


async def achunks(aiterable, chunk_size):
    """
    Asynchronous version of "chunks" function.
    Splits iterable stream by chunks of size chunk_size.
    :param aiterable: something asynchronously interable (using "async for")
    :param chunk_size: desirable size if chunks to be produced
    :yield: lists of elements extracted from iterable
    """
    if not isinstance(aiterable, AsyncIterable):
        raise ValueError(
            'First parameter is not async. iterable. Use `chunks` function instead of `achunks`')
    curr_chunk = []
    async for val in aiterable:
        if curr_chunk and len(curr_chunk) >= chunk_size:
            yield curr_chunk
            curr_chunk = []
        curr_chunk.append(val)
    if curr_chunk:
        yield curr_chunk
