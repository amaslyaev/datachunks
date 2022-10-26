"""
Synchronous (chunks) and asynchronous (achunks) generators to split iterables by chunks
of given size.
"""

from typing import List, TypeVar, Iterable, AsyncIterable as TAsyncIterable
from collections.abc import AsyncIterable

_T = TypeVar("_T")


def chunks(iterable: Iterable[_T], chunk_size: int) -> Iterable[List[_T]]:
    """
    Splits given iterable by chunks of given size. Very usefull when we need to split
    read or write operations to butches of reasonable size.
    :param iterable: something interable
    :param chunk_size: desirable size if chunks to be produced
    :yield: lists of elements extracted from iterable
    """
    if isinstance(iterable, AsyncIterable):
        raise ValueError(
            "First parameter is async. iterable. Use `achunks` function "
            "instead of `chunks`"
        )
    curr_chunk: List[_T] = []
    for val in iterable:
        if curr_chunk and len(curr_chunk) >= chunk_size:
            yield curr_chunk
            curr_chunk = []
        curr_chunk.append(val)
    if curr_chunk:
        yield curr_chunk


async def achunks(
    aiterable: TAsyncIterable[_T], chunk_size: int
) -> TAsyncIterable[List[_T]]:
    """
    Asynchronous version of "chunks" function.
    Splits iterable stream by chunks of size chunk_size.
    :param aiterable: something asynchronously interable (using "async for")
    :param chunk_size: desirable size if chunks to be produced
    :yield: lists of elements extracted from iterable
    """
    if not isinstance(aiterable, AsyncIterable):
        raise ValueError(
            "First parameter is not async. iterable. Use `chunks` function "
            "instead of `achunks`"
        )
    curr_chunk: List[_T] = []
    async for val in aiterable:
        if curr_chunk and len(curr_chunk) >= chunk_size:
            yield curr_chunk
            curr_chunk = []
        curr_chunk.append(val)
    if curr_chunk:
        yield curr_chunk
