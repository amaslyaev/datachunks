"""
Data chunking for humans.
See README.md for details.
"""

__version__ = '0.0.4'
from ._chunks import chunks, achunks  # noqa: F401
from ._chunkingfeeder import ChunkingFeeder, AsyncChunkingFeeder  # noqa: F401
