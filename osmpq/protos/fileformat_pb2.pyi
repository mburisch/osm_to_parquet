from typing import ClassVar as _ClassVar
from typing import Optional as _Optional

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message

DESCRIPTOR: _descriptor.FileDescriptor

class Blob(_message.Message):
    __slots__ = ()
    RAW_SIZE_FIELD_NUMBER: _ClassVar[int]
    RAW_FIELD_NUMBER: _ClassVar[int]
    ZLIB_DATA_FIELD_NUMBER: _ClassVar[int]
    LZMA_DATA_FIELD_NUMBER: _ClassVar[int]
    OBSOLETE_BZIP2_DATA_FIELD_NUMBER: _ClassVar[int]
    LZ4_DATA_FIELD_NUMBER: _ClassVar[int]
    ZSTD_DATA_FIELD_NUMBER: _ClassVar[int]
    raw_size: int
    raw: bytes
    zlib_data: bytes
    lzma_data: bytes
    OBSOLETE_bzip2_data: bytes
    lz4_data: bytes
    zstd_data: bytes
    def __init__(
        self,
        raw_size: _Optional[int] = ...,
        raw: _Optional[bytes] = ...,
        zlib_data: _Optional[bytes] = ...,
        lzma_data: _Optional[bytes] = ...,
        OBSOLETE_bzip2_data: _Optional[bytes] = ...,
        lz4_data: _Optional[bytes] = ...,
        zstd_data: _Optional[bytes] = ...,
    ) -> None: ...

class BlobHeader(_message.Message):
    __slots__ = ()
    TYPE_FIELD_NUMBER: _ClassVar[int]
    INDEXDATA_FIELD_NUMBER: _ClassVar[int]
    DATASIZE_FIELD_NUMBER: _ClassVar[int]
    type: str
    indexdata: bytes
    datasize: int
    def __init__(
        self,
        type: _Optional[str] = ...,
        indexdata: _Optional[bytes] = ...,
        datasize: _Optional[int] = ...,
    ) -> None: ...
