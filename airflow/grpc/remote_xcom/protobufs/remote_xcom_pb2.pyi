from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class task_invoke(_message.Message):
    __slots__ = ["args", "annotations", "xcoms"]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    ANNOTATIONS_FIELD_NUMBER: _ClassVar[int]
    XCOMS_FIELD_NUMBER: _ClassVar[int]
    args: _containers.RepeatedScalarFieldContainer[str]
    annotations: bytes
    xcoms: bytes
    def __init__(self, args: _Optional[_Iterable[str]] = ..., annotations: _Optional[bytes] = ..., xcoms: _Optional[bytes] = ...) -> None: ...

class task_reply(_message.Message):
    __slots__ = ["xcoms"]
    XCOMS_FIELD_NUMBER: _ClassVar[int]
    xcoms: bytes
    def __init__(self, xcoms: _Optional[bytes] = ...) -> None: ...
