from typing import Dict, Any
from google.protobuf.json_format import MessageToDict


class Annotation(object):
    dag_id: str
    task_id: str
    run_id: str


class Message(object):
    annotation: Annotation = None
    args: Dict[Any, Any] = None
    status_code: str = None
    xcoms: Dict[Any, Any] = None

