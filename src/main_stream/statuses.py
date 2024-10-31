from enum import Enum


class TaskStatus(bytes, Enum):
    Pended = b"PENDED"
    Running = b"RUNNING"
    Error = b"ERROR"
    Completed = b"COMPLETED"
    Cancelled = b"CANCELED"
