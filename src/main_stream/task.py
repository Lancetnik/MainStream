import typing
from uuid import uuid4
from contextlib import suppress
import asyncio

from nats.aio.msg import Msg
from faststream.nats import JStream

from .statuses import TaskStatus

if typing.TYPE_CHECKING:
    from .broker import TaskBroker

class TaskResult:
    def __init__(
        self,
        *,
        task_id: str,
    ) -> None:
        self.task_id = task_id

        self.status: TaskStatus = TaskStatus.Pended

    async def _start(
        self,
        *,
        broker: "TaskBroker",
        status_bucket_name: str,
    ) -> None:
        self.__kv = await broker.key_value(status_bucket_name)
        self.__watcher = await self.__kv.watch(self.task_id)
        self.__result_future: asyncio.Future[Msg] = asyncio.Future()
        self.__sub = await broker._connection.subscribe(
            f"{self.task_id}.result",
            future=self.__result_future,
            max_msgs=1,
        )

        await self.__kv.put(self.task_id, TaskStatus.Pended.value)

        async def watch_status_changes():
            while (
                self.status is not TaskStatus.Cancelled
                and self.status is not TaskStatus.Completed
            ):
                with suppress(TimeoutError):
                    if data := await self.__watcher.updates(timeout=5.0):
                        self.status = TaskStatus(data.value)

            await self.__watcher.stop()

        self.task = asyncio.create_task(watch_status_changes())

    async def result(self, timeout: float = 5.0) -> bytes:
        try:
            data = (await asyncio.wait_for(self.__result_future, timeout=timeout)).data
        finally:
            await self.__sub.drain()
            await self.__kv.delete(self.task_id)
        return data

    async def cancel(self) -> None:
        self.status = TaskStatus.Cancelled
        self.__result_future.cancel()
        await self.__kv.put(self.task_id, TaskStatus.Cancelled.value)


class Task:
    def __init__(
        self,
        *,
        name: str,
        stream: JStream,
        status_bucket_name: str,
        _broker: "TaskBroker",
    ) -> None:
        self.name = name
        self.stream = stream
        self.status_bucket_name = status_bucket_name

        self.__broker = _broker

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}')"

    async def delay(self, **kwargs: typing.Any) -> TaskResult:
        task_id = uuid4().hex

        task = TaskResult(task_id=task_id)

        await task._start(
            broker=self.__broker,
            status_bucket_name=self.status_bucket_name,
        )

        await self.__broker.publish(
            kwargs,
            subject=self.name,
            stream=self.stream.name,
            headers={
                "reply_to": f"{task_id}.result",
                "Nats-Msg-Id": task_id,
            },
        )

        return task
