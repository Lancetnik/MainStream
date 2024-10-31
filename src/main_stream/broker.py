import typing
from functools import wraps
from contextlib import suppress
import asyncio

from faststream.nats import NatsBroker, JStream
from faststream._internal.utils.functions import to_async

from .statuses import TaskStatus
from .task import Task
from .exceptions import TaskCanceledError

AnyFunc: typing.TypeAlias = typing.Callable[..., typing.Any]
TaskDecorator: typing.TypeAlias = typing.Callable[[AnyFunc], Task]


class TaskBroker(NatsBroker):
    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        super().__init__(*args, **kwargs)

        self.subject_prefix = "mainstream"
        self.status_bucket_name = f"{self.prefix}-data"
        self._stream = JStream(
            name=f"{self.subject_prefix}-flow",
            subjects=[f"{self.subject_prefix}.*"],
        )

    async def connect(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        client = await super().connect(*args, **kwargs)
        return client

    @typing.overload
    def task(
        self, func: typing.Literal[None] = None, *, name: typing.Optional[str] = None
    ) -> TaskDecorator: ...

    @typing.overload
    def task(self, func: AnyFunc, *, name: typing.Optional[str] = None) -> Task: ...

    def task(
        self,
        func: typing.Optional[AnyFunc] = None,
        *,
        name: typing.Optional[str] = None,
    ) -> typing.Union[TaskDecorator, Task]:
        decorator = self._task(name)

        if func is None:
            return decorator

        return decorator(func)

    def _task(
        self,
        name: typing.Optional[str],
    ) -> TaskDecorator:
        def decorator(func: AnyFunc) -> Task:
            nonlocal name
            if name is None:
                destination_name = (
                    f"{self.subject_prefix}.{func.__name__}-{str(hash(func))[:8]}"
                )
                name = f"{func.__name__}-{str(hash(func))[:8]}"
            else:
                destination_name = name

            self._add_task_handler(
                func=func,
                destination_name=destination_name,
                name=name,
            )

            return Task(
                name=destination_name,
                stream=self._stream,
                status_bucket_name=self.status_bucket_name,
                _broker=self,
            )

        return decorator

    def _add_task_handler(
        self,
        *,
        func: AnyFunc,
        destination_name: str,
        name: str,
    ) -> None:
        async_func = to_async(func)

        @self.subscriber(
            destination_name,
            durable=name,
            stream=self._stream,
        )
        @wraps(func)
        async def _(**kwargs: typing.Any) -> typing.Any:
            task_id: str = self.context.get_local("message").headers["Nats-Msg-Id"]

            kv = await self.key_value(bucket=self.status_bucket_name)

            watcher = await kv.watch(task_id)

            async def wait_cancel_status() -> typing.Literal[TaskStatus.Cancelled]:
                while True:
                    with suppress(TimeoutError):
                        if data := await watcher.updates(timeout=5.0):
                            status = TaskStatus(data.value)
                            if status is TaskStatus.Cancelled:
                                return status

            task_status = TaskStatus((await kv.get(task_id)).value or b"")
            if task_status is TaskStatus.Cancelled:
                await kv.purge(task_id)
                await watcher.stop()
                raise TaskCanceledError

            else:
                await kv.put(task_id, TaskStatus.Running.value)

            try:
                done, in_progerss = await asyncio.wait(
                    (
                        asyncio.create_task(async_func(**kwargs)),
                        asyncio.create_task(wait_cancel_status()),
                    ),
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in in_progerss:
                    if not task.done():
                        task.cancel()

                data = next(iter(done)).result()

            except Exception:
                await kv.put(task_id, TaskStatus.Error.value)
                raise

            else:
                if data is TaskStatus.Cancelled:
                    await kv.purge(task_id)
                    raise TaskCanceledError

                else:
                    await kv.put(task_id, TaskStatus.Completed.value)
                    return data

            finally:
                await watcher.stop()
