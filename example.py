import asyncio
import logging

from mainstream import TaskBroker
from faststream import FastStream

broker = TaskBroker(log_level=logging.DEBUG)

@broker.task
async def func1(user: str, user_id: int) -> str:
    await asyncio.sleep(3.0)
    return f"{user}-{user_id}"

app = FastStream(broker)

@app.after_startup
async def t():
    task = await func1.delay(user="John", user_id=1)
    # await task.cancel()
    # print(task.status)
    result = await task.result(5.0)
    print(result)
