"""
Simple example showing hatter usage.
"""
import asyncio

from hatter.domain import HatterMessage
from hatter.hatter import Hatter

hatter = Hatter("localhost", "backend", "backend", "rc")


hatter.register_serde(str, lambda x: x.encode("UTF-8"), lambda x: x.decode("UTF-8"))


@hatter.listen(queue_name="queue_{var1}")
async def do_something1(var1: str, msg: str):
    print("message came in on queue listener1", msg)
    print("var1 was", var1)
    yield HatterMessage(data="new_data", destination_queue="queue_for_ex")
    await asyncio.sleep(3)
    yield HatterMessage(data="new_data", destination_queue="queue_for_ex")


@hatter.listen(exchange_name="exchange_{var1}")
async def do_something2(var1: str, msg: str):
    print("message came in on exchange listener2", msg)
    print("var1 was", var1)


@hatter.listen(queue_name="queue_for_ex", exchange_name="exchange_{var1}")
async def do_something3(var1: str, msg: str):
    print("message came in on exchange listener3", msg)
    print("var1 was", var1)


@hatter.listen(queue_name="queue_multi_arg_{var1}")
async def do_something1(var1: str, b: int, c: int):
    print("var1", var1)
    print("b", b)
    print("c", c)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(hatter.run(var1="var1_value"))
    except KeyboardInterrupt:
        print("Shutting down")
