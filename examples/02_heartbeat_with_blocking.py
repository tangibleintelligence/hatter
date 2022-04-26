"""
Simple example showing concurrent processing. Note that since python has a GIL, this will still be limited to a single CPU core.
"""


import asyncio
import time

from hatter.domain import HatterMessage
from hatter.hatter import Hatter

hatter = Hatter("localhost", "backend", "backend", "local", heartbeat=3)

hatter.register_serde(str, lambda x: x.encode("UTF-8"), lambda x: x.decode("UTF-8"))


@hatter.listen(queue_name="blocking_queue", blocks=True)
async def do_something_concurrently(msg: str):
    print(f"message came in: {msg}")
    print("sleeping")
    time.sleep(6)
    print("end sleep, returning")
    return "hello back"


if __name__ == "__main__":

    loop = asyncio.get_event_loop()

    async def send_message_delayed():
        await asyncio.sleep(6)
        await hatter.publish(HatterMessage(data="hi there", destination_queue="blocking_queue"))

    tasks = asyncio.gather(send_message_delayed(), hatter.run())

    try:
        loop.run_until_complete(tasks)
        loop.stop()
        loop.close()
    except KeyboardInterrupt:
        print("Shutting down")
