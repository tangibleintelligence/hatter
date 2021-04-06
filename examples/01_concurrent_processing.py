"""
Simple example showing concurrent processing. Note that since python has a GIL, this will still be limited to a single CPU core.
"""
import asyncio

from hatter.hatter import Hatter

hatter = Hatter("rabbitmq.host", "rabbitmq.user", "rabbitmq.pass", "rabbitmq.vh")

hatter.register_serde(str, lambda x: x.encode("UTF-8"), lambda x: x.decode("UTF-8"))


@hatter.listen(queue_name="concurrent_queue", concurrency=2)  # Allows for 2 simultaneous messages to be handled at once.
async def do_something_concurrently(msg: str):
    print("message came in:", msg)
    print("sleeping")
    await asyncio.sleep(5)
    print("end sleep, returning")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(hatter.run())
    except KeyboardInterrupt:
        print("Shutting down")
