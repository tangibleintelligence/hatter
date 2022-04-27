"""
Simple example showing concurrent processing. Note that since python has a GIL, this will still be limited to a single CPU core.
"""
import logging

import asyncio
import time
from clearcut import get_logger
from hatter.domain import HatterMessage
from hatter.hatter import Hatter

hatter = Hatter("localhost", "backend", "backend", "local", heartbeat=3)

hatter.register_serde(str, lambda x: x.encode("UTF-8"), lambda x: x.decode("UTF-8"))

logger = get_logger('with blocking')

async def wait():
    await asyncio.sleep(1)


@hatter.listen(queue_name="do_something", blocks=True)
async def do_something(msg: str):
    logger.info(f"message came in something: {msg}")
    logger.info("sleeping something")
    time.sleep(6)
    logger.info("end sleep something")
    logger.info("async waiting something")
    await wait()
    logger.info("end async waiting something")
    return "hello back"


@hatter.listen(queue_name="do_something_else", blocks=True)
async def do_something_else(msg: str):
    logger.info(f"message came in something else: {msg}")
    logger.info("sleeping something else")
    time.sleep(6)
    logger.info("end sleep something else")
    logger.info("async waiting something else")
    await wait()
    logger.info("end async waiting something else")
    return "hello back something else"


if __name__ == "__main__":

    loop = asyncio.get_event_loop()

    async def send_messages_delayed():
        await asyncio.sleep(1)
        await hatter.publish(HatterMessage(data="hi there", destination_queue="do_something"))
        await asyncio.sleep(1)
        await hatter.publish(HatterMessage(data="hi there", destination_queue="do_something"))
        await asyncio.sleep(1)
        await hatter.publish(HatterMessage(data="hi there", destination_queue="do_something_else"))
        await asyncio.sleep(1)
        await hatter.publish(HatterMessage(data="hi there", destination_queue="do_something"))
        await asyncio.sleep(1)
        print('now a burst')
        await hatter.publish(HatterMessage(data="hi there", destination_queue="do_something_else"))
        await hatter.publish(HatterMessage(data="hi there", destination_queue="do_something"))
        await hatter.publish(HatterMessage(data="hi there", destination_queue="do_something_else"))
        await hatter.publish(HatterMessage(data="hi there", destination_queue="do_something"))

    tasks = asyncio.gather(send_messages_delayed(), hatter.run())

    try:
        loop.run_until_complete(tasks)
        loop.stop()
        loop.close()
    except KeyboardInterrupt:
        print("Shutting down")
