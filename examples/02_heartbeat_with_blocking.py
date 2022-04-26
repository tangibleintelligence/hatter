"""
Simple example showing concurrent processing. Note that since python has a GIL, this will still be limited to a single CPU core.
"""
import logging
import logging.config


import time

from hatter.domain import HatterMessage


import asyncio

from hatter.hatter import Hatter

hatter = Hatter("localhost", "backend", "backend", "local", heartbeat=3)

hatter.register_serde(str, lambda x: x.encode("UTF-8"), lambda x: x.decode("UTF-8"))


@hatter.listen(queue_name="blocking_queue")
async def do_something_concurrently(msg: str):
    print(f"message came in: {msg}")
    print("sleeping")
    await asyncio.sleep(15)
    print("end sleep, returning")

LOGGING_APP_LEVEL = 'DEBUG'

if __name__ == "__main__":
    LOGGING: dict = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            },
        },
        'handlers': {
            'stderr': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
            },
        },
        'root': {
            'handlers': ['stderr'],
            'level': LOGGING_APP_LEVEL,
        },
        'loggers': {
            'application': {
                'handlers': ['stderr'],
                'level': LOGGING_APP_LEVEL,
                'propagate': True,
            },
            'aiohttp': {
                'handlers': ['stderr'],
                'level': LOGGING_APP_LEVEL,
                'propagate': True,
            },
            'aio_pika': {
                'handlers': ['stderr'],
                'level': LOGGING_APP_LEVEL,
                'propagate': True,
            },
            'aiormq': {
                'handlers': ['stderr'],
                'level': LOGGING_APP_LEVEL,
                'propagate': True,
            },
        },
    }
    logging.config.dictConfig(LOGGING)

    loop = asyncio.get_event_loop()

    async def send_message_delayed():
        await asyncio.sleep(6)
        await hatter.publish(HatterMessage(data="hi there", destination_queue="blocking_queue"))

    tasks = asyncio.gather(send_message_delayed(), hatter.run())

    try:
        loop.run_until_complete(tasks)
    except KeyboardInterrupt:
        print("Shutting down")
