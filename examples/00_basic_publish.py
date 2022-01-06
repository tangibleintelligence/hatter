"""
Simple example showing hatter publishing.
"""
import asyncio

from hatter.domain import HatterMessage
from hatter.hatter import Hatter

hatter = Hatter("rabbitmq.tangible.ai", "rabbitmq", "rabbitmq-pass", "vh")


hatter.register_serde(str, lambda x: x.encode("UTF-8"), lambda x: x.decode("UTF-8"))


async def publish_sample():
    async with hatter:
        msg = HatterMessage(data="abc", destination_queue="queue_for_ex")
        await hatter.publish(msg)

        msg = HatterMessage(data={"msg": "abc"}, destination_queue="queue_for_ex")
        await hatter.publish(msg)

        msg = HatterMessage(data={"ms2": "abc"}, destination_queue="queue_for_ex")
        await hatter.publish(msg)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(publish_sample())
    except KeyboardInterrupt:
        print("Shutting down")
