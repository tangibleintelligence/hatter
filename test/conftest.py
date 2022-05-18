import asyncio
import random
import string
from enum import Enum
from typing import Optional, Dict, Any

import pytest
from pydantic import BaseSettings, Field

from hatter.domain import HatterMessage
from hatter.hatter import Hatter


class RMQSettings(BaseSettings):
    class Config:
        env_prefix = "HATTER_TEST_"

    host: str
    port: Optional[int] = Field(None, ge=1, lt=2 ** 16)
    user: str
    password: str
    vhost: Optional[str] = ""


class RMQAggregator:
    """
    Starts listening on a few internal functions using hatter.listen, and stores the messages into queue which can be popped
    later on. Allows for e2e testing, including a RMQ cluster.
    """

    _settings = RMQSettings()

    class Destination(Enum):
        sample_queue = "rmq_agg_sample_queue_{nonce}"
        sample_exchange = "rmq_agg_sample_exchange_{nonce}"

    def __init__(self) -> None:
        self._queues: Dict[RMQAggregator.Destination, asyncio.Queue] = {x: asyncio.Queue() for x in RMQAggregator.Destination}
        self.hatter = Hatter(self._settings.host, self._settings.user, self._settings.password, self._settings.vhost, self._settings.port)
        self.nonce = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
        self._wait_task: Optional[asyncio.Task] = None
        # TODO register any custom serdes

    async def __aenter__(self) -> Hatter:
        # Define decorated functions
        # Sample fanout exchange
        @self.hatter.listen(exchange_name=RMQAggregator.Destination.sample_exchange.value)
        async def ex_listen(msg: Any):
            await self._queues[RMQAggregator.Destination.sample_exchange].put(msg)

        # Sample named queue
        @self.hatter.listen(queue_name=RMQAggregator.Destination.sample_queue.value)
        async def queue_listen(msg: Any):
            await self._queues[RMQAggregator.Destination.sample_queue].put(msg)

        # Connect to RMQ and start listening
        # Equivalent to opening of `async with self.hatter(nonce=self.nonce):`
        await self.hatter(nonce=self.nonce).__aenter__()

        # Start a background listening asyncio task
        self._wait_task = asyncio.create_task(self.hatter.wait())

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Cancel wait task
        if self._wait_task is not None:
            self._wait_task.cancel()
        # Exit hatter
        await self.hatter.__aexit__(exc_type, exc_val, exc_tb)

    async def get(self, destination: Destination):
        return await asyncio.wait_for(self._queues[destination].get(), 10)

    async def publish(self, msg: HatterMessage):
        await self.hatter.publish(msg)


@pytest.fixture(scope="function")
async def rmq_aggregator():
    agg = RMQAggregator()
    async with agg:
        yield agg

    # Delete the temporary queues made. This requires another RMQ connection but we don't want to start the full listening process
    # noinspection PyProtectedMember
    async with agg.hatter._amqp_manager as mgr:
        mgr.publish_channel.queue_delete(RMQAggregator.Destination.sample_queue.value.format(nonce=agg.nonce))
        mgr.publish_channel.exchange_delete(RMQAggregator.Destination.sample_exchange.value.format(nonce=agg.nonce))
