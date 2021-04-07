"""
Object to manage RabbitMQ connection/channels/threads via aio-pika
"""
from logging import getLogger

import aio_pika
from aio_pika import RobustConnection, Channel

# TODO clearcut logging
logger = getLogger(__name__)


class AMQPManager:
    """
    Creates and manages a connection to interact with a RabbitMQ instance.
    """

    def __init__(
        self, rabbitmq_host: str, rabbitmq_user: str, rabbitmq_pass: str, rabbitmq_virtual_host: str, rabbitmq_port: int, tls: bool
    ):
        self._rabbitmq_host = rabbitmq_host
        self._rabbitmq_port = rabbitmq_port
        self._rabbitmq_user = rabbitmq_user
        self._rabbitmq_pass = rabbitmq_pass
        self._tls = tls
        self._rabbitmq_virtual_host = rabbitmq_virtual_host
        self._connection: RobustConnection = None
        self._publish_channel: Channel = None

    async def __aenter__(self):
        # Create connection based on args passed in init. Channels will be created as needed per queue
        self._connection = await aio_pika.connect_robust(
            host=self._rabbitmq_host,
            port=self._rabbitmq_port,
            login=self._rabbitmq_user,
            password=self._rabbitmq_pass,
            virtualhost=self._rabbitmq_virtual_host,
            ssl=self._tls
        )

        # Create a channel for ad-hoc publishing of messages
        self._publish_channel = await self.new_channel()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._publish_channel is not None:
            await self._publish_channel.close()
        if self._connection is not None:
            await self._connection.close()

    async def new_channel(self, prefetch=1, on_return_raises=True):
        channel = await self._connection.channel(on_return_raises=on_return_raises)
        await channel.set_qos(prefetch_count=prefetch)
        return channel

    @property
    def publish_channel(self):
        return self._publish_channel
