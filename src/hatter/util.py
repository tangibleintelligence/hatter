"""
Helpful stateless utility methods
"""
import re
from typing import Set

from aio_pika import Channel, Message, DeliveryMode

from hatter.domain import HatterMessage


def exchange_name(base: str):
    return f"exchange__{base}"


def queue_name(base: str):
    return f"queue__{base}"


def get_substitution_names(a_str: str) -> Set[str]:
    """Finds all strings wrapped in {braces} which we expect should/could be substituted in an f-string/`format` call."""
    return set(re.findall("{(.*?)}", a_str))


async def publish_hatter_message(msg: HatterMessage, channel: Channel):
    """Intelligently publishes the given message on the given channel"""
    # Exchange or queue based?
    if msg.destination_exchange is not None:
        # Exchange based it is
        exchange = await channel.get_exchange(msg.destination_exchange)
        amqp_message = Message(body=msg.data, reply_to=msg.reply_to_queue)  # TODO other fields like ttl
        routing_key = msg.routing_key or ""
        await exchange.publish(amqp_message, routing_key=routing_key, mandatory=True)  # TODO might need to disable mandatory sometimes?
