"""
Helpful stateless utility methods
"""
import re
import warnings
from typing import Set, Optional, Tuple

from aio_pika import Channel, Exchange, Queue, ExchangeType
from aiormq import ChannelPreconditionFailed

from hatter.domain import MAX_MESSAGE_PRIORITY


def get_substitution_names(a_str: str) -> Set[str]:
    """Finds all strings wrapped in {braces} which we expect should/could be substituted in an f-string/`format` call."""
    return set(re.findall("{(.*?)}", a_str))


async def create_exchange_queue(
    exchange_name: Optional[str], queue_name: Optional[str], consume_channel: Channel
) -> Tuple[Optional[Exchange], Queue]:
    """Create (if needed) an exchange and/or queue based on our pre-determined pattern."""
    exchange: Optional[Exchange]
    queue: Queue

    if exchange_name is not None:
        # Named exchanges are always fanout TODO or headers
        exchange = await consume_channel.declare_exchange(exchange_name, type=ExchangeType.FANOUT, durable=True)
    else:
        exchange = None

    if queue_name is not None:
        # Named queues are always durable and presumed to be shared
        try:
            queue = await _declare_named_queue(consume_channel, queue_name)
        except ChannelPreconditionFailed as e:
            if e.args is not None and len(e.args) > 0 and "x-max-priority" in e.args[0]:
                # For backwards compatibility, if a queue was previously declared without a priority, redeclare it with one.
                warnings.warn(f"Redeclaring queue {queue_name} to support message priority. Any pending messages will be lost.")
                await consume_channel.reopen()  # The ChannelPreconditionFailed error will cause the channel to close
                await consume_channel.queue_delete(queue_name)
                queue = await _declare_named_queue(consume_channel, queue_name)
            else:
                raise e
    else:
        # Anonymous queues are transient/temporary
        queue = await consume_channel.declare_queue(None, durable=False, exclusive=True, auto_delete=True)

    # If only a queue was passed, we'll use the automatic binding to Rabbit's default '' exchange
    # If an exchange was passed, we need to bind the queue (whether it's temporary or shared) to the given exchange
    if exchange_name is not None:
        await queue.bind(exchange)  # TODO include headers functionality

    return exchange, queue


async def _declare_named_queue(consume_channel, queue_name):
    return await consume_channel.declare_queue(
        queue_name, durable=True, exclusive=False, auto_delete=False, arguments={"x-max-priority": MAX_MESSAGE_PRIORITY}
    )


def flexible_is_subclass(cls: type, potential_superclass: type) -> bool:
    """
    Checks if `cls` is a subclass of `potential_superclass`, but compares on __qualname__ if `__main__` is in one of the class names.
    """
    if issubclass(cls, potential_superclass):
        return True

    if (cls.__module__ == "__main__") ^ (potential_superclass.__module__ == "__main__"):
        # Check on qualname
        if cls.__qualname__ == potential_superclass.__qualname__:
            warnings.warn(f"Equating classes {cls} and {potential_superclass} on qualified name only.")
            return True

    return False
