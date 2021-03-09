"""
Helpful stateless utility methods
"""
import re
import warnings
from typing import Set, Optional, Tuple

from aio_pika import Channel, Exchange, Queue, ExchangeType


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
        queue = await consume_channel.declare_queue(queue_name, durable=True, exclusive=False, auto_delete=False)
    else:
        # Anonymous queues are transient/temporary
        queue = await consume_channel.declare_queue(None, durable=False, exclusive=True, auto_delete=True)

    # If only a queue was passed, we'll use the automatic binding to Rabbit's default '' exchange
    # If an exchange was passed, we need to bind the queue (whether it's temporary or shared) to the given exchange
    if exchange_name is not None:
        await queue.bind(exchange)  # TODO include headers functionality

    return exchange, queue


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
