"""
Helpful stateless utility methods
"""
import asyncio
import concurrent.futures
import inspect
import re
import threading
import warnings
from functools import wraps
from typing import Set, Optional, Tuple, TypeVar, Union, Callable, Coroutine

from aio_pika import Channel, Exchange, Queue, ExchangeType
from aiormq import ChannelPreconditionFailed

from clearcut import get_logger
from hatter.domain import MAX_MESSAGE_PRIORITY

logger = get_logger(__name__)


def get_substitution_names(a_str: str) -> Set[str]:
    """Finds all strings wrapped in {braces} which we expect should/could be substituted in an f-string/`format` call."""
    return set(re.findall("{(.*?)}", a_str))


async def create_exchange(exchange_name: str, consume_channel: Channel) -> Exchange:
    """Create (if needed) and exchange based on our pre-determined pattern."""
    return await consume_channel.declare_exchange(exchange_name, type=ExchangeType.FANOUT)


async def create_exchange_queue(
    exchange_name: Optional[str], queue_name: Optional[str], consume_channel: Channel
) -> Tuple[Optional[Exchange], Queue]:
    """Create (if needed) an exchange and/or queue based on our pre-determined pattern."""
    exchange: Optional[Exchange]
    queue: Queue

    if exchange_name is not None:
        # Named exchanges are always fanout TODO or headers
        exchange = await create_exchange(exchange_name, consume_channel)
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
        # Anonymous queues are transient/temporary. Autodelete+ttl them.
        queue = await consume_channel.declare_queue(
            None, durable=False, exclusive=False, auto_delete=True, arguments={"x-expires": 1000 * 60}
        )

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


T = TypeVar("T")

thread_it_storage: threading.local = threading.local()

try:
    _main_thread_event_loop = asyncio.get_event_loop()
except RuntimeError:
    warnings.warn("Unable to identify a main thread event loop. thread_it and main_loop behavior may be unexpected.")
    _main_thread_event_loop = None


def thread_it(
    coro: Union[Coroutine[None, None, T], Callable[..., Coroutine[None, None, T]]]
) -> Union[asyncio.Task[T], Callable[..., Coroutine[None, None, T]]]:
    """
    Spins up a secondary loop in another thread, runs the coroutine there, and returns an
    awaitable which can be waited on in the calling thread.

    Basically can be used in place of `asyncio.create_task` except that passed coroutine will be run in a separate thread, for coroutines
    which contain non-trivial amounts of blocking code.

    Can also be used as a decorator on an async def function. In this case, instead of returning a task, returns a coroutine function
    (async def, basically).
    """

    if inspect.iscoroutine(coro):

        def _run_coro_in_new_loop():
            # Init a new loop if necessary
            if not getattr(thread_it_storage, "loop_created", False):
                _loop = None
                _loop = asyncio.new_event_loop()
                logger.debug(f"Setting event loop {_loop} / {id(_loop)} on thread {threading.current_thread().name}")
                asyncio.set_event_loop(_loop)
                thread_it_storage.loop_created = True
            else:
                _loop = asyncio.get_event_loop()
            return _loop.run_until_complete(coro)

        # Schedule it and return a task
        return asyncio.create_task(asyncio.to_thread(_run_coro_in_new_loop))
    elif inspect.iscoroutinefunction(coro):

        @wraps(coro)
        async def wrapper(*args, **kwargs):
            # Still using thread_it, just recursive call. but now we're passing in an actual coroutine so it'll use the previous if branch
            # and actually start (when this wrapper is called, that is)
            return await thread_it(coro(*args, **kwargs))

        # handle the decoration
        return wrapper
    else:
        raise ValueError(f"Coroutine or coroutine function expected, not {type(coro)}")


def main_loop(
    coro: Union[Coroutine[None, None, T], Callable[..., Coroutine[None, None, T]]]
) -> Union[asyncio.Task[T], Callable[..., Coroutine[None, None, T]]]:
    """
    Runs coroutine in the main thread's event loop, regardless of current thread/loop. Returns a task/coroutine which can await in the
    calling thread/loop.

    Can also be used as a decorator on an async def coroutine function.
    """

    if inspect.iscoroutine(coro):

        async def _run_coro_in_main_loop():
            if _main_thread_event_loop is None:
                warnings.warn("Using current loop, may not be main thread event loop.")
                loop = asyncio.get_event_loop()
            else:
                loop = _main_thread_event_loop
            fut: concurrent.futures.Future = asyncio.run_coroutine_threadsafe(coro, loop)

            # Convert the concurrent.futures.Future into an asyncio.Future (why this isn't already the response I have no idea)
            asyncio_fut: asyncio.Future = asyncio.wrap_future(fut)
            fut_result = await asyncio_fut
            return fut_result

        # Schedule it and return a task
        return asyncio.create_task(_run_coro_in_main_loop())
    elif inspect.iscoroutinefunction(coro):

        @wraps(coro)
        async def wrapper(*args, **kwargs):
            # Still using main_loop, just recursive call. but now we're passing in an actual coroutine so it'll use the previous if branch
            # and actually start (when this wrapper is called, that is)
            return await main_loop(coro(*args, **kwargs))

        # handle the decoration
        return wrapper
    else:
        raise ValueError(f"Coroutine or coroutine function expected, not {type(coro)}")
