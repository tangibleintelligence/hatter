"""
Defines main object which is used to decorate methods
"""
import asyncio
import inspect
from logging import getLogger
from typing import Callable, AsyncGenerator, List, Optional, Set, Dict, NewType, Type, Any, Union, Coroutine

from aio_pika import IncomingMessage, Channel

from hatter.amqp import AMQPManager
from hatter.domain import DecoratedCoroOrGen, RegisteredCoroOrGen, HatterMessage
from hatter.util import get_substitution_names, publish_hatter_message, create_exchange_queue

logger = getLogger(__name__)

T = NewType("T", object)


class Hatter:
    """
    A `Hatter` instance can be leveraged to decorate coroutines (`async def` "functions") and register them to respond to messages on
    specific queues.

    Planned usage:

        hatter = Hatter(...)

        @hatter.listen('queue_name', ...)
        def fn(msg):
            ...

    """

    def __init__(self, rabbitmq_host: str, rabbitmq_user: str, rabbitmq_pass: str, rabbitmq_virtual_host: str = "/"):

        # Init an AMQPManager. Actual connectivity isn't started until __enter__ via a with block.
        self._amqp_manager: AMQPManager = AMQPManager(rabbitmq_host, rabbitmq_user, rabbitmq_pass, rabbitmq_virtual_host)

        # we need a registry of coroutines and/or async generators (to be added via @hatter.listen(...) decorators). Each coroutine or
        # generator in this registry will be set as a callback for its associated queue when calling `run`
        self._registry: List[RegisteredCoroOrGen] = list()

        # We also need a registry of serializers/deserializers which can be used to convert raw AMQP messages to a known python type, and
        # vice versa
        # noinspection PyPep8Naming
        self._serializers: Dict[Type[T], Callable[[T], bytes]] = dict()
        self._deserializers: Dict[Type[T], Callable[[bytes], T]] = dict()

    def register_serde(self, typ: Type[T], serializer: Callable[[T], bytes], deserializer: Callable[[bytes], T]):
        self._serializers[typ] = serializer
        self._deserializers[typ] = deserializer

    def listen(
        self, queue_name: Optional[str] = None, exchange_name: Optional[str] = None
    ) -> Callable[[Union[DecoratedCoroOrGen]], DecoratedCoroOrGen]:
        """
        Registers decorated coroutine (`async def`) or async generator (`async def` with `yield` instead of return) to run when a message is
        pushed from RabbitMQ on the given queue or exchange.

        It can return a `HatterMessage` object (with an exchange etc specified) and that message will be sent to the message broker. If
        decorating a generator, all yielded messages will be sent sequentially.

        A queue or exchange name can be parameterized by including `{a_var}` within the name string. These will be filled by properties
        specified in hatter.run(). Any of these parameters can also be included as arguments and will be passed in.

        If an exchange name is passed, by default it will be assumed that the exchange is a fanout exchange, and a temporary queue will
        be established to consume from this exchange. If _both_ an exchange and queue are passed, the exchange will still be assumed to
        be a fanout exchange, but the named queue will be used instead of a temporary one.

        TODO If an exchange_name is passed, headers can also be passed to make it a headers exchange.

        TODO maybe there's a cleaner way to do this? ^
        """

        def decorator(coro_or_gen: DecoratedCoroOrGen) -> DecoratedCoroOrGen:
            # Register this coroutine/async-generator for later listening
            if asyncio.iscoroutinefunction(coro_or_gen):
                self._register_listener(coro_or_gen, queue_name, exchange_name)
                return coro_or_gen
            else:
                raise ValueError(
                    f"Cannot decorate `{coro_or_gen.__name__}` (type `{type(coro_or_gen).__name__}`). Must be coroutine (`async def`) or async-generator (`async def` that `yield`s)."
                )

        return decorator

    def _register_listener(self, coro_or_gen: DecoratedCoroOrGen, queue_name: Optional[str], exchange_name: Optional[str]):
        """
        Adds function to registry
        """
        self._registry.append(RegisteredCoroOrGen(coro_or_gen=coro_or_gen, queue_name=queue_name, exchange_name=exchange_name))

    # TODO also need __aenter__ and __aexit__ paradigms if something like FastAPI will manage lifecycle?
    async def run(self, **kwargs):
        """
        Begins listening on all registered queues.

        TODO also get params from env variables, not just kwargs
        """
        async with self._amqp_manager as mgr:
            # TODO experiment with one channel per consumption
            consume_channel = await mgr.new_channel()

            await asyncio.gather(*[self.consume_coro_or_gen(registered_obj, consume_channel, kwargs) for registered_obj in self._registry])

    async def consume_coro_or_gen(self, registered_obj: RegisteredCoroOrGen, consume_channel: Channel, run_kwargs: Dict[str, Any]):
        """
        Sets up prerequisites and begins consuming on the specified queue.
        """

        logger.debug(f"Starting consumption for: {str(registered_obj)}")

        ex_name, q_name, resolved_substitutions = await self.build_exchange_queue_names(registered_obj, run_kwargs)

        # Make sure that all args/kwargs of decorated coro/generator are accounted for. We support:
        # - An unlimited number of kwargs which match one to one with a param in the queue and/or exchange name (will be passed through)
        # - A single kwarg which isn't one of these params, but which we can serialize/deserialize. This is assumed to be the message body
        # - A few specific other kwargs:
        #   - reply_to: str
        #   TODO any more?
        fixed_callable_kwargs: Dict[str, Any] = dict()
        dynamic_callable_kwargs: Dict[str, Callable[[IncomingMessage], Any]] = dict()
        message_arg_name = None  # This is special for the argument that we'll passed the deserialized message into

        parameters = inspect.signature(registered_obj.coro_or_gen).parameters
        for param in parameters.values():
            # We only support keyword params, or position_or_keyword params. This basically means no `/` usage, or *args/**kwargs
            if param.kind not in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY):
                raise ValueError("Only keyword-compatible arguments are supported.")

            # is this a substitution we resolved earlier?
            if param.name in resolved_substitutions:
                fixed_callable_kwargs[param.name] = run_kwargs[param.name]
            # If not, is it a special kwarg?
            elif param.name == "reply_to":
                dynamic_callable_kwargs[param.name] = lambda msg: msg.reply_to
            # If not, is it something we have a serde for?
            elif param.annotation in self._deserializers.keys():
                # We can only do this for one very special argument
                if message_arg_name is None:
                    message_arg_name = param.name
                    dynamic_callable_kwargs[param.name] = lambda msg: self._deserializers[param.annotation](msg.body)
                else:
                    raise ValueError(
                        f"{param.name} cannot be used for the message body; {message_arg_name} has already been allocated to this role."
                    )

        # Create exchange and/or queue
        exchange, queue = await create_exchange_queue(ex_name, q_name, consume_channel)

        # Consume from this queue, forever
        async with queue.iterator() as queue_iter:
            message: IncomingMessage
            async for message in queue_iter:
                # Build kwargs from fixed...
                callable_kwargs = dict()
                callable_kwargs.update(fixed_callable_kwargs)

                # ... and dynamic (i.e. based on the message)
                for kwarg, kwarg_function in dynamic_callable_kwargs.items():
                    callable_kwargs[kwarg] = kwarg_function(message)

                # We call the registered coroutine/generator with these built kwargs
                try:
                    return_val = await registered_obj.coro_or_gen(**callable_kwargs)
                    await self._handle_coro_or_gen_return(return_val)

                    # At this point, we're processed the message and sent along new ones successfully. We can ack the original message
                    # TODO consider doing all of this transactionally
                    message.ack()
                except Exception as e:
                    # TODO impl more properly. Consider exception type when deciding to requeue. Send to exception handling exchange
                    message.nack(requeue=False)
                    raise e

    @staticmethod
    async def build_exchange_queue_names(registered_obj, run_kwargs):
        """
        Substitute any kwargs passed into `run` into the exchange and queue names. Return the final exchange/queue names, as well as
        any substitutions which we made.
        """

        # Exchange and/or queue specified?
        ex_name = registered_obj.exchange_name
        q_name = registered_obj.queue_name

        # Check for substitution params in the queue/exchange name, and make sure they are provided in kwargs
        unresolved_substitutions: Set[str] = set()
        resolved_substitutions: Set[str] = set()
        for name in (ex_name, q_name):
            if name is not None:
                substitutions = get_substitution_names(name)
                for sub in substitutions:
                    if sub not in run_kwargs.keys():
                        unresolved_substitutions.add(sub)
                    else:
                        resolved_substitutions.add(sub)

        if len(unresolved_substitutions) > 0:
            raise ValueError(f"Queue and/or exchange name includes unresolved substitution parameters(s): {unresolved_substitutions}")
        else:
            # Otherwise perform substitutions
            if ex_name is not None:
                ex_name = ex_name.format(**run_kwargs)
            if q_name is not None:
                q_name = q_name.format(**run_kwargs)

        return ex_name, q_name, resolved_substitutions

    async def _handle_coro_or_gen_return(self, return_val):
        """
        Handles the output of a decorated function. Basically, a decorated function can return/yield HatterMessage object(s),
        and we need to route those back to the broker.
        """
        # Since we support decorating generators too (i.e. "functions" that yield instead of return), need to check if this
        # is a generator, and if so exhaust the generator
        if isinstance(return_val, AsyncGenerator):
            async for v in return_val:
                if v is None:
                    continue
                if isinstance(v, HatterMessage):
                    logger.debug(f"Publishing {v}")
                    await publish_hatter_message(v, self._amqp_manager.publish_channel)
                else:
                    raise ValueError(f"Only HatterMessage objects may be yielded, not {type(v)}")
        else:
            if return_val is None:
                return
            if isinstance(return_val, HatterMessage):
                logger.debug(f"Publishing {return_val}")
                await publish_hatter_message(return_val, self._amqp_manager.publish_channel)
            else:
                raise ValueError(f"Only HatterMessage objects may be returned, not {type(return_val)}")
