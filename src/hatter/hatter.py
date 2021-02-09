"""
Defines main object which is used to decorate methods
"""
import inspect
from functools import partial
from logging import getLogger
from typing import Callable, AsyncGenerator, List, Optional, Set, Dict, NewType, Type, Any, Union

from aio_pika import ExchangeType, Exchange, Queue, IncomingMessage

from hatter.amqp import AMQPManager
from hatter.domain import DecoratedCoroOrGen, RegisteredCoroOrGen, HatterMessage
from hatter.util import get_substitution_names, publish_hatter_message

logger = getLogger(__name__)

T = NewType("T", object)


class Hatter:
    """
    A `Hatter` instance can be leveraged to decorate coroutines (`async def` "functions") and register them to respond to messages on specific queues.

    Planned usage:

        hatter = Hatter(...)

        @hatter.listen('queue_name', ...)
        def fn(msg):
            ...

    """

    def __init__(self, rabbitmq_host: str, rabbitmq_user: str, rabbitmq_pass: str, rabbitmq_virtual_host: str = "/"):

        # Init an AMQPManager. Actual connectivity isn't started until __enter__ via a with block.
        self._amqp_manager: AMQPManager = AMQPManager(rabbitmq_host, rabbitmq_user, rabbitmq_pass, rabbitmq_virtual_host)

        # we need a registry of coroutines and/or async generators (to be added via @hatter.listen(...) decorators). Each coroutine or generator in this registry will be set as a callback for its
        # associated queue when calling `run`
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
        Registers decorated coroutine (`async def`) or async generator (`async def` with `yield` instead of return) to run when a message is pushed from RabbitMQ on the given queue or exchange.

        It can return a `HatterMessage` object (with an exchange etc specified) and that message will be sent to the message broker. If decorating a
        generator, all yielded messages will be sent sequentially.

        A queue or exchange name can be parameterized by including `{a_var}` within the name string. These will be filled by properties specified
        in hatter.run(). Any of these parameters can also be included as arguments and will be passed in.

        If an exchange name is passed, by default it will be assumed that the exchange is a fanout exchange, and a temporary queue will be established to
        consume from this exchange. If _both_ an exchange and queue are passed, the exchange will still be assumed to be a fanout exchange,
        but the named queue will be used instead of a temporary one.

        TODO If an exchange_name is passed, headers can also be passed to make it a headers exchange.

        TODO maybe there's a cleaner way to do this? ^
        """

        def decorator(coro_or_gen: DecoratedCoroOrGen) -> DecoratedCoroOrGen:
            # Register this coroutine/async-generator for later listening
            self._register_listener(coro_or_gen, queue_name, exchange_name)
            return coro_or_gen

        return decorator

    def _register_listener(self, coro_or_gen: DecoratedCoroOrGen, queue_name: Optional[str], exchange_name: Optional[str]):
        """
        Adds function to registry
        """
        self._registry.append(RegisteredCoroOrGen(coro_or_gen=coro_or_gen, queue_name=queue_name, exchange_name=exchange_name))

    # TODO also need __enter__ and __exit__ paradigms if something like FastAPI will manage lifecycle?
    async def run(self, **kwargs):
        """
        Begins listening on all registered queues.

        TODO also get params from env variables, not just kwargs
        """
        async with self._amqp_manager as mgr:
            # TODO experiment with one channel per consumption
            consume_channel = await mgr.new_channel()
            for registered_obj in self._registry:
                logger.debug(f"Registering: {str(registered_obj)}")

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
                            if sub not in kwargs.keys():
                                unresolved_substitutions.add(sub)
                            else:
                                resolved_substitutions.add(sub)

                if len(unresolved_substitutions) > 0:
                    raise ValueError(
                        f"Queue and/or exchange name includes unresolved substitution parameters(s): {unresolved_substitutions}"
                    )

                # Otherwise perform substitutions
                if ex_name is not None:
                    ex_name = ex_name.format(**kwargs)

                if q_name is not None:
                    q_name = q_name.format(**kwargs)

                # Make sure that all args/kwargs of decorated coro/generator are accounted for. We support:
                # - An unlimited number of kwargs which match one to one with a param in the queue and/or exchange name (will be passed through)
                # - A single kwarg which isn't one of these params, but which we can serialize/deserialize. This is assumed to be the message body
                # - A few specific other kwargs:
                #   - reply_to: str
                #   TODO any more?

                fixed_callable_kwargs: Dict[str, Any] = dict()
                dynamic_callable_kwargs: Dict[str, Callable[[IncomingMessage], Any]]
                message_arg_name = None

                parameters = inspect.signature(registered_obj.coro_or_gen).parameters
                for param in parameters.values():
                    # We only support keyword params, or position_or_keyword params. This basically means no `/` usage, or *args/**kwargs
                    if param.kind not in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY):
                        raise ValueError("Only keyword-compatible arguments are supported.")

                    # is this a substitution we resolved earlier?
                    if param.name in resolved_substitutions:
                        fixed_callable_kwargs[param.name] = kwargs[param.name]
                    # If not, is it a special kwarg?
                    elif param.name == "reply_to":
                        dynamic_callable_kwargs[param.name] = lambda msg: msg.reply_to
                    # If not, is it something we have a serde for?
                    elif param.annotation in self._deserializers.keys():
                        # We can only do this once
                        if message_arg_name is None:
                            message_arg_name = param.name
                            dynamic_callable_kwargs[param.name] = lambda msg: self._deserializers[param.annotation](msg.body)
                        else:
                            raise ValueError(
                                f"{param.name} cannot be used for the message body; {message_arg_name} has already been allocated to this role."
                            )

                # Create exchange and/or queue
                exchange: Optional[Exchange]
                queue: Optional[Queue]

                if ex_name is not None:
                    # Named exchanges are always fanout TODO or headers
                    exchange = await consume_channel.declare_exchange(ex_name, type=ExchangeType.FANOUT, durable=True)
                else:
                    exchange = None

                if q_name is not None:
                    # Named queues are always durable and presumed to be shared
                    queue = await consume_channel.declare_queue(q_name, durable=True, exclusive=False, auto_delete=False)
                else:
                    # Anonymous queues are transient/temporary
                    queue = await consume_channel.declare_queue(None, durable=False, exclusive=True, auto_delete=True)

                # If only a queue was passed, we'll use the automatic binding to Rabbit's default '' exchange
                # If an exchange was passed, we need to bind the queue (whether it's temporary or shared) to the given exchange
                if ex_name is not None:
                    await queue.bind(exchange)  # TODO include headers functionality

                # Form a closure to register on this queue.
                async def consumption_callable(message: IncomingMessage):
                    # Build kwargs from fixed...
                    callable_kwargs = dict()
                    callable_kwargs.update(fixed_callable_kwargs)

                    # ... and dynamic (i.e. based on the message)
                    for kwarg, kwarg_function in dynamic_callable_kwargs.items():
                        callable_kwargs[kwarg] = kwarg_function(message)

                    # We call the registered coroutine/generator with these built kwargs
                    try:
                        # noinspection PyCallingNonCallable
                        return_val = await registered_obj.coro_or_gen(**callable_kwargs)

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
                            if return_val is not None and isinstance(return_val, HatterMessage):
                                logger.debug(f"Publishing {return_val}")
                                await publish_hatter_message(return_val, self._amqp_manager.publish_channel)
                            else:
                                raise ValueError(f"Only HatterMessage objects may be returned, not {type(return_val)}")

                        # At this point, we're processed the message and sent along new ones successfully. We can ack the original message
                        # TODO consider doing all of this transactionally

                        message.ack()
                    except Exception as e:
                        # TODO impl more properly. Consider exception type when deciding to requeue. Send to exception handling exchange
                        message.nack(requeue=False)
                        raise e

                # Register the above closure
                # TODO impl
