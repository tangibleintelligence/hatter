"""
Defines main object which is used to decorate methods
"""
import asyncio
import inspect
import uuid
from logging import getLogger
from typing import Callable, List, Optional, Set, Dict, NewType, Type, Any, Union

from aio_pika import IncomingMessage, Channel, Message, Queue

from hatter.amqp import AMQPManager
from hatter.domain import DecoratedCoroOrGen, RegisteredCoroOrGen, HatterMessage
from hatter.util import get_substitution_names, create_exchange_queue

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
        # TODO also maybe some default ones? like str <-> bytes, json for pydantic, etc.?
        self._serializers: Dict[Type[T], Callable[[T], bytes]] = dict()
        self._deserializers: Dict[Type[T], Callable[[bytes], T]] = dict()

        # Will get replaced/modified at runtime
        self._run_kwargs = dict()
        self._tasks: List[asyncio.Task] = list()

    def register_serde(self, typ: Type[T], serializer: Callable[[T], bytes], deserializer: Callable[[bytes], T]):
        self._serializers[typ] = serializer
        self._deserializers[typ] = deserializer

    def listen(
        self, *, queue_name: Optional[str] = None, exchange_name: Optional[str] = None
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
            if inspect.iscoroutinefunction(coro_or_gen) or inspect.isasyncgenfunction(coro_or_gen):
                self._register_listener(coro_or_gen, queue_name, exchange_name)
                return coro_or_gen
            else:
                raise ValueError(
                    f"Cannot decorate `{coro_or_gen.__name__}` (type `{type(coro_or_gen).__name__}`). Must be coroutine (`async def`) or "
                    f"async-generator (`async def` that `yield`s)."
                )

        return decorator

    def _register_listener(self, coro_or_gen: DecoratedCoroOrGen, queue_name: Optional[str], exchange_name: Optional[str]):
        """
        Adds function to registry
        """
        self._registry.append(RegisteredCoroOrGen(coro_or_gen=coro_or_gen, queue_name=queue_name, exchange_name=exchange_name))

    def __call__(self, **kwargs):
        """
        Registers the given kwargs as runtime kwargs, to be substituted when listening.

        Anticipated to be used in conjunction with a  `with` block, listening on the registered coroutines, using the given runtime kwargs.
        Usage example:

            async with hatter(kwarg1="val1"):
                # `with` is not blocking...anything can be done here
        """
        # TODO also get params from env variables, not just kwargs
        self._run_kwargs = kwargs
        return self

    async def __aenter__(self):
        """
        Connects to RabbitMQ and starts listening on registered queues/coroutines. Does not block. If blocking method is desired, either
        perform a subsequent blocking call within the `async with` block, or use the shorthand `.run(**kwargs)` method.
        """
        await self._amqp_manager.__aenter__()
        # TODO experiment with one channel per consumption
        consume_channel = await self._amqp_manager.new_channel()

        for registered_obj in self._registry:
            self._tasks.append(asyncio.create_task(self.consume_coro_or_gen(registered_obj, consume_channel, self._run_kwargs)))

        # TODO need a way to monitor for any of these tasks failing and either restart them or abort everything

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for task in self._tasks:
            task.cancel()
        await self._amqp_manager.__aexit__(exc_type, exc_val, exc_tb)

    async def wait(self):
        """
        Can be used to wait on all running listeners. Will return when any waiting listener has an error
        """
        completed_tasks, _ = await asyncio.wait(self._tasks, return_when=asyncio.FIRST_EXCEPTION)

        # TODO need to check completed_tasks in case one threw an error

    # TODO also need __aenter__ and __aexit__ paradigms if something like FastAPI will manage lifecycle?
    async def run(self, **kwargs):
        """
        Shorthand coroutine which connects, starts listening on all registered coroutines, and blocks while listening. This can be used as
        the "entrypoint" to a hatter-backed microservice.
        """
        async with self(**kwargs):
            logger.info("Hatter connected and listening...")
            await self.wait()

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
        async for message in queue:
            message: IncomingMessage
            # Build kwargs from fixed...
            callable_kwargs = dict()
            callable_kwargs.update(fixed_callable_kwargs)

            # ... and dynamic (i.e. based on the message)
            for kwarg, kwarg_function in dynamic_callable_kwargs.items():
                callable_kwargs[kwarg] = kwarg_function(message)

            # We call the registered coroutine/generator with these built kwargs
            try:
                if inspect.iscoroutinefunction(registered_obj.coro_or_gen):
                    # it returns
                    return_val = await registered_obj.coro_or_gen(**callable_kwargs)
                    if return_val is not None:
                        if isinstance(return_val, HatterMessage):
                            logger.debug(f"Publishing {return_val}")
                            await self.publish(return_val)
                        else:
                            raise ValueError(f"Only HatterMessage objects may be returned, not {type(return_val)}")
                else:
                    # it yields
                    async for v in registered_obj.coro_or_gen(**callable_kwargs):
                        if v is None:
                            continue
                        if isinstance(v, HatterMessage):
                            logger.debug(f"Publishing {v}")
                            await self.publish(v)
                        else:
                            raise ValueError(f"Only HatterMessage objects may be yielded, not {type(v)}")
                # At this point, we're processed the message and sent along new ones successfully. We can ack the original message
                # TODO consider doing all of this transactionally
                message.ack()
            except asyncio.CancelledError:
                # We were told to cancel. nack with requeue so someone else will pick up the work
                message.nack(requeue=True)
                logger.info("Cancellation requested.")
                raise
            except Exception:
                # TODO impl more properly. Consider exception type when deciding to requeue. Send to exception handling exchange
                message.nack(requeue=False)
                logger.exception("Exception on message")

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

    async def publish(self, msg: HatterMessage, correlation_id_needed: bool = False) -> str:
        """
        Publishes the given message. Can be used for ad-hoc publishing of messages in a `@hatter.listen` decorated method, or by a
        publish-only application (such as a client in an RPC application). If publishing a message at the end of a listening method, you
        can just return or yield the `HatterMessage` object and it will be published automatically.

        If desired, returns a generated correlation ID which can be used when tracking responses (e.g. in an RPC application)

        All optional keyword arguments are defined in the same way as RabbitMQ.
        """
        if correlation_id_needed:
            correlation_id = str(uuid.uuid4())
        else:
            correlation_id = None
        await self._publish_hatter_message(msg, self._amqp_manager.publish_channel, correlation_id)
        return correlation_id

    async def create_temporary_queue(self) -> Queue:
        """
        Creates a temporary (transient) queue. Useful for, for example, RPC callbacks
        """
        _, queue = await create_exchange_queue(None, None, await self._amqp_manager.new_channel())
        return queue

    async def _publish_hatter_message(self, msg: HatterMessage, channel: Channel, correlation_id: str):
        """Intelligently publishes the given message on the given channel"""
        # try to serialize message body
        serializer = self._serializers.get(type(msg.data), None)
        if serializer is None:
            raise ValueError(f"No registered serializer compatible with {type(msg.data)}")

        amqp_message = Message(body=serializer(msg.data), reply_to=msg.reply_to_queue, correlation_id=correlation_id)
        # TODO other fields like ttl

        # Exchange or queue based?
        if msg.destination_exchange is not None:
            # Exchange based it is
            exchange = await channel.get_exchange(msg.destination_exchange)
            routing_key = msg.routing_key or ""
            await exchange.publish(amqp_message, routing_key=routing_key, mandatory=True)  # TODO might need to disable mandatory sometimes?
        else:
            # Queue based
            await channel.default_exchange.publish(amqp_message, routing_key=msg.destination_queue, mandatory=True)
