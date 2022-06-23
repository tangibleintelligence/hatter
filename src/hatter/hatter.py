"""
Defines main object which is used to decorate methods
"""
import asyncio
import inspect
import pickle
import warnings
from aio_pika import IncomingMessage, Channel, Message, Queue
from aiormq import PublishError
from opentelemetry.trace import SpanKind
from typing import Callable, List, Optional, Set, Dict, NewType, Type, Any, Union, Tuple, get_origin, AsyncIterator

from clearcut import context_from_carrier, get_logger_tracer
from clearcut.otlputils import carrier_from_context, add_event
from hatter.amqp import AMQPManager
from hatter.domain import DecoratedCoroOrGen, RegisteredCoroOrGen, HatterMessage
from hatter.util import get_substitution_names, create_exchange_queue, flexible_is_subclass, thread_it

logger, tracer = get_logger_tracer(__name__)

T = NewType("T", object)


class _Serde:
    """A serializer/deserializer pair"""

    def __init__(self, obj_serializer: Callable[[T], bytes], obj_deserializer: Callable[[bytes], T]):
        self._obj_serializer = obj_serializer
        self._obj_deserializer = obj_deserializer

    # we want to enhance the serde slightly to also record the type itself (needed for generic/unknown deserialization)
    def serialize(self, obj: T) -> bytes:
        with tracer.start_as_current_span("hatter:serialize"):
            obj_bytes = self._obj_serializer(obj)
            return pickle.dumps((obj_bytes, type(obj)))

    def deserialize(self, bites: bytes, expected_type: Optional[Type[T]] = None, chain: bool = False) -> T:
        with tracer.start_as_current_span("hatter:deserialize"):
            try:
                obj_bytes, typ = self.deserialize_raw(bites)
                if expected_type is not None:
                    if not flexible_is_subclass(typ, expected_type):
                        raise ValueError(f"Unexpected object after deserialization: expected {expected_type}, provided {typ}")

                # If typ is a dict, we will have done a "double serialize", need to unwrap that.
                if chain and typ == dict:
                    # Get the dict to serialized data...
                    dict_to_bytes: Dict[str, bytes] = self._obj_deserializer(obj_bytes)
                    # ...then deser all the values
                    return {k: self.deserialize(v, chain=True) for k, v in dict_to_bytes.items()}

                return self._obj_deserializer(obj_bytes)
            except pickle.UnpicklingError as e:
                # Perhaps it wasn't published using hatter? Try to deserialize directly
                try:
                    ret = self._obj_deserializer(bites)
                    warnings.warn("Submitted message was not published with hatter, and type cannot be checked.")
                    logger.debug("Unpickling error:", exc_info=e)
                    return ret
                except Exception as e:
                    raise ValueError(f"Cannot deserialize bytes. (Starting bytes: [{bites[:-20]}].)", e)

    @staticmethod
    def deserialize_raw(raw_bytes: bytes) -> Tuple[bytes, Type[T]]:
        obj_bytes, typ = pickle.loads(raw_bytes)
        return obj_bytes, typ


class SerdeRegistry:
    """
    Stores serializers and deserializers. Can be interacted with similarly to a dict, but is more intelligent about key selection and
    matching.
    """

    # Default: try with pickle
    # TODO some other default ones? like str <-> bytes, json for pydantic, etc.?
    _default_serde = _Serde(pickle.dumps, pickle.loads)

    def __init__(self):
        self._serdes: Dict[Type[T], _Serde] = dict()

    def __contains__(self, item):
        return self._closest_registered_type(item) is not None

    def __getitem__(self, item):
        closest_type = self._closest_registered_type(item)
        if closest_type is None:
            return SerdeRegistry._default_serde
        else:
            return self._serdes[closest_type]

    def register_serde(self, typ: Type[T], obj_serializer: Callable[[T], bytes], obj_deserializer: Callable[[bytes], T]):
        _serde = _Serde(obj_serializer, obj_deserializer)
        self._serdes[typ] = _serde

    def generic_deserialize(self, raw_bytes: bytes, chain: bool) -> T:
        """
        Deserializes without knowledge of the type. Not as reliable as finding a specific serde, as type checking is impossible.
        """
        # Figure out the type first
        _, typ = _Serde.deserialize_raw(raw_bytes)

        # And now deserialize
        return self[typ].deserialize(raw_bytes, chain=chain)

    def _closest_registered_type(self, search_type: Type[T]) -> Optional[Type[T]]:
        """
        Find the "best" matching serde type we have registered. This should be the type itself, or the closest superclass to the provided
        type.

        TODO be smarter about a real hierarchy.
        """
        # in the dict directly?
        if search_type in self._serdes:
            return search_type

        # what about anything that's a superclass?
        for potential_type in self._serdes.keys():
            if flexible_is_subclass(search_type, potential_type):
                return potential_type

        return None


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

    def __init__(
        self,
        rabbitmq_host: str,
        rabbitmq_user: str,
        rabbitmq_pass: str,
        rabbitmq_virtual_host: str = "/",
        rabbitmq_port: int = 5672,
        rabbitmq_rest_port: int = 15672,
        tls: bool = False,
        heartbeat: Optional[int] = None,
    ):

        # Init an AMQPManager. Actual connectivity isn't started until __enter__ via a with block.
        self._amqp_manager: AMQPManager = AMQPManager(
            rabbitmq_host, rabbitmq_user, rabbitmq_pass, rabbitmq_virtual_host, rabbitmq_port, rabbitmq_rest_port, tls, heartbeat
        )

        # we need a registry of coroutines and/or async generators (to be added via @hatter.listen(...) decorators). Each coroutine or
        # generator in this registry will be set as a callback for its associated queue when calling `run`
        self._registry: List[RegisteredCoroOrGen] = list()

        # We also need a registry of serializers/deserializers which can be used to convert raw AMQP messages to a known python type, and
        # vice versa
        self._serde_registry = SerdeRegistry()

        # Will get replaced/modified at runtime
        self._run_kwargs = dict()
        self._tasks: List[asyncio.Task] = list()

    def register_serde(self, typ: Type[T], serializer: Callable[[T], bytes], deserializer: Callable[[bytes], T]):
        self._serde_registry.register_serde(typ, serializer, deserializer)

    def serialize(self, obj: Any) -> bytes:
        """
        Generically serialize an object. If possible, will use a serde matching the type of the object passed in.
        """
        return self._serde_registry[type(obj)].serialize(obj)

    def generic_deserialize(self, raw_message_bytes: bytes, chain: bool = False) -> T:
        """
        If we don't have a "target" type (i.e. we aren't using hatter to decorate an annotated function) then we need to deserialize
        "generically" based on the type given.

        `chain` indicates whether or not to chain into nested structures (such as dicts) and deserialize inner components as well.
        This is usually needed when parsing the return value passed back through an RPC queue.
        """
        return self._serde_registry.generic_deserialize(raw_message_bytes, chain)

    def listen(
        self,
        *,
        queue_name: Optional[str] = None,
        exchange_name: Optional[str] = None,
        concurrency: int = 1,
        autoack: bool = False,
        blocks: bool = False,
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

        If concurrency is passed and > 1, then that many messages will be processed concurrently, if available from the queue. Note that
        due to Python's GIL, actual parallel processing cannot happen in threads, so this should generally be used for coroutines which
        are not CPU intensive, and/or spend a lot of time `await`ing other coroutines.

        TODO If an exchange_name is passed, headers can also be passed to make it a headers exchange.

        TODO maybe there's a cleaner way to do this? ^
        """

        def decorator(coro_or_gen: DecoratedCoroOrGen) -> DecoratedCoroOrGen:
            # Register this coroutine/async-generator for later listening
            if inspect.iscoroutinefunction(coro_or_gen) or inspect.isasyncgenfunction(coro_or_gen):
                self._register_listener(coro_or_gen, queue_name, exchange_name, concurrency, autoack, blocks)
                return coro_or_gen
            else:
                raise ValueError(
                    f"Cannot decorate `{coro_or_gen.__name__}` (type `{type(coro_or_gen).__name__}`). Must be coroutine (`async def`) or "
                    f"async-generator (`async def` that `yield`s)."
                )

        return decorator

    def _register_listener(
        self,
        coro_or_gen: DecoratedCoroOrGen,
        queue_name: Optional[str],
        exchange_name: Optional[str],
        concurrency: int,
        autoack: bool,
        blocks: bool,
    ):
        """
        Adds function to registry
        """
        self._registry.append(
            RegisteredCoroOrGen(
                coro_or_gen=coro_or_gen,
                queue_name=queue_name,
                exchange_name=exchange_name,
                concurrency=concurrency,
                autoack=autoack,
                blocks=blocks,
            )
        )

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
        self._amqp_manager.listening_coros = [c.coro_or_gen.__name__ for c in self._registry]
        await self._amqp_manager.__aenter__()

        for registered_obj in self._registry:
            self._tasks.append(asyncio.create_task(self.consume_coro_or_gen(registered_obj, self._run_kwargs)))

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for task in self._tasks:
            logger.info(f"Cancelling task {task}")
            task.cancel()
        await self._amqp_manager.__aexit__(exc_type, exc_val, exc_tb)

    async def wait(self):
        """
        Can be used to wait on all running listeners. Will return when any waiting listener has an error
        """
        if len(self._tasks) == 0:
            raise RuntimeError("No coroutines found listening")
        completed_tasks, _ = await asyncio.wait(self._tasks, return_when=asyncio.FIRST_EXCEPTION)

        # Are there exceptions?
        exceptions: List[BaseException] = [x.exception() for x in completed_tasks if x.exception() is not None]
        for exception in exceptions:
            logger.exception("Exception raised from listener", exc_info=exception)
        if len(exceptions) > 0:
            raise RuntimeError("One or more listeners raised an exception.")

    async def run(self, **kwargs):
        """
        Shorthand coroutine which connects, starts listening on all registered coroutines, and blocks while listening. This can be used as
        the "entrypoint" to a hatter-backed microservice.
        """
        async with self(**kwargs):
            logger.info("Hatter connected and listening...")
            await self.wait()

    async def consume_coro_or_gen(self, registered_obj: RegisteredCoroOrGen, run_kwargs: Dict[str, Any]):
        """
        Sets up prerequisites and begins consuming on the specified queue.
        """

        logger.debug(f"Starting consumption for: {str(registered_obj)}")

        consume_channel: Channel = await self._amqp_manager.new_channel(prefetch=registered_obj.concurrency)

        ex_name, q_name, resolved_substitutions = await self.build_exchange_queue_names(registered_obj, run_kwargs)

        # Make sure that all args/kwargs of decorated coro/generator are accounted for. We support:
        # - An unlimited number of kwargs which match one to one with a param in the queue and/or exchange name (will be passed through)
        # - An unlimited number of kwargs which aren't one of these params, but which we can serialize/deserialize. These are transmitted
        #   as the message body. If there's only one kwarg in this category, then the HatterMessage doesn't need to explicitly name it...we
        #   can match it up regardless.
        # - A few specific other kwargs:
        #   - reply_to: str
        #   - correlation_id: str
        #   TODO any more?
        fixed_callable_kwargs: Dict[str, Any] = dict()
        dynamic_callable_kwargs: Dict[str, Callable[[IncomingMessage], Any]] = dict()
        message_body_kwargs: Dict[str, Callable[[bytes], Any]] = dict()

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
            elif param.name == "correlation_id":
                dynamic_callable_kwargs[param.name] = lambda msg: msg.correlation_id
            # If not, we assume it's intended to be deserialized from the message body.
            else:
                # Convert the annotation to a type (if possible)
                ann = param.annotation
                if ann == inspect.Signature.empty:
                    typ = None
                elif isinstance(ann, type):
                    typ = ann
                elif isinstance(get_origin(ann), type):
                    typ = get_origin(ann)
                else:
                    # Generically deserialize. Not perfect because we don't have a second validation.
                    typ = None

                def _deserialize(data_: bytes, type_=typ):
                    if type_ is None:
                        return self.generic_deserialize(data_)
                    else:
                        return self._serde_registry[type_].deserialize(data_, type_)

                message_body_kwargs[param.name] = _deserialize

        # Create exchange and/or queue
        exchange, queue = await create_exchange_queue(ex_name, q_name, consume_channel)

        # Consume from this queue, forever
        async for message in queue.iterator(no_ack=registered_obj.autoack):
            logger.debug("Got a message")
            message: IncomingMessage
            try:
                if message.reply_to is not None and registered_obj.blocks:
                    # Check if response queue exists still. We use "blocks=True" as a limit as those are generally the slower functions.
                    # Because non-existent queues trigger a channel close, we use the REST api to query instead.
                    resp = await self._amqp_manager.rest_client_session.get(f"/api/queues/{self._amqp_manager.vhost}/{message.reply_to}")
                    if resp.status == 200:
                        # all good, proceed.
                        pass
                    elif resp.status == 404:
                        logger.info(f"Skipping message {message.message_id} as reply-to queue is non-existent.")
                        if not registered_obj.autoack:
                            await message.nack(requeue=False)
                        continue
                    elif resp.status in (401, 403):
                        logger.warning(
                            f"Unable to validate reply-to queue...user {self._amqp_manager.rest_client_session.auth.login} "
                            f"should have the `management` tag set."
                        )
                        pass

                # Use context on the message headers
                with context_from_carrier(message.headers):
                    add_event("message received")
                    # Build kwargs from fixed...
                    callable_kwargs = dict()
                    callable_kwargs.update(fixed_callable_kwargs)

                    # ... dynamic (i.e. based on the message but used for routing/config) ...
                    for kwarg, kwarg_function in dynamic_callable_kwargs.items():
                        callable_kwargs[kwarg] = kwarg_function(message)

                    # ... and message body (i.e. based on the message and passed into the function)
                    # Extract the message body

                    data = self.generic_deserialize(message.body)

                    # If a dict, special handling
                    if isinstance(data, dict):
                        # The passed dict might be the entire value of a single kwarg function. Check for that...
                        # single_message_body_kwarg, single_kwarg_function = next(iter(message_body_kwargs.items()))
                        if (
                            len(message_body_kwargs) == 1
                            and (single_message_body_kwarg := next(iter(message_body_kwargs.items()))[0]) not in data
                        ):
                            # This means there's one kwarg on the hatter'ed function, _and_ that kwarg name doesn't exist in the dict
                            # Assume that a dict _was_ the actual single value intended to be passed (and not a mapping from kwarg names
                            # to values).
                            callable_kwargs[single_message_body_kwarg] = {k: self.generic_deserialize(v) for k, v in data.items()}
                        else:
                            # Assume that the dict keys are function kwargs
                            for kwarg, kwarg_function in message_body_kwargs.items():
                                if kwarg in data:
                                    callable_kwargs[kwarg] = kwarg_function(data[kwarg])
                    else:
                        # Better hope that there's only one arg to the function. If there is, pass in the data object to it
                        if len(message_body_kwargs) == 1:
                            single_message_body_kwarg, single_kwarg_function = next(iter(message_body_kwargs.items()))
                            # Use the explicit typed serde instead of generic_deserialize
                            callable_kwargs[single_message_body_kwarg] = single_kwarg_function(message.body)
                        else:
                            raise RuntimeError(
                                "Decorated coroutine has more than one body argument, but a dictionary was not passed as the message."
                            )

                    # We call the registered coroutine/generator with these built kwargs
                    # Do this in a coroutine so that other messages can be processed concurrently
                    asyncio.create_task(self._process_message(message, registered_obj, **callable_kwargs))
            except asyncio.CancelledError:
                # We were told to cancel. nack with requeue so someone else will pick up the work
                if not registered_obj.autoack:
                    await message.nack(requeue=True)
                logger.info("Cancellation requested during message task instantiation.")
                raise
            except Exception as e:
                await self._handle_message_exception(message, e, registered_obj.autoack)

            logger.debug("Listening again")

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

    async def publish(self, msg: HatterMessage) -> Optional[str]:
        """
        Publishes the given message. Can be used for ad-hoc publishing of messages in a `@hatter.listen` decorated method, or by a
        publish-only application (such as a client in an RPC application). If publishing a message at the end of a listening method, you
        can just return or yield the `HatterMessage` object and it will be published automatically.

        Returns the message's correlation ID which can be used when tracking responses (e.g. in an RPC application)

        All optional keyword arguments are defined in the same way as RabbitMQ.
        """
        await self._publish_hatter_message(msg, self._amqp_manager.publish_channel)
        return msg.correlation_id

    @property
    def publish_channel(self) -> Optional[Channel]:
        return self._amqp_manager.publish_channel

    async def adhoc_consume(self, exchange_name: str) -> AsyncIterator[Any]:
        """
        By design, this only implements a very simple situation:

        - Infinite consumption from a fanout exchange (with the given name)
        - Expects single values which are `generic_deserialize`d.

        For more complex situations, this most likely shouldn't be used directly, and instead consider how using hatter decorators and
        coroutines better suits your situation.
        """
        async with await self._amqp_manager.new_channel() as c:
            # Declare the fanout exchange...
            ex, queue = await create_exchange_queue(exchange_name, None, c)

            # ... and start listening on a throwaway queue
            async for message in queue.iterator():
                message: IncomingMessage
                yield self.generic_deserialize(message.body, chain=True)

    async def create_temporary_queue(self) -> Queue:
        """
        Creates a temporary (transient) queue. Useful for, for example, RPC callbacks
        """
        _, queue = await create_exchange_queue(None, None, await self._amqp_manager.new_channel())
        return queue

    async def _process_message(self, message: IncomingMessage, registered_obj: RegisteredCoroOrGen, **callable_kwargs):
        logger.debug(f"Processing message {message.delivery_tag}")
        try:
            # Do we need to run the actual coro in a separate thread, not just coroutine? We need to run in a separate thread if:
            # - this particular coroutine is marked as "blocking", meaning a non-trivial amount of cpu bound work will take place that
            #   could block the whole event loop
            # Depending on the length of blocking/execution, we might be able to skip this UNLESS more than one message can be handled
            # by this process, from concurrency > 1 and/or multiple listening coroutines.
            # (registered_obj.concurrency > 1 or len(self._registry) > 1)
            # But for now we'll just always do it.

            with tracer.start_as_current_span("hatter:run_coro_or_gen"):
                if inspect.iscoroutinefunction(registered_obj.coro_or_gen):
                    # it returns
                    if registered_obj.blocks:
                        logger.debug(f"Running {registered_obj.coro_or_gen.__name__} in separate thread to prevent blocking.")
                        _task = thread_it(registered_obj.coro_or_gen(**callable_kwargs))
                    else:
                        _task = asyncio.create_task(registered_obj.coro_or_gen(**callable_kwargs))
                    return_val = await _task
                    await self._handle_return_value(message, return_val)
                else:
                    # it yields
                    if registered_obj.blocks:
                        warnings.warn(
                            "Async iterators which block are not currently supported. Running in main event loop; problems may result."
                        )

                    async for v in registered_obj.coro_or_gen(**callable_kwargs):
                        await self._handle_return_value(message, v)

            # At this point, we're processed the message and sent along new ones successfully. We can ack the original message
            # TODO consider doing all of this transactionally
            if not registered_obj.autoack:
                logger.debug(f"Acking message {message.delivery_tag}")
                await message.ack()
        except asyncio.CancelledError:
            # We were told to cancel. nack with requeue so someone else will pick up the work
            await message.nack(requeue=True)  # TODO check, this might not work if the message was autoacked
            logger.info("Cancellation requested.")
            raise
        except Exception as e_:
            await self._handle_message_exception(message, e_, registered_obj.autoack)

    async def _handle_return_value(self, triggering_message: Message, return_val: Any):
        if isinstance(return_val, HatterMessage):
            try:
                logger.debug(f"Publishing {return_val}")
                await self.publish(return_val)
            except PublishError as e:
                # TODO save to mongo or something?
                logger.exception(f"Unable to transmit return message {return_val}", e)
        else:
            # RPC?
            if triggering_message.reply_to is not None:
                # box it into a HatterMessage
                rpc_reply = HatterMessage(
                    data=return_val, destination_queue=triggering_message.reply_to, correlation_id=triggering_message.correlation_id
                )
                try:
                    await self.publish(rpc_reply)
                except PublishError as e:
                    logger.exception(f"Unable to transmit response to RPC queue {triggering_message.reply_to}.", exc_info=e)
                    if isinstance(return_val, Exception):
                        logger.info(f"Return value was exception:", exc_info=return_val)
                    else:
                        logger.info(f"Return value was: {return_val}")
            elif return_val is not None:
                warnings.warn(
                    f"Decorated coroutine/generator returned/yielded a {type(return_val)} value, but incoming message did not contain a "
                    f"'reply_to' queue (i.e., RPC pattern)."
                )

    async def _handle_message_exception(self, message, exception, autoack):
        # Send exception to reply-to queue, if applicable
        # TODO impl more properly. Consider exception type when deciding to requeue. Send to exception handling exchange
        if message.reply_to is not None:
            try:
                await self.publish(HatterMessage(data=exception, correlation_id=message.correlation_id, destination_queue=message.reply_to))
            except Exception as e_:
                logger.exception("Unreplyable exception", exc_info=exception)
                logger.exception("Error sending exception back to reply-to queue", exc_info=e_)
        if not autoack:
            await message.nack(requeue=False)
        logger.exception("Exception on message")

    async def _publish_hatter_message(self, msg: HatterMessage, channel: Channel):
        """Intelligently publishes the given message on the given channel"""
        # try to serialize message body
        # TODO catch pickle errors

        if isinstance(msg.data, dict):
            # serialize each dict value using a matching serde...
            bites_dict: Dict[str, bytes] = {k: self.serialize(v) for k, v in msg.data.items()}
            # ...then serialize the new str -> bytes dict for transmit
            bites: bytes = self._serde_registry[dict].serialize(bites_dict)
        else:
            # just have to serialize the raw value as needed
            bites: bytes = self.serialize(msg.data)

        amqp_message = Message(
            body=bites,
            reply_to=msg.reply_to_queue,
            correlation_id=msg.correlation_id,
            priority=msg.priority,
            headers=carrier_from_context(),
        )
        # TODO other fields like ttl

        # Exchange or queue based?
        with tracer.start_as_current_span("hatter:publish", kind=SpanKind.PRODUCER):
            if msg.destination_exchange is not None:
                # Exchange based it is
                exchange = await channel.get_exchange(msg.destination_exchange)
                routing_key = msg.routing_key or ""
                await exchange.publish(amqp_message, routing_key=routing_key, mandatory=False)
            else:
                # Queue based
                await channel.default_exchange.publish(amqp_message, routing_key=msg.destination_queue, mandatory=True)
