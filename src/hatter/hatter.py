"""
Defines main object which is used to decorate methods
"""
import functools
from functools import partial
from typing import Callable, List, Optional

from hatter.domain import DecoratedCallable, ListeningCallable, Message
from hatter.util import get_param_names


class Hatter:
    """
    A `Hatter` instance can be leveraged to decorate functions and register them to respond to messages on specific queues.

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
            rabbitmq_virtual_host: str = '/'
    ):
        self._rabbitmq_host = rabbitmq_host
        self._rabbitmq_user = rabbitmq_user
        self._rabbitmq_pass = rabbitmq_pass
        self._rabbitmq_virtual_host = rabbitmq_virtual_host

        # we need a registry of functions (to be added via @hatter.listen(...) decorators). Each function in this registry will be set as a callback for its
        # associated queue when calling `run`
        self._registry: List[ListeningCallable] = list()

    def listen(
            self,
            queue_name: Optional[str] = None,
            exchange_name: Optional[str] = None
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        """
        Registers decorated function to run when a message is pushed from RabbitMQ on the given queue or exchange.

        A queue or exchange name can be parameterized by including `{a_var}` within the name string. These will be filled by properties specified
        in hatter.run().

        If an exchange name is passed, by default it will be assumed that the exchange is a fanout exchange and a temporary queue will be established to
        consume from this exchange.

        TODO If an exchange_name is passed, headers can also be passed to make it a headers exchange.

        TODO maybe there's a cleaner way to do this? ^
        """

        def decorator(func: DecoratedCallable) -> DecoratedCallable:
            # Register this function for later listening
            self._register_listener(
                func,
                queue_name,
                exchange_name
            )
            return func

        return decorator

    def _register_listener(
            self,
            func: DecoratedCallable,
            queue_name: Optional[str],
            exchange_name: Optional[str]
    ):
        """
        Adds function to registry
        """
        self._registry.append(
            ListeningCallable(
                func = func,
                queue_name = queue_name,
                exchange_name = exchange_name
            )
        )

    def run(self, **kwargs):
        """
        Begins listening on all registered queues.

        TODO also get params from env variables, not just kwargs
        """
        for lc in self._registry:
            print(lc)

            # Exactly one of the arguments should be type hinted as a Message type.
            message_arg_names = [k for k, v in lc.func.__annotations__.items() if v is Message]
            if len(message_arg_names) == 0:
                raise ValueError("An argument of the decorated function must be type hinted as a Message")
            if len(message_arg_names) > 1:
                raise ValueError("Only one argument of the decorated function must be type hinted as a Message")

            partial_kwargs = dict()
            partial_kwargs[message_arg_names[0]] = Message(data = 'ab')

            # Check for params in the queue/exchange name
            for param_name in get_param_names(lc.queue_name or lc.exchange_name):
                partial_kwargs[param_name] = kwargs[param_name]  # will break ugly if not in kwargs

            partial_fn = partial(lc.func, **partial_kwargs)

            # TODO would register partial_fn, binding the actual amqp message instead of the fake one above
            partial_fn()
