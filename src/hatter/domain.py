"""
Data objects
"""
from typing import Any, TypeVar, Optional, Coroutine, Union, AsyncGenerator

from pydantic import BaseModel, root_validator

DecoratedCoroOrGen = TypeVar("DecoratedCoroOrGen", bound=Union[Coroutine[..., Any], AsyncGenerator[Any, ...]])


class RegisteredCoroOrGen(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    coro_or_gen: DecoratedCoroOrGen
    queue_name: Optional[str]
    exchange_name: Optional[str]

    @root_validator
    def at_least_one(cls, values):
        # at least one must be specified
        queue_name = values["queue_name"]
        exchange_name = values["exchange_name"]

        if queue_name is None and exchange_name is None:
            raise ValueError("At least one of queue_name or exchange_name must be specified")

        return values


class HatterMessage(BaseModel):
    data: Any
    reply_to_queue: Optional[str]
    destination_exchange: Optional[str]
    destination_queue: Optional[str]
    routing_key: Optional[str]
    # TODO headers ttl etc

    @root_validator
    def routable(cls, values):
        """To properly route, we must either have an exchange or a queue name...and not both."""
        destination_queue = values["destination_queue"]
        destination_exchange = values["destination_exchange"]

        if destination_queue is None and destination_exchange is None:
            raise ValueError("Either destination_exchange or destination_queue must be provided.")

        if destination_queue is not None and destination_exchange is not None:
            raise ValueError(
                "Only one of destination_exchange or destination_queue may be provided. Perhaps you meant to pass an exchange and routing key?"
            )
