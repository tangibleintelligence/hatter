"""
Data objects
"""
from typing import Any, TypeVar, Optional, Coroutine, Union, AsyncGenerator, Callable
from uuid import uuid4

from pydantic import BaseModel, root_validator, validator

DecoratedCoro = TypeVar("DecoratedCoro", bound=Coroutine[None, None, Any])
DecoratedGen = TypeVar("DecoratedGen", bound=AsyncGenerator[Any, None])
DecoratedCoroOrGen = Union[DecoratedCoro, DecoratedGen]


class RegisteredCoroOrGen(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    coro_or_gen: Union[DecoratedCoroOrGen, Callable]
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
    correlation_id: Optional[str]
    destination_exchange: Optional[str]
    destination_queue: Optional[str]
    routing_key: Optional[str]
    # TODO headers ttl etc

    @validator("correlation_id", always=True)
    def generate_correlation_id(cls, v):
        if v is None:
            return str(uuid4())
        else:
            return v

    @root_validator
    def routable(cls, values):
        """To properly route, we must either have an exchange or a queue name...and not both."""
        destination_queue = values["destination_queue"]
        destination_exchange = values["destination_exchange"]
        routing_key = values["routing_key"]

        if destination_queue is None and destination_exchange is None:
            raise ValueError("Either destination_exchange or destination_queue must be provided.")

        if destination_queue is not None and destination_exchange is not None:
            raise ValueError(
                "Only one of destination_exchange or destination_queue may be provided. Perhaps you meant to pass an exchange and routing "
                "key?"
            )

        # If we only have a queue name, and also a routing_key, that doesn't make sense either
        if destination_exchange is None and routing_key is not None:
            raise ValueError(
                "Routing key doesn't make sense with only a queue name. Perhaps you meant to pass an exchange and routing key?"
            )

        return values
