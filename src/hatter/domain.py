"""
Data objects
"""
import abc
from enum import Enum
from typing import Any, Callable, TypeVar, Optional

from pydantic import BaseModel, root_validator

DecoratedCallable = TypeVar("DecoratedCallable", bound = Callable[..., Any])


class RegisteredCallable(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    func: DecoratedCallable
    queue_name: Optional[str]
    exchange_name: Optional[str]

    @root_validator
    def exactly_one(cls, values):
        # exactly one must be specified
        queue_name = values['queue_name']
        exchange_name = values['exchange_name']

        if (queue_name is None and exchange_name is None) or (queue_name is not None and exchange_name is not None):
            raise ValueError("Exactly one of queue_name or exchange_name must be specified")

        return values


class Message(BaseModel):
    data: Any
    reply_to_queue: Optional[str]
    destination_exchange: Optional[str]
    # TODO headers ttl etc

# class ExchangeType(Enum):
#     DIRECT = 'direct'
#     FANOUT = 'fanout'
#     HEADERS = 'headers'
#
#
# class Exchange(BaseModel, abc.ABC):
#     name: str
#     durable: bool = False
#
#
# class DirectExchange(Exchange):
#     @property
#     def exchange_type(self):
#         return 'direct'
#
#
# class FanoutExchange(Exchange):
#     @property
#     def exchange_type(self):
#         return 'direct'
#
#
# class HeadersExchange(Exchange):
#     @property
#     def exchange_type(self):
#         return 'direct'
