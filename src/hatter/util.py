"""
Helpful stateless utility methods
"""
import re
from typing import Set


def exchange_name(base: str):
    return f'exchange__{base}'


def queue_name(base: str):
    return f'queue__{base}'


def get_param_names(a_str: str) -> Set[str]:
    return set(re.findall("{(.*?)}", a_str))
