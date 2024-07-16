#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, Callable, Dict, Iterable, Optional

import dpath
from airbyte_cdk.models import AirbyteStream


def get_first(iterable: Iterable[object], predicate: Callable[[object], bool] = lambda m: True) -> Optional[object]:
    """Returns the first element in `iterable` that satisfies the `predicate`.

    Parameters
    ----------
    iterable : Iterable[object]
        An iterable from which to get the first element.
    predicate : Callable[[object], bool], optional
        A function to test each element, defaults to always True.

    Returns
    -------
    Optional[object]
        The first element that satisfies the predicate, or `None`.
    """
    for item in iterable:
        if predicate(item):
            return item
    return None


def get_defined_id(stream: AirbyteStream, data: Dict[str, Any]) -> Optional[str]:
    if not stream.source_defined_primary_key:
        return None
    primary_key = []
    for key in stream.source_defined_primary_key:
        try:
            primary_key.append(str(dpath.get(data, key)))
        except KeyError:
            primary_key.append("__not_found__")
    return "_".join(primary_key)
