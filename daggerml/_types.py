from typing import Any
from dataclasses import dataclass


@dataclass(frozen=True)
class Func:
    """A python class representing a cloud function

    Attributes
    ----------
    executor : str
        the executor ID (what infrastructure does this func run on)
    data : any
        the data that defines the func (for that executor)
    """
    executor: str
    data: Any


@dataclass(frozen=True)
class Resource:
    """an external resource (e.g. docker image, s3 object, etc.)

    Attributes
    ----------
    type : str
        the resource type (e.g. docker-image, or s3-blob, etc.)
    data : dict[str, any]
        the requisite data for accessing and permissioning the data (the
        s3-location, etc.).

    Notes
    -----
    You should only instantiate this class directly if you really know what
    you're doing. Otherwise, there are helper functions that return these
    things.
    """
    type: str
    data: Any
