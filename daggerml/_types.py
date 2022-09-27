from typing import Any
from dataclasses import dataclass


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
    """
    type: str
    data: Any
