import os
import socket


def _current_owner() -> str:
    return f"{os.geteuid()}@{socket.gethostname()}"
