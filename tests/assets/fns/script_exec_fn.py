#!/usr/bin/env python3
import sys

from daggerml import Dml


def handler(dump):
    with open(sys.argv[1], "w") as f:
        f.write(dump)


if __name__ == "__main__":
    print("testing stdout...")
    print("testing stderr...", file=sys.stderr)
    with Dml(data=sys.stdin.read().strip(), message_handler=handler) as dml:
        with dml.new("test", "test") as d0:
            d0.n0 = sum(d0.argv[2:].value())  # 2 because the script is 1
            d0.result = d0.n0
