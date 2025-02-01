import time
from dataclasses import dataclass
from random import randint


def snake2kebab(x: str) -> str:
    return x.replace("_", "-")


def flatten(nested: list[list]) -> list:
    return [x for xs in nested for x in xs]


def kwargs2opts(*args, **kwargs) -> list[str]:
    x = {f"--{snake2kebab(k)}": v for k, v in kwargs.items()}
    return flatten([[k] if v is True else [k, v] for k, v in x.items()])


def raise_ex(x):
    if isinstance(x, Exception):
        raise x
    return x


def assocattr(x, k, v):
    setattr(x, k, v)
    return x


def current_time_millis():
    return round(time.time() * 1000)


def postwalk(obj, match, update):
    if isinstance(obj, dict):
        return {k: postwalk(v, match, update) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [postwalk(v, match, update) for v in obj]
    if isinstance(obj, set):
        return {postwalk(v, match, update) for v in obj}
    return update(obj) if match(obj) else obj


def replace(obj, **changes):
    def props(x):
        dunder = x.startswith("__")
        method = type(getattr(obj, x)).__name__ == "method"
        property = x in properties(obj)
        return not (dunder or method or property)

    result = type(obj)()
    [setattr(result, x, getattr(obj, x)) for x in filter(props, dir(obj))]
    for k, v in changes.items():
        setattr(result, k, v)
    return result


def properties(obj):
    result = []
    for name in dir(obj):
        attr = getattr(obj.__class__, name, None)
        if isinstance(attr, property):
            result.append(name)
    return result


def setter(obj, name):
    attr = getattr(obj.__class__, name, None)
    if attr:
        return getattr(attr, "setter", None)


@dataclass
class BackoffWithJitter:
    min: int = 10
    max: int = 10000
    k: int = 3
    state: int = 0

    def __call__(self):
        self.state = min(self.max, randint(self.min, max(self.min, self.state) * self.k))
        return self.state
