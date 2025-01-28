import time
from dataclasses import dataclass
from random import randint


def snake2kebab(x: str) -> str:
    return x.replace('_', '-')


def flatten(nested: list[list]) -> list:
    return [x for xs in nested for x in xs]


def kwargs2opts(*args, **kwargs) -> list[str]:
    x = {f'--{snake2kebab(k)}': v for k, v in kwargs.items()}
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


def replace(obj, **changes):
    def props(x):
        return not (x.startswith('__') or type(getattr(obj, x)).__name__ == 'method')
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
        return getattr(attr, 'setter', None)


@dataclass
class BackoffWithJitter:
    min: int = 10
    max: int = 10000
    k: int = 3
    state: int = 0

    def __call__(self):
        self.state = min(self.max, randint(self.min, max(self.min, self.state) * self.k))
        return self.state
