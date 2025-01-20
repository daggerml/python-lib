import time


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
