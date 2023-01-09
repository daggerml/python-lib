import sys
import json
from os.path import basename
from argparse import ArgumentParser

try:
    from argcomplete import autocomplete
except ModuleNotFoundError:
    def autocomplete(parser):
        pass


class Cli:
    def __init__(self, func, *args, **kwargs):
        kwargs['prog'] = basename(sys.argv[0])
        self.parser = ArgumentParser(*args, **kwargs)
        self.parser.set_defaults(func=func)
        self.subparsers = self.parser.add_subparsers(title='commands')

    def arg(self, *args, **kwargs):
        self.parser.add_argument(*args, **kwargs)
        return self

    def command(self, *args, **kwargs):
        def wrapped(f):
            return Command(self.subparsers, f, *args, **kwargs)
        return wrapped

    def __call__(self, *args, **kwargs):
        autocomplete(self.parser)
        args = vars(self.parser.parse_args(*args, **kwargs))
        func = args['func']
        args.pop('func', None)
        try:
            result = func(**args)
            if result is not None:
                print(json.dumps(result, indent=4))
            self.parser.exit(0)
        except Exception as e:
            self.parser.error(e)


class Command:
    def __init__(self, subparsers, func, *args, **kwargs):
        self.command = func.__name__
        self.name = self.command.replace('_', '-')
        self.func = func
        self.parser = subparsers.add_parser(self.name, *args, **kwargs)
        self.parser.set_defaults(func=self)

    def arg(self, *args, **kwargs):
        self.parser.add_argument(*args, **kwargs)
        return self

    def __call__(self, *args, **kwargs):
        result = self.func(*args, **kwargs)
        if result is not None:
            result = {self.command: result}
        return result


def cli(*args, **kwargs):
    def wrapped(f):
        return Cli(f, *args, **kwargs)
    return wrapped


def arg(*args, **kwargs):
    def wrapped(f):
        f.arg(*args, **kwargs)
        return f
    return wrapped
