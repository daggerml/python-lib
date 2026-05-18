from __future__ import annotations

import argparse
import inspect
import json
import logging
import sys
import types
import typing
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Callable, Literal, Union, cast, get_args, get_origin

from daggerml._internal import Dml, Ref, Runnable, Uri


class PrettyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(2, f"error: {message}\n")


@dataclass(frozen=True)
class _Target:
    path: tuple[str, ...]
    method_name: str
    kind: Literal["instance", "classmethod"]


class MethodCLI:
    """
    Turn a class with methods and nested namespace objects into an argparse CLI.

    - Accepts a class constructor, not a class instance.
    - Root args/options/flags come from the class constructor signature.
    - A root -v / -vv / -vvv flag configures logging level.
    - The class is instantiated after root args are parsed.
    - Public instance methods become commands.
    - Public annotated namespace attributes become namespaces.
    - Required method parameters become positional args.
    - Constructor parameters are root options.
    - Parameters with defaults become options/flags.
    - Command and option names use kebab-case.
    - Positional arg names remain snake_case.
    - bool kwargs are always flags.
      - default False/None: --foo
      - default True: --no-foo
    - Annotated[T, "help"] provides per-argument help.
    - Docstrings provide command help and optional parameter help.
    - Results are printed to stdout as JSON.
    """

    def __init__(
        self,
        cls: type[Any],
        *,
        prog: str | None = None,
        parsers: dict[Any, Callable[[str], Any]] | None = None,
    ) -> None:
        if not isinstance(cls, type):
            raise TypeError("MethodCLI expects a class, not an instance")
        self.cls = cls
        self.parsers = dict(parsers or {})
        self.parser = PrettyArgumentParser(
            prog=prog or self._kebab(cls.__name__),
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        self.parser.add_argument(
            "-v",
            dest="_verbosity",
            action="count",
            default=0,
            help="Increase logging verbosity. Use -v, -vv, or -vvv.",
        )
        self._add_constructor_args(self.parser)
        self._build_namespace_from_type(cls, self.parser, path=())

    def main(self, argv: list[str] | None = None) -> int:
        try:
            return self.run(argv)
        except KeyboardInterrupt:
            print("error: interrupted", file=sys.stderr)
            return 130
        except Exception as exc:
            logging.exception("command failed")
            print(f"error: {exc}", file=sys.stderr)
            return 1

    def run(self, argv: list[str] | None = None) -> int:
        ns = self.parser.parse_args(argv)
        data = vars(ns)
        verbosity = data.pop("_verbosity", 0)
        self._configure_logging(verbosity)
        target = data.pop("_target", None)
        if target is None:
            self.parser.error("missing command")
        init_kwargs = {
            name: data.pop(f"_init_{name}") for name in self._constructor_param_names() if f"_init_{name}" in data
        }
        method_kwargs = {k: v for k, v in data.items() if not k.startswith("_subcommand_")}
        target = cast(_Target, target)
        root = self.cls if target.kind == "classmethod" else self.cls(**init_kwargs)
        method = self._resolve_method(root, target)
        result = method(**method_kwargs)
        print(json.dumps(result, indent=2, sort_keys=True, default=self._json_default))
        return 0

    def _configure_logging(self, verbosity: int) -> None:
        level = logging.WARNING if verbosity <= 0 else logging.INFO if verbosity == 1 else logging.DEBUG
        logging.basicConfig(level=level, format="%(levelname)s: %(message)s", force=True)

    def _add_constructor_args(self, parser: argparse.ArgumentParser) -> None:
        init = self.cls.__init__
        doc = self._parse_docstring(inspect.getdoc(init) or inspect.getdoc(self.cls) or "")
        group = parser.add_argument_group("constructor arguments")
        self._add_callable_args(
            group, init, doc.param_help, dest_prefix="_init_", skip_self=True, required_as_options=True
        )

    def _constructor_param_names(self) -> list[str]:
        sig = inspect.signature(self.cls.__init__)
        return [
            name
            for name, param in sig.parameters.items()
            if name != "self" and param.kind not in (param.VAR_POSITIONAL, param.VAR_KEYWORD)
        ]

    def _build_namespace_from_type(
        self, typ: type[Any], parser: argparse.ArgumentParser, path: tuple[str, ...]
    ) -> None:
        subparsers = parser.add_subparsers(dest="_subcommand_" + "_".join(path or ("root",)), required=True)
        for name, member in sorted(vars(typ).items()):
            if self._is_public_method_descriptor(name, member):
                command_name = self._kebab(name)
                doc = self._parse_docstring(inspect.getdoc(member) or "")
                child = subparsers.add_parser(
                    command_name,
                    help=doc.summary,
                    description=doc.description or doc.summary,
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                )
                child.set_defaults(_target=_Target(path=path, method_name=name, kind="instance"))
                self._add_callable_args(child, member, doc.param_help, skip_self=True, required_as_options=False)
            elif not path and self._is_public_root_classmethod_descriptor(name, member):
                command_name = self._kebab(name)
                method = getattr(typ, name)
                doc = self._parse_docstring(inspect.getdoc(method) or "")
                child = subparsers.add_parser(
                    command_name,
                    help=doc.summary,
                    description=doc.description or doc.summary,
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                )
                child.set_defaults(_target=_Target(path=path, method_name=name, kind="classmethod"))
                self._add_callable_args(child, method, doc.param_help, skip_self=False, required_as_options=False)
        for name, namespace_type in sorted(self._namespace_types_for(typ).items()):
            child = subparsers.add_parser(
                self._kebab(name),
                help=f"{name} commands",
                formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            )
            self._build_namespace_from_type(namespace_type, child, path + (name,))

    def _namespace_types_for(self, typ: type[Any]) -> dict[str, type[Any]]:
        out: dict[str, type[Any]] = {}
        for name, member in vars(typ).items():
            if name.startswith("_"):
                continue
            if not isinstance(member, property):
                continue
            fget = member.fget
            if fget is None:
                continue
            try:
                prop_hints = typing.get_type_hints(fget, include_extras=True)
            except Exception:
                prop_hints = getattr(fget, "__annotations__", {})
            ret = prop_hints.get("return")
            if ret is None:
                continue
            base, _ = self._split_annotated(ret)
            if isinstance(base, type) and self._looks_like_namespace_type(base):
                out[name] = base
        return out

    def _looks_like_namespace_type(self, typ: type[Any]) -> bool:
        scalar_types = (str, bytes, int, float, bool, list, tuple, dict, set, Path)
        if typ in scalar_types or issubclass(typ, Enum):
            return False
        return any(self._is_public_method_descriptor(n, m) for n, m in vars(typ).items())

    def _add_callable_args(
        self,
        parser_or_group: argparse.ArgumentParser | argparse._ArgumentGroup,
        fn: Callable[..., Any],
        doc_help: dict[str, str],
        *,
        dest_prefix: str = "",
        skip_self: bool,
        required_as_options: bool,
    ) -> None:
        sig = inspect.signature(fn)
        hints = typing.get_type_hints(fn, include_extras=True)
        for name, param in sig.parameters.items():
            if skip_self and name == "self":
                continue
            if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
                raise TypeError(f"{fn.__qualname__}: *args and **kwargs are not supported")
            typ, annotated_help = self._split_annotated(hints.get(name, param.annotation))
            help_text = annotated_help or doc_help.get(name) or self._type_display(typ)
            has_default = param.default is not inspect._empty
            default = None if not has_default else param.default
            dest = f"{dest_prefix}{name}"
            if self._is_bool_type(typ):
                if not has_default:
                    raise TypeError(f"{fn.__qualname__}.{name}: bool parameters must have defaults")
                self._add_bool_flag(parser_or_group, name, dest, default, help_text)
                continue
            converter, extra = self._parser_for(typ)
            if has_default:
                kwargs: dict[str, Any] = {"dest": dest, "default": default, "help": help_text, **extra}
                if converter is not None:
                    kwargs["type"] = converter
                parser_or_group.add_argument(f"--{self._kebab(name)}", **kwargs)
            elif required_as_options:
                kwargs = {"dest": dest, "required": True, "help": help_text, **extra}
                if converter is not None:
                    kwargs["type"] = converter
                parser_or_group.add_argument(f"--{self._kebab(name)}", **kwargs)
            else:
                kwargs = {"help": help_text, **extra}
                if converter is not None:
                    kwargs["type"] = converter
                parser_or_group.add_argument(name, **kwargs)

    def _add_bool_flag(
        self,
        parser_or_group: argparse.ArgumentParser | argparse._ArgumentGroup,
        name: str,
        dest: str,
        default: Any,
        help_text: str,
    ) -> None:
        kebab = self._kebab(name)
        if default is True:
            parser_or_group.add_argument(f"--no-{kebab}", dest=dest, action="store_false", default=True, help=help_text)
        else:
            parser_or_group.add_argument(
                f"--{kebab}", dest=dest, action="store_true", default=bool(default), help=help_text
            )

    def _resolve_method(self, root: Any, target: _Target) -> Callable[..., Any]:
        obj = root
        for name in target.path:
            obj = getattr(obj, name)
        return getattr(obj, target.method_name)

    def _parser_for(self, typ: Any) -> tuple[Callable[[str], Any] | None, dict[str, Any]]:
        typ, _ = self._unwrap_optional(typ)
        if typ in self.parsers:
            return self.parsers[typ], {}
        origin = get_origin(typ)
        args = get_args(typ)
        if origin is Literal:
            choices = list(args)
            parser = type(choices[0]) if choices else str
            return parser, {"choices": choices}
        if typ in {Ref, Uri}:
            return lambda s: typ(s), {}
        if typ in (str, int, float):
            return typ, {}
        if origin in (list, dict):
            return self._json_parser(typ), {}
        return str, {}

    def _json_parser(self, typ: Any) -> Callable[[str], Any]:
        def parse(value: str) -> Any:
            try:
                return json.loads(value)
            except json.JSONDecodeError as exc:
                raise argparse.ArgumentTypeError(f"expected JSON for {self._type_display(typ)}: {exc.msg}") from exc

        return parse

    def _split_annotated(self, typ: Any) -> tuple[Any, str | None]:
        if get_origin(typ) is Annotated:
            base, *metadata = get_args(typ)
            for item in metadata:
                if isinstance(item, str):
                    return base, item
            return base, None
        return typ, None

    def _unwrap_optional(self, typ: Any) -> tuple[Any, bool]:
        origin = get_origin(typ)
        args = get_args(typ)
        if origin in (Union, types.UnionType) and type(None) in args:
            rest = tuple(a for a in args if a is not type(None))
            if len(rest) == 1:
                return rest[0], True
        return typ, False

    def _is_bool_type(self, typ: Any) -> bool:
        typ, _ = self._unwrap_optional(typ)
        return typ is bool

    def _is_public_method_descriptor(self, name: str, member: Any) -> bool:
        if name.startswith("_"):
            return False
        if isinstance(member, (staticmethod, classmethod)):
            return False
        return inspect.isfunction(member)

    def _is_public_root_classmethod_descriptor(self, name: str, member: Any) -> bool:
        return not name.startswith("_") and isinstance(member, classmethod)

    def _json_default(self, obj: Any) -> Any:
        if isinstance(obj, Ref):
            return str(obj.to)
        if isinstance(obj, Uri):
            return obj.uri
        if isinstance(obj, Runnable):
            raise NotImplementedError("Runnable objects cannot be serialized to JSON directly")
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    def _kebab(self, name: str) -> str:
        return name.replace("_", "-")

    def _type_display(self, typ: Any) -> str:
        if typ is inspect._empty:
            return "value"
        return str(typ).replace("typing.", "")

    @dataclass
    class _Doc:
        summary: str | None
        description: str | None
        param_help: dict[str, str]

    def _parse_docstring(self, doc: str) -> _Doc:
        if not doc:
            return self._Doc(None, None, {})
        lines = doc.splitlines()
        summary = lines[0].strip() or None
        param_help: dict[str, str] = {}
        desc_lines: list[str] = []
        in_args = False
        current: str | None = None
        for raw in lines[1:]:
            stripped = raw.strip()
            if stripped in {"Args:", "Arguments:", "Parameters:"}:
                in_args = True
                current = None
                continue
            if in_args:
                if not stripped:
                    continue
                if not raw.startswith((" ", "\t")):
                    in_args = False
                    current = None
                else:
                    if ":" in stripped:
                        key, text = stripped.split(":", 1)
                        current = key.strip()
                        param_help[current] = text.strip()
                    elif current:
                        param_help[current] = (param_help[current] + " " + stripped).strip()
                    continue
            if not in_args and stripped:
                desc_lines.append(stripped)
        return self._Doc(summary, "\n".join(desc_lines).strip() or None, param_help)


def cli() -> None:
    raise SystemExit(MethodCLI(Dml, prog="dml").main())
