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

try:
    from daggerml.__about__ import __version__
except ImportError:
    __version__ = "local"

from daggerml._core import Dml, Error, Ref, Runnable, Uri, dml_dumps, dml_loads


def _serialize_as(value: Any, expected: type[Any], render: Callable[[Any], str] = str) -> str:
    if expected is int:
        ok = isinstance(value, int) and not isinstance(value, bool)
    else:
        ok = isinstance(value, expected)
    if not ok:
        raise TypeError(f"expected {expected.__name__}, got {type(value).__name__}")
    return render(value)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Ref):
        return str(obj.to)
    if isinstance(obj, Uri):
        return obj.uri
    if isinstance(obj, Runnable):
        raise NotImplementedError("Runnable objects cannot be serialized to JSON directly")
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _serialize_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=_json_default)


def _read_cli_text(value: str) -> str:
    if value == "-":
        return sys.stdin.read()
    return Path(value).read_text(encoding="utf-8")


def _parse_json_file(value: str) -> Any:
    try:
        return json.loads(_read_cli_text(value))
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(f"expected JSON input: {exc.msg}") from exc
    except OSError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _parse_dml_file(value: str) -> Any:
    try:
        return dml_loads(_read_cli_text(value))
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(f"expected DML-serialized input: {exc.msg}") from exc
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(f"expected DML-serialized input: {exc}") from exc
    except OSError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _parse_bool(value: str) -> bool:
    lowered = value.strip().lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    raise argparse.ArgumentTypeError("expected boolean input: true or false")


def _parse_none(value: str) -> None:
    if value == "null":
        return None
    raise argparse.ArgumentTypeError("expected null")


CLI_FAMILY_PARSERS: dict[str, Callable[[str], Any]] = {
    "none": _parse_none,
    "dml": _parse_dml_file,
    "json": _parse_json_file,
    "float": float,
    "int": int,
    "bool": _parse_bool,
    "str": str,
    "ref": Ref,
}

CLI_FAMILY_SERIALIZERS: dict[str, Callable[[Any], str]] = {
    "dml": dml_dumps,
    "json": _serialize_json,
    "float": lambda value: _serialize_as(value, float),
    "int": lambda value: _serialize_as(value, int),
    "bool": lambda value: _serialize_as(value, bool, lambda item: "true" if item else "false"),
    "str": lambda value: _serialize_as(value, str),
    "ref": lambda value: _serialize_as(value, Ref, lambda item: str(item.to)),
}

CLI_SERDE_PRIORITY = ("none", "dml", "json", "float", "int", "bool", "str", "ref")
CLI_COLLECTION_TYPES = (dict, list)
CLI_LITERAL_SCALARS = {float, int, bool, str}


def _union_members(typ: Any) -> tuple[Any, ...]:
    origin = get_origin(typ)
    if origin in (Union, types.UnionType):
        return get_args(typ)
    return (typ,)


def _is_collection_type(typ: Any) -> bool:
    return typ in CLI_COLLECTION_TYPES or typing.is_typeddict(typ) or get_origin(typ) in CLI_COLLECTION_TYPES


def _family_for_type(typ: Any) -> str | None:
    if typ is type(None):
        return "none"
    if typ is Any or typ is Error:
        return "dml"
    if _is_collection_type(typ):
        return "json"
    if typ in CLI_LITERAL_SCALARS:
        return typ.__name__
    if typ is Ref:
        return "ref"
    if get_origin(typ) is Literal:
        choices = get_args(typ)
        if not choices or any(type(choice) is not type(choices[0]) for choice in choices):
            return None
        return _family_for_type(type(choices[0]))
    return None


def _matches_type(value: Any, typ: Any) -> bool:
    if typ is Any:
        return True
    if typ is type(None):
        return value is None
    if typ is Error:
        return isinstance(value, Error)
    if typ is Ref:
        return isinstance(value, Ref)
    if typ is str:
        return isinstance(value, str)
    if typ is bool:
        return isinstance(value, bool)
    if typ is int:
        return isinstance(value, int) and not isinstance(value, bool)
    if typ is float:
        return isinstance(value, float)
    if typing.is_typeddict(typ):
        return isinstance(value, dict)
    origin = get_origin(typ)
    if origin in CLI_COLLECTION_TYPES:
        return isinstance(value, origin)
    if origin is Literal:
        return value in get_args(typ)
    return False


def _matches_any_type(value: Any, allowed_types: tuple[Any, ...]) -> bool:
    return any(_matches_type(value, allowed) for allowed in allowed_types)


def _serde_families_for_type(typ: Any) -> tuple[_SerdeFamily, ...]:
    grouped: dict[str, list[Any]] = {}
    for member in _union_members(typ):
        family = _family_for_type(member)
        if family is None:
            return ()
        grouped.setdefault(family, []).append(member)
    ordered = ["none", "dml", "json", *(["str"] if "str" in grouped else [])]
    ordered.extend(name for name in CLI_SERDE_PRIORITY if name not in ordered)
    return tuple(_SerdeFamily(name, tuple(grouped[name])) for name in ordered if name in grouped)


class PrettyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(2, f"error: {message}\n")


class _GroupedSubParsersAction(argparse._SubParsersAction):
    class _GroupedChoicePseudoAction(argparse._SubParsersAction._ChoicesPseudoAction):
        def __init__(self, name: str, aliases: tuple[str, ...], help: str | None, category: str) -> None:
            super().__init__(name, aliases, help)
            self.category = category

    def add_parser(self, name: str, *, category: str = "command", **kwargs: Any):
        if kwargs.get("prog") is None:
            kwargs["prog"] = f"{self._prog_prefix} {name}"

        aliases = tuple(kwargs.pop("aliases", ()))

        if name in self._name_parser_map:
            raise argparse.ArgumentError(self, f"conflicting subparser: {name}")
        for alias in aliases:
            if alias in self._name_parser_map:
                raise argparse.ArgumentError(self, f"conflicting subparser alias: {alias}")

        if "help" in kwargs:
            help_text = kwargs.pop("help")
            choice_action = self._GroupedChoicePseudoAction(name, aliases, help_text, category)
            self._choices_actions.append(choice_action)

        parser = self._parser_class(**kwargs)
        self._name_parser_map[name] = parser
        for alias in aliases:
            self._name_parser_map[alias] = parser
        return parser


class PrettyHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    def _format_action(self, action):
        if isinstance(action, _GroupedSubParsersAction):
            return self._format_grouped_subparsers(action)
        return super()._format_action(action)

    def _format_grouped_subparsers(self, action: _GroupedSubParsersAction) -> str:
        parts: list[str] = []
        format_action = super()._format_action
        groups = (
            ("command", "commands"),
            ("namespace", "namespaces"),
        )
        for category, heading in groups:
            subactions = [
                subaction
                for subaction in action._get_subactions()
                if getattr(subaction, "category", None) == category
            ]
            if not subactions:
                continue
            parts.append(f"{self._current_indent * ' '}{heading}:\n")
            self._indent()
            parts.extend(format_action(subaction) for subaction in subactions)
            self._dedent()
        return self._join_parts(parts)


@dataclass(frozen=True)
class _Target:
    path: tuple[str, ...]
    method_name: str
    kind: Literal["instance", "classmethod"]


@dataclass(frozen=True)
class _SerdeFamily:
    name: str
    allowed_types: tuple[Any, ...]


@dataclass(frozen=True)
class _ConstructorParam:
    name: str
    typ: Any
    dest: str


@dataclass(frozen=True)
class _SubparserSpec:
    name: str
    help: str | None
    description: str | None
    category: Literal["command", "namespace"]


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
    - Results are printed to stdout using the resolved output transport.
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
        self._constructor_params = self._constructor_param_metadata()
        self.parser = PrettyArgumentParser(
            prog=prog or self._kebab(cls.__name__),
            formatter_class=PrettyHelpFormatter,
        )
        self.parser.add_argument(
            "-v",
            dest="_verbosity",
            action="count",
            default=0,
            help="Increase logging verbosity. Use -v, -vv, or -vvv.",
        )
        self.parser.add_argument("--version", action="version", version=f"%(prog)s, version {__version__}")
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
            param.name: data.pop(param.dest) for param in self._constructor_params.values() if param.dest in data
        }
        target = cast(_Target, target)
        root = self.cls if target.kind == "classmethod" else self.cls(**init_kwargs)
        method = self._resolve_method(root, target)
        method_kwargs = {k: v for k, v in data.items() if not k.startswith("_subcommand_")}
        if target.kind == "classmethod":
            method_kwargs.update({name: init_kwargs[name] for name in self._classmethod_init_arg_names(method)})
        method_args: list[Any] = []
        sig = inspect.signature(method)
        for name, param in sig.parameters.items():
            if (
                param.kind in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD)
                and param.default is inspect._empty
                and name in method_kwargs
            ):
                method_args.append(method_kwargs.pop(name))
            if param.kind is param.VAR_POSITIONAL and name in method_kwargs:
                method_args.extend(method_kwargs.pop(name))
        result = method(*method_args, **method_kwargs)
        if result is not None:
            print(self._serialize_command_result(target, method, result))
        return 0

    def _serialize_command_result(self, target: _Target, method: Callable[..., Any], result: Any) -> str:
        if target.kind == "classmethod" and isinstance(result, self.cls):
            return self._serialize_result(self.cls.status, result.status())
        return self._serialize_result(method, result)

    def _configure_logging(self, verbosity: int) -> None:
        level = logging.WARNING if verbosity <= 0 else logging.INFO if verbosity == 1 else logging.DEBUG
        logging.basicConfig(level=level, format="%(levelname)s: %(message)s", force=True)

    def _add_constructor_args(self, parser: argparse.ArgumentParser) -> None:
        init = self.cls.__init__
        if init is object.__init__:
            return
        doc = self._parse_docstring(inspect.getdoc(init) or inspect.getdoc(self.cls) or "")
        group = parser.add_argument_group("constructor arguments")
        self._add_callable_args(
            group, init, doc.param_help, dest_prefix="_init_", skip_self=True, required_as_options=True
        )

    def _constructor_param_metadata(self) -> dict[str, _ConstructorParam]:
        sig = inspect.signature(self.cls.__init__)
        hints = typing.get_type_hints(self.cls.__init__, include_extras=True)
        params: dict[str, _ConstructorParam] = {}
        for name, param in sig.parameters.items():
            if name == "self" or param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
                continue
            typ, _ = self._split_annotated(hints.get(name, param.annotation))
            params[name] = _ConstructorParam(name=name, typ=typ, dest=f"_init_{name}")
        return params

    def _build_namespace_from_type(
        self, typ: type[Any], parser: argparse.ArgumentParser, path: tuple[str, ...]
    ) -> None:
        subparsers = parser.add_subparsers(
            dest="_subcommand_" + "_".join(path or ("root",)),
            required=True,
            action=_GroupedSubParsersAction,
        )
        for name, member in sorted(vars(typ).items()):
            if self._is_public_method_descriptor(name, member):
                if not self._callable_is_generatable(member, skip_self=True):
                    continue
                doc = self._parse_docstring(inspect.getdoc(member) or "")
                child = subparsers.add_parser(
                    **self._subparser_kwargs(
                        _SubparserSpec(
                            name=self._kebab(name),
                            help=doc.summary,
                            description=doc.description or doc.summary,
                            category="command",
                        )
                    )
                )
                child.set_defaults(_target=_Target(path=path, method_name=name, kind="instance"))
                self._add_callable_args(child, member, doc.param_help, skip_self=True, required_as_options=False)
            elif not path and self._is_public_root_classmethod_descriptor(name, member):
                method = getattr(typ, name)
                if not self._callable_is_generatable(method, skip_self=False):
                    continue
                doc = self._parse_docstring(inspect.getdoc(method) or "")
                child = subparsers.add_parser(
                    **self._subparser_kwargs(
                        _SubparserSpec(
                            name=self._kebab(name),
                            help=doc.summary,
                            description=doc.description or doc.summary,
                            category="command",
                        )
                    )
                )
                child.set_defaults(_target=_Target(path=path, method_name=name, kind="classmethod"))
                self._add_callable_args(
                    child,
                    method,
                    doc.param_help,
                    skip_self=False,
                    required_as_options=False,
                    skip_names=self._classmethod_init_arg_names(method),
                )
        for name, namespace in sorted(self._namespace_entries_for(typ).items()):
            child = subparsers.add_parser(
                **self._subparser_kwargs(
                    _SubparserSpec(
                        name=self._kebab(name),
                        help=namespace.help_text,
                        description=namespace.doc.description or namespace.doc.summary,
                        category="namespace",
                    )
                )
            )
            self._build_namespace_from_type(namespace.namespace_type, child, path + (name,))

    def _subparser_kwargs(self, spec: _SubparserSpec) -> dict[str, Any]:
        return {
            "name": spec.name,
            "help": spec.help,
            "description": spec.description,
            "formatter_class": PrettyHelpFormatter,
            "category": spec.category,
        }

    def _namespace_entries_for(self, typ: type[Any]) -> dict[str, _Namespace]:
        out: dict[str, _Namespace] = {}
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
            base, annotated_help = self._split_annotated(ret)
            if isinstance(base, type) and self._looks_like_namespace_type(base):
                out[name] = _Namespace(
                    namespace_type=base,
                    help_text=annotated_help,
                    doc=self._parse_docstring(inspect.getdoc(fget) or ""),
                )
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
        skip_names: set[str] | None = None,
    ) -> None:
        sig = inspect.signature(fn)
        hints = typing.get_type_hints(fn, include_extras=True)
        for name, param in sig.parameters.items():
            if skip_self and name == "self":
                continue
            if skip_names is not None and name in skip_names:
                continue
            if param.kind is param.VAR_KEYWORD:
                raise TypeError(f"{fn.__qualname__}: **kwargs are not supported")
            typ, annotated_help = self._split_annotated(hints.get(name, param.annotation))
            help_text = annotated_help or doc_help.get(name) or self._type_display(typ)
            has_default = param.default is not inspect._empty
            default = None if not has_default else param.default
            dest = f"{dest_prefix}{name}"
            if param.kind is param.VAR_POSITIONAL:
                converter, extra = self._converter_for_type(typ)
                kwargs = {"nargs": "*", "help": help_text, **extra}
                if converter is not None:
                    kwargs["type"] = converter
                parser_or_group.add_argument(name, **kwargs)
                continue
            if typ is bool and has_default:
                self._add_bool_flag(parser_or_group, name, dest, default, help_text)
                continue
            literal_spec = self._literal_spec(typ)
            if literal_spec is not None:
                converter, extra = literal_spec
                self._add_scalar_arg(
                    parser_or_group,
                    name,
                    dest,
                    help_text,
                    converter,
                    extra,
                    has_default=has_default,
                    default=default,
                    required_as_options=required_as_options,
                )
                continue
            converter, extra = self._converter_for_type(typ)
            self._add_scalar_arg(
                parser_or_group,
                name,
                dest,
                help_text,
                converter,
                extra,
                has_default=has_default,
                default=default,
                required_as_options=required_as_options,
            )

    def _add_scalar_arg(
        self,
        parser_or_group: argparse.ArgumentParser | argparse._ArgumentGroup,
        name: str,
        dest: str,
        help_text: str,
        converter: Callable[[str], Any] | None,
        extra: dict[str, Any],
        *,
        has_default: bool,
        default: Any,
        required_as_options: bool,
    ) -> None:
        if has_default:
            kwargs: dict[str, Any] = {
                "dest": dest,
                "default": default,
                "help": help_text,
                "metavar": self._metavar(name),
                **extra,
            }
            if converter is not None:
                kwargs["type"] = converter
            parser_or_group.add_argument(f"--{self._kebab(name)}", **kwargs)
            return
        if required_as_options:
            kwargs = {"dest": dest, "required": True, "help": help_text, "metavar": self._metavar(name), **extra}
            if converter is not None:
                kwargs["type"] = converter
            parser_or_group.add_argument(f"--{self._kebab(name)}", **kwargs)
            return
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

    def _classmethod_init_arg_names(self, method: Callable[..., Any]) -> set[str]:
        sig = inspect.signature(method)
        hints = typing.get_type_hints(method, include_extras=True)
        names: set[str] = set()
        for name, param in sig.parameters.items():
            if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
                continue
            constructor_param = self._constructor_params.get(name)
            if constructor_param is None:
                continue
            typ, _ = self._split_annotated(hints.get(name, param.annotation))
            if typ == constructor_param.typ:
                names.add(name)
        return names

    def _converter_for_type(self, typ: Any) -> tuple[Callable[[str], Any] | None, dict[str, Any]]:
        if typ in self.parsers:
            return self.parsers[typ], {}
        families = _serde_families_for_type(typ)
        if not families:
            raise TypeError(f"{self._type_display(typ)} is not CLI-generatable")
        return self._build_parser(families, typ), {}

    def _split_annotated(self, typ: Any) -> tuple[Any, str | None]:
        if get_origin(typ) is Annotated:
            base, *metadata = get_args(typ)
            for item in metadata:
                if isinstance(item, str):
                    return base, item
            return base, None
        return typ, None

    def _return_type(self, fn: Callable[..., Any]) -> Any:
        hints = typing.get_type_hints(fn, include_extras=True)
        return self._split_annotated(hints.get("return", inspect.signature(fn).return_annotation))[0]

    def _literal_spec(self, typ: Any) -> tuple[Callable[[str], Any] | None, dict[str, Any]] | None:
        if get_origin(typ) is not Literal:
            return None
        choices = list(get_args(typ))
        parser = type(choices[0]) if choices else str
        return parser, {"choices": choices}

    def _build_parser(self, families: tuple[_SerdeFamily, ...], typ: Any) -> Callable[[str], Any]:
        def parse(value: str) -> Any:
            for family in families:
                parser = CLI_FAMILY_PARSERS[family.name]
                try:
                    parsed = parser(value)
                except (argparse.ArgumentTypeError, TypeError, ValueError):
                    continue
                if _matches_any_type(parsed, family.allowed_types):
                    return parsed
            raise argparse.ArgumentTypeError(f"could not parse value as {self._type_display(typ)}")

        return parse

    def _callable_is_generatable(self, fn: Callable[..., Any], *, skip_self: bool) -> bool:
        sig = inspect.signature(fn)
        hints = typing.get_type_hints(fn, include_extras=True)
        for name, param in sig.parameters.items():
            if skip_self and name == "self":
                continue
            if param.kind is param.VAR_KEYWORD:
                return False
            typ, _ = self._split_annotated(hints.get(name, param.annotation))
            has_default = param.default is not inspect._empty
            if param.kind is param.VAR_POSITIONAL:
                try:
                    self._converter_for_type(typ)
                except TypeError:
                    return False
                continue
            if typ is bool:
                if not has_default:
                    try:
                        self._converter_for_type(typ)
                    except TypeError:
                        return False
                continue
            if self._literal_spec(typ) is not None:
                continue
            try:
                self._converter_for_type(typ)
            except TypeError:
                return False
        return True

    def _serialize_result(self, method: Callable[..., Any], result: Any) -> str:
        output_type = self._return_type(method)
        families = tuple(family for family in _serde_families_for_type(output_type) if family.name != "none")
        if not families:
            raise TypeError(f"{self._type_display(output_type)} is not CLI-serializable")
        for family in families:
            if not _matches_any_type(result, family.allowed_types):
                continue
            serializer = CLI_FAMILY_SERIALIZERS.get(family.name)
            if serializer is None:
                raise TypeError(f"{self._type_display(output_type)} is not CLI-serializable")
            try:
                return serializer(result)
            except Exception as exc:
                raise TypeError(
                    f"failed to serialize command result as {self._type_display(output_type)}: {exc}"
                ) from exc
        raise TypeError(f"failed to serialize command result as {self._type_display(output_type)}")

    def _is_public_method_descriptor(self, name: str, member: Any) -> bool:
        if name.startswith("_"):
            return False
        if isinstance(member, (staticmethod, classmethod)):
            return False
        return inspect.isfunction(member)

    def _is_public_root_classmethod_descriptor(self, name: str, member: Any) -> bool:
        return not name.startswith("_") and isinstance(member, classmethod)

    def _kebab(self, name: str) -> str:
        return name.replace("_", "-")

    def _metavar(self, name: str) -> str:
        return name.upper()

    def _type_display(self, typ: Any) -> str:
        if typ is inspect._empty:
            return "value"
        return str(typ).replace("typing.", "")

    def _parse_docstring(self, doc: str) -> _Doc:
        if not doc:
            return _Doc(None, None, {})
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
        return _Doc(summary, "\n".join(desc_lines).strip() or None, param_help)


@dataclass
class _Doc:
    summary: str | None
    description: str | None
    param_help: dict[str, str]


@dataclass(frozen=True)
class _Namespace:
    namespace_type: type[Any]
    help_text: str | None
    doc: _Doc


def cli() -> None:
    raise SystemExit(MethodCLI(Dml, prog="dml").main())
