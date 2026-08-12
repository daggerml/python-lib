from __future__ import annotations

import ast
import inspect
from dataclasses import dataclass, fields, is_dataclass, replace
from functools import wraps
from pathlib import Path
from textwrap import dedent
from typing import Any, Callable, Protocol, TypeAlias, TypeVar, cast, dataclass_transform, overload

from daggerml import Runnable
from daggerml import api as core_api
from daggerml.api import DmlRepoError
from daggerml.contrib.codecs import DelayedLoad, DelayedRef, DelayedRunnable

_DAGCLASS_CALL_NODE_NAME = "<dagclass-call>"
_DAGCLASS_RESERVED_NAMES = {"dag", "dml", "argv", "call", "put", "commit"}


def _iter_dagclass_members(instance):
    members = getattr(instance, "__dagclass_members__", None)
    order = getattr(instance, "__dagclass_member_order__", None)
    if not isinstance(members, dict) or not isinstance(order, list):
        raise DmlRepoError("dagclass instance is not compiled")
    for name in order:
        yield name, members[name]


class _DagclassAnalyzer:
    def __init__(self, *, member_names: set[str], method_names: set[str]):
        self.member_names = member_names
        self.method_names = method_names
        self.dependencies: list[str] = []
        self._dep_set: set[str] = set()

    def _add_dependency(self, name: str) -> None:
        if name not in self._dep_set:
            self._dep_set.add(name)
            self.dependencies.append(name)

    def _read_self_name(self, name: str, defined: set[str]) -> None:
        if name not in self.member_names:
            raise DmlRepoError(f"Unknown dagclass member reference: self.{name}")
        if name not in defined:
            self._add_dependency(name)

    def _define_self_name(self, name: str, defined: set[str]) -> set[str]:
        if name not in self.member_names:
            raise DmlRepoError(f"Unknown dagclass member assignment: self.{name}")
        if name in self.method_names:
            raise DmlRepoError(f"Cannot assign to compiled dagclass method: self.{name}")
        return defined | {name}

    def _scan_children(self, node: ast.AST, defined: set[str]) -> set[str]:
        for child in ast.iter_child_nodes(node):
            defined = self._scan(child, defined)
        return defined

    def _scan_target(self, target: ast.AST, defined: set[str]) -> set[str]:
        if isinstance(target, ast.Attribute):
            if isinstance(target.value, ast.Name) and target.value.id == "self":
                return self._define_self_name(target.attr, defined)
            return self._scan_children(target, defined)
        if isinstance(target, (ast.Tuple, ast.List)):
            for item in target.elts:
                defined = self._scan_target(item, defined)
        return defined

    def _scan_block(self, statements: list[ast.stmt], defined: set[str]) -> set[str]:
        for statement in statements:
            defined = self._scan(statement, defined)
        return defined

    def _scan(self, node: ast.AST, defined: set[str]) -> set[str]:
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id == "self":
                if isinstance(node.ctx, ast.Load):
                    self._read_self_name(node.attr, defined)
                return defined
        if isinstance(node, ast.Assign):
            defined = self._scan(node.value, defined)
            for target in node.targets:
                defined = self._scan_target(target, defined)
            return defined
        if isinstance(node, ast.AnnAssign):
            if node.value is not None:
                defined = self._scan(node.value, defined)
            return self._scan_target(node.target, defined)
        if isinstance(node, ast.AugAssign):
            if (
                isinstance(node.target, ast.Attribute)
                and isinstance(node.target.value, ast.Name)
                and node.target.value.id == "self"
            ):
                self._read_self_name(node.target.attr, defined)
            defined = self._scan(node.value, defined)
            return self._scan_target(node.target, defined)
        if isinstance(node, ast.If):
            defined = self._scan(node.test, defined)
            body = self._scan_block(node.body, set(defined))
            orelse = self._scan_block(node.orelse, set(defined))
            return body & orelse
        if isinstance(node, (ast.For, ast.AsyncFor, ast.While)):
            self._scan_children(node, set(defined))
            return defined
        return self._scan_children(node, defined)

    def analyze(self, fn: ast.FunctionDef) -> list[str]:
        self._scan_block(fn.body, set())
        return list(self.dependencies)


def _analyze_dagclass_method(*, cls, method_name: str, method, member_names: set[str], method_names: set[str]):
    try:
        source = dedent(inspect.getsource(method))
    except (OSError, TypeError) as e:
        raise DmlRepoError(f"Failed to inspect dagclass method source for {cls.__name__}.{method_name}: {e}") from e
    module = ast.parse(source)
    if len(module.body) != 1 or not isinstance(module.body[0], ast.FunctionDef):
        raise DmlRepoError(f"dagclass method source for {cls.__name__}.{method_name} must be a single function")
    fn = module.body[0]
    if not fn.args.args or fn.args.args[0].arg != "self":
        raise DmlRepoError(f"dagclass method {cls.__name__}.{method_name} must declare self as first parameter")
    analyzer = _DagclassAnalyzer(member_names=member_names, method_names=method_names)
    return analyzer.analyze(fn), fn.decorator_list


def _compile_plain_dagclass_method(*, cls, method_name: str, method, member_names: set[str], method_names: set[str]):
    dependencies, decorators = _analyze_dagclass_method(
        cls=cls,
        method_name=method_name,
        method=method,
        member_names=member_names,
        method_names=method_names,
    )
    if decorators:
        raise DmlRepoError(f"dagclass method {cls.__name__}.{method_name} has unsupported decorators")
    delayed = funkify(method, uri="script", adapter="local", prepop={name: ref(name) for name in dependencies})
    return delayed, dependencies


def _dagclass_decorated_method(value: Any) -> Callable[..., Any] | None:
    current = value
    while isinstance(current, DelayedRunnable):
        fn = current.kwargs.get("fn")
        if callable(fn):
            params = list(inspect.signature(fn).parameters.values())
            return fn if params and params[0].name == "self" else None
        current = current.sub
    return None


def _add_dagclass_prepop(value: DelayedRunnable, dependencies: list[str]) -> DelayedRunnable:
    if isinstance(value.sub, DelayedRunnable):
        return replace(value, sub=_add_dagclass_prepop(value.sub, dependencies))
    kwargs = dict(value.kwargs)
    kwargs["prepop"] = {**kwargs.get("prepop", {}), **{name: ref(name) for name in dependencies}}
    return replace(value, kwargs=kwargs)


def _collect_member_dependencies(value: Any, member_names: set[str]) -> set[str]:
    deps: set[str] = set()

    def visit(obj: Any) -> None:
        if isinstance(obj, DelayedRef):
            if obj.name not in member_names:
                raise DmlRepoError(f"Unknown dagclass member reference: {obj.name}")
            deps.add(obj.name)
            return
        if isinstance(obj, DelayedLoad):
            return
        if isinstance(obj, DelayedRunnable):
            visit(obj.sub)
            visit(obj.kwargs)
            return
        if isinstance(obj, Runnable):
            visit(obj.sub)
            visit(obj.kwargs)
            return
        if isinstance(obj, dict):
            for key, value in obj.items():
                visit(key)
                visit(value)
            return
        if isinstance(obj, (list, tuple, set, frozenset)):
            for item in obj:
                visit(item)
            return

    visit(value)
    return deps


def _toposort_members(member_deps: dict[str, set[str]], order_hint: list[str]) -> list[str]:
    ordered: list[str] = []
    temp: set[str] = set()
    done: set[str] = set()

    def visit(name: str) -> None:
        if name in done:
            return
        if name in temp:
            raise DmlRepoError(f"dagclass member dependency cycle detected at: {name}")
        temp.add(name)
        for dep in sorted(
            member_deps.get(name, set()),
            key=lambda item: order_hint.index(item) if item in order_hint else len(order_hint),
        ):
            visit(dep)
        temp.remove(name)
        done.add(name)
        ordered.append(name)

    for name in order_hint:
        visit(name)
    if set(ordered) != set(order_hint):
        raise DmlRepoError("dagclass member ordering is incomplete or inconsistent")
    return ordered


def _bind_dagclass_member(value, members: dict[str, Any]):
    if isinstance(value, DelayedRef):
        if value.name not in members:
            raise DmlRepoError(f"Unknown dagclass member reference: {value.name}")
        return members[value.name]
    if isinstance(value, DelayedRunnable):
        return replace(
            value,
            sub=_bind_dagclass_member(value.sub, members),
            kwargs={key: _bind_dagclass_member(item, members) for key, item in value.kwargs.items()},
        )
    if isinstance(value, Runnable):
        return replace(
            value,
            sub=_bind_dagclass_member(value.sub, members),
            kwargs={key: _bind_dagclass_member(item, members) for key, item in value.kwargs.items()},
        )
    if isinstance(value, dict):
        return {key: _bind_dagclass_member(item, members) for key, item in value.items()}
    if isinstance(value, list):
        return [_bind_dagclass_member(item, members) for item in value]
    if isinstance(value, tuple):
        return tuple(_bind_dagclass_member(item, members) for item in value)
    if isinstance(value, set):
        return {_bind_dagclass_member(item, members) for item in value}
    if isinstance(value, frozenset):
        return frozenset(_bind_dagclass_member(item, members) for item in value)
    return value


def _bind_dagclass_value(value):
    if getattr(value.__class__, "__dagclass__", False):
        entrypoint = getattr(value.__class__, "__dagclass_entrypoint__", "main")
        if not hasattr(value, entrypoint):
            raise DmlRepoError(f"Dagclass instance missing configured entrypoint: {entrypoint}")
        return value.__dagclass_members__[entrypoint]
    return value


FunkifyInput: TypeAlias = Callable[..., Any] | Runnable | DelayedRunnable
DagclassType = TypeVar("DagclassType", bound=type[Any])


class _DagclassProtocol(Protocol):
    __dagclass__: bool
    __dagclass_entrypoint__: str
    __dagclass_wrapped_init__: bool

    def __init__(self, *args: Any, **kwargs: Any) -> None: ...


def is_node_like(x: object) -> bool:
    """Return True if x is a Node or any Delayed* type (DelayedRef, DelayedLoad, DelayedRunnable)."""
    return isinstance(x, (core_api.Node, DelayedRef, DelayedLoad, DelayedRunnable))


def ref(name: str) -> DelayedRef:
    return DelayedRef(name)


def load(dagname: str, nodename: str | None = None) -> DelayedLoad:
    return DelayedLoad(dagname=dagname, nodename=nodename)


def _compile_dagclass_instance(instance) -> None:
    if getattr(instance, "__dagclass_compiled__", False):
        return

    attributes: dict[str, Any] = {}
    attribute_order: list[str] = []
    method_defs: dict[str, tuple[Any, DelayedRunnable | None]] = {}
    field_names = {f.name for f in fields(instance)}

    for f in fields(instance):
        current = getattr(instance, f.name)
        bound = _bind_dagclass_value(current)
        if bound is not current:
            setattr(instance, f.name, bound)
        attributes[f.name] = getattr(instance, f.name)
        attribute_order.append(f.name)

    for name, class_value in instance.__class__.__dict__.items():
        if name.startswith("_"):
            continue
        if name in field_names:
            continue
        if isinstance(class_value, (staticmethod, classmethod, property)):
            raise DmlRepoError(f"dagclass member {name} uses unsupported descriptor type: {type(class_value).__name__}")
        if inspect.isfunction(class_value):
            method_defs[name] = (class_value, None)
            continue
        decorated_method = _dagclass_decorated_method(class_value)
        if decorated_method is not None:
            method_defs[name] = (decorated_method, class_value)
            continue
        if callable(class_value):
            raise DmlRepoError(f"dagclass member {name} uses unsupported callable type: {type(class_value).__name__}")
        if name in instance.__dict__:
            attributes[name] = getattr(instance, name)
            attribute_order.append(name)
            continue
        bound = _bind_dagclass_value(class_value)
        if bound is not class_value:
            setattr(instance, name, bound)
        attributes[name] = getattr(instance, name)
        attribute_order.append(name)

    member_names = set(attributes.keys()) | set(method_defs.keys())
    method_names = set(method_defs.keys())
    reserved = sorted(member_names & _DAGCLASS_RESERVED_NAMES)
    if reserved:
        bad = ", ".join(reserved)
        raise DmlRepoError(f"dagclass uses reserved names: {bad}")
    attribute_deps: dict[str, set[str]] = {}
    attribute_names = set(attributes)
    for name, value in attributes.items():
        attribute_deps[name] = _collect_member_dependencies(value, attribute_names)

    members: dict[str, Any] = {}
    order: list[str] = []
    for name in _toposort_members(attribute_deps, attribute_order):
        bound = _bind_dagclass_member(attributes[name], members)
        setattr(instance, name, bound)
        members[name] = bound
        order.append(name)

    compiled_methods: dict[str, Any] = {}
    method_deps: dict[str, set[str]] = {}
    for name, (method, decorated) in method_defs.items():
        if decorated is None:
            compiled, deps = _compile_plain_dagclass_method(
                cls=instance.__class__,
                method_name=name,
                method=method,
                member_names=member_names,
                method_names=method_names,
            )
        else:
            deps, _decorators = _analyze_dagclass_method(
                cls=instance.__class__,
                method_name=name,
                method=method,
                member_names=member_names,
                method_names=method_names,
            )
            compiled = _add_dagclass_prepop(decorated, deps)
        compiled_methods[name] = compiled
        method_deps[name] = set(deps) & method_names

    method_order = _toposort_members(method_deps, list(method_defs))
    for name in method_order:
        compiled = _bind_dagclass_member(compiled_methods[name], members)
        setattr(instance, name, compiled)
        members[name] = compiled
        order.append(name)

    instance.__dagclass_members__ = members
    instance.__dagclass_member_order__ = order
    instance.__dagclass_compiled__ = True


@dataclass_transform()
@overload
def dagclass(
    _cls: None = None, *, entrypoint: str = "main", **dataclass_kwargs: Any
) -> Callable[[DagclassType], DagclassType]: ...
@overload
def dagclass(_cls: DagclassType, *, entrypoint: str = "main", **dataclass_kwargs: Any) -> DagclassType: ...
def dagclass(
    _cls: DagclassType | None = None, *, entrypoint: str = "main", **dataclass_kwargs: Any
) -> Callable[[DagclassType], DagclassType] | DagclassType:
    def wrap(cls: DagclassType) -> DagclassType:
        if not is_dataclass(cls):
            cls = dataclass(cls, **dataclass_kwargs)
        elif dataclass_kwargs:
            bad = ", ".join(sorted(dataclass_kwargs.keys()))
            raise DmlRepoError(f"api.dagclass dataclass kwargs not allowed on pre-dataclass class: {bad}")
        cls = cast(DagclassType, cls)
        dagclass_cls = cast(_DagclassProtocol, cls)
        dagclass_cls.__dagclass__ = True
        dagclass_cls.__dagclass_entrypoint__ = entrypoint
        if getattr(dagclass_cls, "__dagclass_wrapped_init__", False):
            return cls
        original_init = dagclass_cls.__init__

        @wraps(original_init)
        def _dagclass_init(self, *args, **kwargs):
            original_init(self, *args, **kwargs)
            _compile_dagclass_instance(self)

        dagclass_cls.__init__ = _dagclass_init
        dagclass_cls.__dagclass_wrapped_init__ = True
        return cls

    if _cls is None:
        return wrap
    return wrap(_cls)


def _default_run_name(instance) -> str:
    module = __import__(instance.__class__.__module__, fromlist=["__name__"])
    if not getattr(module, "__file__", None):
        return f"{instance.__class__.__module__}::{instance.__class__.__name__}"
    module_file = Path(module.__file__).resolve()
    repo_root = None
    for parent in (module_file.parent, *module_file.parents):
        if (parent / ".git").exists():
            repo_root = parent
            break
    base = repo_root if repo_root is not None else Path.cwd().resolve()
    try:
        rel = module_file.relative_to(base)
    except ValueError:
        rel = module_file
    rel_no_ext = rel.with_suffix("").as_posix()
    return f"{rel_no_ext}::{instance.__class__.__name__}"


def run(instance, *args, name: str | None = None, entrypoint: str | None = None, **kwargs):
    if not getattr(instance.__class__, "__dagclass__", False):
        raise DmlRepoError("api.run instance is not a dagclass instance")
    if not getattr(instance, "__dagclass_compiled__", False):
        raise DmlRepoError("api.run instance is not compiled")
    entry = entrypoint or getattr(instance.__class__, "__dagclass_entrypoint__", "main")
    if not hasattr(instance, entry):
        raise DmlRepoError(f"api.run entrypoint not found: {entry}")
    fn = instance.__dagclass_members__.get(entry)
    if not isinstance(fn, DelayedRunnable):
        raise DmlRepoError("api.run entrypoint must be DelayedRunnable")
    run_name = name or _default_run_name(instance)
    dml = core_api.get_default_dml()
    dag = core_api.new(dml=dml, name=run_name, message=run_name)
    for member_name, member_value in _iter_dagclass_members(instance):
        dag.put(member_value, name=member_name)
    result = dag.call(fn, *args, name=_DAGCLASS_CALL_NODE_NAME, **kwargs)
    dag.commit(result)


@overload
def funkify(
    sub_or_fn: None = None, *, adapter: str = "local", uri: str = "script", **kwargs: Any
) -> Callable[[FunkifyInput], DelayedRunnable]: ...
@overload
def funkify(
    sub_or_fn: Callable[..., Any], *, adapter: str = "local", uri: str = "script", **kwargs: Any
) -> DelayedRunnable: ...
@overload
def funkify(
    sub_or_fn: Runnable | DelayedRunnable, *, adapter: str = "local", uri: str = "script", **kwargs: Any
) -> DelayedRunnable: ...
def funkify(
    sub_or_fn: FunkifyInput | None = None, *, adapter: str = "local", uri: str = "script", **kwargs: Any
) -> Callable[[FunkifyInput], DelayedRunnable] | DelayedRunnable:
    def _make(value: FunkifyInput) -> DelayedRunnable:
        if callable(value):
            if "fn" in kwargs:
                raise DmlRepoError("Unknown kwarg: fn")
            return DelayedRunnable(uri=uri, adapter=adapter, sub=None, kwargs={"fn": value, **kwargs})
        if isinstance(value, (Runnable, DelayedRunnable)):
            return DelayedRunnable(uri=uri, adapter=adapter, sub=value, kwargs=dict(kwargs))
        raise DmlRepoError(f"Invalid funkify input: {type(value).__name__}")

    if sub_or_fn is None:
        return _make
    return _make(sub_or_fn)
