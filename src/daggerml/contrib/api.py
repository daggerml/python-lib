from __future__ import annotations

import ast
import inspect
import linecache
from dataclasses import dataclass, fields, is_dataclass
from functools import wraps
from pathlib import Path
from textwrap import dedent
from typing import Any, Callable, Protocol, TypeAlias, TypeVar, cast, overload

from daggerml import Runnable
from daggerml import api as core_api
from daggerml.api import DmlRepoError
from daggerml.contrib.codecs import DelayedLoad, DelayedRef, DelayedRunnable

try:
    from typing import dataclass_transform
except ImportError:
    from typing_extensions import dataclass_transform

_DAGCLASS_CALL_NODE_NAME = "<dagclass-call>"
_DAGCLASS_RESERVED_NAMES = {"dag", "dml", "argv", "call", "put", "commit"}


def _iter_dagclass_members(instance):
    members = getattr(instance, "__dagclass_members__", None)
    order = getattr(instance, "__dagclass_member_order__", None)
    if not isinstance(members, dict) or not isinstance(order, list):
        raise DmlRepoError("dagclass instance is not compiled")
    for name in order:
        yield name, members[name]


class _DagclassAnalyzer(ast.NodeVisitor):
    def __init__(self, *, member_names: set[str], method_names: set[str]):
        self.member_names = member_names
        self.method_names = method_names
        self.dependencies: list[str] = []
        self._dep_set: set[str] = set()

    def _add_dependency(self, name: str) -> None:
        if name not in self._dep_set:
            self._dep_set.add(name)
            self.dependencies.append(name)

    def _unsupported(self, msg: str) -> None:
        raise DmlRepoError(msg)

    def _read_self_name(self, name: str, assigned: set[str]) -> None:
        if name not in self.member_names:
            raise DmlRepoError(f"Unknown dagclass member reference: self.{name}")
        if name not in assigned:
            self._add_dependency(name)

    def _assign_self_name(self, name: str) -> None:
        if name not in self.member_names:
            raise DmlRepoError(f"Unknown dagclass member assignment: self.{name}")
        if name in self.method_names:
            raise DmlRepoError(f"Cannot assign to compiled dagclass method: self.{name}")

    def _visit_expr(self, node: ast.AST, assigned: set[str]) -> None:
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id == "self":
                if isinstance(node.ctx, ast.Load):
                    self._read_self_name(node.attr, assigned)
                    return
                if isinstance(node.ctx, ast.Del):
                    self._unsupported("dagclass methods do not support del self.<name>")
                    return
            self._visit_expr(node.value, assigned)
            return
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in {"getattr", "setattr", "hasattr"}:
                if node.args and isinstance(node.args[0], ast.Name) and node.args[0].id == "self":
                    self._unsupported(f"dagclass methods do not support {node.func.id}(self, ...)")
            self._visit_expr(node.func, assigned)
            for arg in node.args:
                self._visit_expr(arg, assigned)
            for kw in node.keywords:
                if kw.value is not None:
                    self._visit_expr(kw.value, assigned)
            return
        if isinstance(node, ast.Subscript):
            self._visit_expr(node.value, assigned)
            self._visit_expr(node.slice, assigned)
            return
        if isinstance(
            node,
            (
                ast.ListComp,
                ast.SetComp,
                ast.DictComp,
                ast.GeneratorExp,
                ast.Lambda,
                ast.Yield,
                ast.YieldFrom,
                ast.Await,
            ),
        ):
            self._unsupported("dagclass methods do not support dynamic or deferred self-capturing constructs")
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.expr):
                self._visit_expr(child, assigned)

    def _assign_target(self, target: ast.AST) -> set[str]:
        names: set[str] = set()
        if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name) and target.value.id == "self":
            self._assign_self_name(target.attr)
            names.add(target.attr)
        elif isinstance(target, (ast.Tuple, ast.List)):
            for elt in target.elts:
                names.update(self._assign_target(elt))
        elif isinstance(target, ast.Subscript):
            if isinstance(target.value, ast.Name) and target.value.id == "self":
                return names
            self._visit_expr(target.value, set())
            self._visit_expr(target.slice, set())
        return names

    def _visit_stmt_list(self, stmts: list[ast.stmt], assigned_in: set[str]) -> set[str]:
        assigned = set(assigned_in)
        for stmt in stmts:
            assigned = self._visit_stmt(stmt, assigned)
        return assigned

    def _visit_stmt(self, stmt: ast.stmt, assigned: set[str]) -> set[str]:
        if isinstance(stmt, ast.Return):
            if stmt.value is not None:
                self._visit_expr(stmt.value, assigned)
            return set(assigned)
        if isinstance(stmt, ast.Expr):
            self._visit_expr(stmt.value, assigned)
            return set(assigned)
        if isinstance(stmt, ast.Assign):
            self._visit_expr(stmt.value, assigned)
            out = set(assigned)
            for target in stmt.targets:
                out.update(self._assign_target(target))
            return out
        if isinstance(stmt, ast.AnnAssign):
            if stmt.value is not None:
                self._visit_expr(stmt.value, assigned)
            out = set(assigned)
            out.update(self._assign_target(stmt.target))
            return out
        if isinstance(stmt, ast.AugAssign):
            if (
                isinstance(stmt.target, ast.Attribute)
                and isinstance(stmt.target.value, ast.Name)
                and stmt.target.value.id == "self"
            ):
                self._read_self_name(stmt.target.attr, assigned)
            self._visit_expr(stmt.target, assigned)
            self._visit_expr(stmt.value, assigned)
            out = set(assigned)
            out.update(self._assign_target(stmt.target))
            return out
        if isinstance(stmt, ast.If):
            self._visit_expr(stmt.test, assigned)
            body_out = self._visit_stmt_list(stmt.body, set(assigned))
            orelse_out = self._visit_stmt_list(stmt.orelse, set(assigned))
            return body_out & orelse_out
        if isinstance(stmt, (ast.For, ast.AsyncFor, ast.While)):
            if isinstance(stmt, ast.For):
                self._visit_expr(stmt.iter, assigned)
                self._assign_target(stmt.target)
            elif isinstance(stmt, ast.AsyncFor):
                self._unsupported("dagclass methods do not support async for")
            else:
                self._visit_expr(stmt.test, assigned)
            self._visit_stmt_list(stmt.body, set(assigned))
            orelse_out = self._visit_stmt_list(stmt.orelse, set(assigned))
            return set(assigned) & orelse_out
        if isinstance(stmt, (ast.With, ast.AsyncWith)):
            if isinstance(stmt, ast.AsyncWith):
                self._unsupported("dagclass methods do not support async with")
            for item in stmt.items:
                self._visit_expr(item.context_expr, assigned)
                if item.optional_vars is not None:
                    self._assign_target(item.optional_vars)
            return self._visit_stmt_list(stmt.body, set(assigned))
        if isinstance(stmt, ast.Delete):
            for target in stmt.targets:
                if (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == "self"
                ):
                    self._unsupported("dagclass methods do not support del self.<name>")
            return set(assigned)
        if isinstance(
            stmt,
            (
                ast.FunctionDef,
                ast.AsyncFunctionDef,
                ast.ClassDef,
                ast.Try,
                ast.TryStar,
                ast.Raise,
                ast.Match,
                ast.Assert,
                ast.Global,
                ast.Nonlocal,
            ),
        ):
            self._unsupported(f"dagclass methods do not support statement type: {type(stmt).__name__}")
        return set(assigned)

    def analyze(self, fn: ast.FunctionDef) -> list[str]:
        self._visit_stmt_list(fn.body, set())
        return list(self.dependencies)


def _make_self_helper_class() -> ast.ClassDef:
    return cast(
        ast.ClassDef,
        ast.parse(
            "class _DagclassSelf:\n"
            "    def __getitem__(self, key):\n"
            "        return getattr(self, key)\n"
            "    def __setitem__(self, key, value):\n"
            "        setattr(self, key, value)\n"
        ).body[0],
    )


def _load_self_attr(name: str) -> ast.Assign:
    return ast.Assign(
        targets=[ast.Attribute(value=ast.Name(id="self", ctx=ast.Load()), attr=name, ctx=ast.Store())],
        value=ast.Subscript(
            value=ast.Name(id="dag", ctx=ast.Load()),
            slice=ast.Constant(value=name),
            ctx=ast.Load(),
        ),
    )


def _function_from_source(source: str, fn_name: str):
    filename = f"<dagclass:{fn_name}:{abs(hash(source))}>"
    lines = [line + "\n" for line in source.splitlines()]
    linecache.cache[filename] = (len(source), None, lines, filename)
    namespace: dict[str, Any] = {}
    exec(compile(source, filename, "exec"), namespace, namespace)
    return namespace[fn_name]


def _compile_plain_dagclass_method(*, cls, method_name: str, method, member_names: set[str], method_names: set[str]):
    try:
        source = dedent(inspect.getsource(method))
    except (OSError, TypeError) as e:
        raise DmlRepoError(f"Failed to inspect dagclass method source for {cls.__name__}.{method_name}: {e}") from e

    module = ast.parse(source)
    if len(module.body) != 1 or not isinstance(module.body[0], ast.FunctionDef):
        raise DmlRepoError(f"dagclass method source for {cls.__name__}.{method_name} must be a single function")
    fn = module.body[0]
    if fn.decorator_list:
        raise DmlRepoError(f"dagclass method {cls.__name__}.{method_name} has unsupported decorators")
    if not fn.args.args or fn.args.args[0].arg != "self":
        raise DmlRepoError(f"dagclass method {cls.__name__}.{method_name} must declare self as first parameter")

    analyzer = _DagclassAnalyzer(member_names=member_names, method_names=method_names)
    dependencies = analyzer.analyze(fn)

    compiled_fn = ast.FunctionDef(
        name=method_name,
        args=ast.arguments(
            posonlyargs=[],
            args=[ast.arg(arg="dag", annotation=None), *fn.args.args[1:]],
            vararg=fn.args.vararg,
            kwonlyargs=fn.args.kwonlyargs,
            kw_defaults=fn.args.kw_defaults,
            kwarg=fn.args.kwarg,
            defaults=fn.args.defaults,
        ),
        body=[
            _make_self_helper_class(),
            ast.Assign(
                targets=[ast.Name(id="self", ctx=ast.Store())],
                value=ast.Call(func=ast.Name(id="_DagclassSelf", ctx=ast.Load()), args=[], keywords=[]),
            ),
            *[_load_self_attr(name) for name in dependencies],
            *fn.body,
        ],
        decorator_list=[],
        returns=fn.returns,
        type_comment=fn.type_comment,
    )
    ast.fix_missing_locations(compiled_fn)
    compiled_source = ast.unparse(compiled_fn) + "\n"
    compiled_callable = _function_from_source(compiled_source, method_name)
    delayed = funkify(
        compiled_callable, uri="script", adapter="local", prepop={name: ref(name) for name in dependencies}
    )
    return delayed, dependencies


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


def _bind_dagclass_value(value):
    if getattr(value.__class__, "__dagclass__", False):
        entrypoint = getattr(value.__class__, "__dagclass_entrypoint__", "main")
        if not hasattr(value, entrypoint):
            raise DmlRepoError(f"Dagclass instance missing configured entrypoint: {entrypoint}")
        return getattr(value, entrypoint)
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

    members: dict[str, Any] = {}
    declaration_order: list[str] = []
    method_defs: dict[str, Any] = {}
    field_names = {f.name for f in fields(instance)}

    for f in fields(instance):
        current = getattr(instance, f.name)
        bound = _bind_dagclass_value(current)
        if bound is not current:
            setattr(instance, f.name, bound)
        members[f.name] = getattr(instance, f.name)
        declaration_order.append(f.name)

    for name, class_value in instance.__class__.__dict__.items():
        if name.startswith("_"):
            continue
        if name in field_names:
            continue
        if isinstance(class_value, (staticmethod, classmethod, property)):
            raise DmlRepoError(f"dagclass member {name} uses unsupported descriptor type: {type(class_value).__name__}")
        if inspect.isfunction(class_value):
            method_defs[name] = class_value
            continue
        if callable(class_value):
            raise DmlRepoError(f"dagclass member {name} uses unsupported callable type: {type(class_value).__name__}")
        if name in instance.__dict__:
            members[name] = getattr(instance, name)
            declaration_order.append(name)
            continue
        bound = _bind_dagclass_value(class_value)
        if bound is not class_value:
            setattr(instance, name, bound)
        members[name] = getattr(instance, name)
        declaration_order.append(name)

    member_names = set(members.keys()) | set(method_defs.keys())
    method_names = set(method_defs.keys())
    reserved = sorted(member_names & _DAGCLASS_RESERVED_NAMES)
    if reserved:
        bad = ", ".join(reserved)
        raise DmlRepoError(f"dagclass uses reserved names: {bad}")
    compiled_methods: dict[str, Any] = {}
    for name, method in method_defs.items():
        compiled, deps = _compile_plain_dagclass_method(
            cls=instance.__class__,
            method_name=name,
            method=method,
            member_names=member_names,
            method_names=method_names,
        )
        compiled_methods[name] = compiled
    for name, compiled in compiled_methods.items():
        setattr(instance, name, compiled)
        members[name] = compiled
        declaration_order.append(name)

    member_deps: dict[str, set[str]] = {}
    for name, value in members.items():
        deps = _collect_member_dependencies(value, set(members.keys()))
        if name in deps:
            raise DmlRepoError(f"dagclass member dependency cycle detected at: {name}")
        member_deps[name] = deps

    order = _toposort_members(member_deps, declaration_order)

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
    fn = getattr(instance, entry)
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
