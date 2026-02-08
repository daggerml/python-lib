from dataclasses import field, is_dataclass
from typing import Any

import pytest

from daggerml._internal.types import DmlRepoError
from daggerml.contrib import api


def test_dagclass_sets_default_entrypoint_metadata():
    @api.dagclass
    class Example:
        pass

    obj = Example()
    assert is_dataclass(Example)
    assert Example.__dict__["__dagclass__"] is True
    assert Example.__dict__["__dagclass_entrypoint__"] == "main"
    assert obj.__dict__["__dagclass_compiled__"] is True


def test_dagclass_sets_custom_entrypoint_metadata():
    @api.dagclass(entrypoint="bar")
    class Example:
        pass

    obj = Example()
    assert Example.__dict__["__dagclass_entrypoint__"] == "bar"
    assert obj.__dict__["__dagclass_compiled__"] is True


def test_dagclass_compiles_exactly_once_per_instance_init():
    @api.dagclass
    class Example:
        pass

    obj = Example()
    assert obj.__dict__["__dagclass_compile_count__"] == 1
    api._compile_dagclass_instance(obj)
    assert obj.__dict__["__dagclass_compile_count__"] == 1


def test_dagclass_behaves_like_dataclass_init_for_fields():
    @api.dagclass
    class Example:
        x: int
        y: int = 2

    obj = Example(5)
    assert obj.x == 5
    assert obj.y == 2


def test_dagclass_calls_user_post_init_transparently():
    @api.dagclass
    class Example:
        x: int
        seen_post_init: bool = False

        def __post_init__(self):
            self.seen_post_init = True

    obj = Example(1)
    assert obj.seen_post_init is True
    assert obj.__dict__["__dagclass_compiled__"] is True


def test_field_default_factory_value_materialized_on_init():
    @api.dagclass
    class Example:
        x: Any = field(default_factory=lambda: 2)

    obj = Example()
    assert obj.x == 2


def test_field_default_factory_value_can_be_overridden():
    @api.dagclass
    class Example:
        x: Any = field(default_factory=lambda: 2)

    obj = Example(9)
    assert obj.x == 9


def test_field_default_factory_returning_dagclass_binds_entrypoint():
    @api.dagclass
    class Other:
        main: api.DelayedRunnable = api.DelayedRunnable(uri="script", adapter="local", sub=None, kwargs={})

    @api.dagclass
    class Host:
        foo: Any = field(default_factory=lambda: Other())

    obj = Host()
    assert isinstance(obj.foo, api.DelayedRunnable)
    assert obj.foo.uri == "script"


def test_direct_class_body_dagclass_assignment_binds_entrypoint():
    @api.dagclass
    class Other:
        main: api.DelayedRunnable = api.DelayedRunnable(uri="script", adapter="local", sub=None, kwargs={})

    @api.dagclass
    class Host:
        foo = Other()

    obj = Host()
    assert isinstance(obj.foo, api.DelayedRunnable)
    assert obj.foo.uri == "script"


def test_dataclasses_field_runs_before_post_init():
    seen = None

    @api.dagclass
    class Example:
        x: Any = field(default_factory=lambda: 2)

        def __post_init__(self):
            nonlocal seen
            seen = self.x

    obj = Example()
    assert seen == 2
    assert obj.x == 2


def test_plain_method_compiles_to_delayed_runnable_with_inferred_prepop():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self, a, b=1):
            return self.x.value() + a.value() * b.value()

    obj = Example()
    assert isinstance(obj.main, api.DelayedRunnable)
    assert obj.main.adapter == "local"
    assert obj.main.uri == "script"
    assert obj.main.kwargs["prepop"] == {"x": api.ref("x")}


def test_method_assignment_shadowing_avoids_inferred_prepop():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self, a, b=1):
            self.x = a
            return self.x.value() + a.value() * b.value()

    obj = Example()
    assert isinstance(obj.main, api.DelayedRunnable)
    assert obj.main.kwargs["prepop"] == {}


def test_method_dependency_orders_compiled_methods_topologically():
    @api.dagclass
    class Example:
        x: Any = 2

        def helper(self):
            return self.x.value()

        def main(self):
            return self.helper()

    obj = Example()
    assert obj.__dagclass_member_order__ == ["x", "helper", "main"]


def test_method_call_dependency_is_inferred_into_prepop():
    @api.dagclass
    class Example:
        x: Any = 2

        def helper(self):
            return self.x.value()

        def main(self):
            return self.helper()

    obj = Example()
    assert isinstance(obj.main, api.DelayedRunnable)
    assert obj.main.kwargs["prepop"] == {"helper": api.ref("helper")}


def test_field_ref_dependency_orders_member_materialization():
    @api.dagclass
    class Example:
        x: Any = api.ref("y")
        y: Any = 2

    obj = Example()
    assert obj.__dagclass_member_order__ == ["y", "x"]


def test_nested_container_ref_dependency_orders_member_materialization():
    @api.dagclass
    class Example:
        x: Any = field(default_factory=lambda: {"items": [api.ref("y")]})
        y: Any = 2

    obj = Example()
    assert obj.__dagclass_member_order__ == ["y", "x"]


def test_explicit_delayed_runnable_ref_dependency_orders_member_materialization():
    @api.dagclass
    class Example:
        x: Any = api.DelayedRunnable(uri="script", adapter="local", sub=None, kwargs={"prepop": {"y": api.ref("y")}})
        y: Any = 2

    obj = Example()
    assert obj.__dagclass_member_order__ == ["y", "x"]


def test_field_ref_to_unknown_member_fails():
    @api.dagclass
    class Example:
        x: Any = api.ref("missing")

    with pytest.raises(DmlRepoError, match="Unknown dagclass member reference: missing"):
        Example()


def test_member_ref_cycle_fails():
    @api.dagclass
    class Example:
        x: Any = api.ref("y")
        y: Any = api.ref("x")

    with pytest.raises(DmlRepoError, match="member dependency cycle"):
        Example()


def test_method_assignment_to_unknown_member_fails():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self, a):
            self.missing = a
            return a

    with pytest.raises(DmlRepoError, match="Unknown dagclass member assignment"):
        Example()


def test_method_getattr_on_self_fails():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self):
            return getattr(self, "x")  # noqa: B009

    with pytest.raises(DmlRepoError, match=r"getattr\(self, \.\.\.\)"):
        Example()


def test_method_item_access_is_escape_hatch_not_inferred_prepop():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self):
            return self["x"].value()

    obj = Example()
    assert isinstance(obj.main, api.DelayedRunnable)
    assert obj.main.kwargs["prepop"] == {}


def test_method_unknown_member_read_fails():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self):
            return self.missing

    with pytest.raises(DmlRepoError, match="Unknown dagclass member reference"):
        Example()


def test_method_assignment_to_compiled_method_name_fails():
    @api.dagclass
    class Example:
        x: Any = 2

        def helper(self):
            return self.x.value()

        def main(self, a):
            self.helper = a
            return a

    with pytest.raises(DmlRepoError, match="Cannot assign to compiled dagclass method"):
        Example()


def test_method_setattr_on_self_fails():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self, a):
            name = "x"
            setattr(self, name, a)
            return a

    with pytest.raises(DmlRepoError, match=r"setattr\(self, \.\.\.\)"):
        Example()


def test_method_hasattr_on_self_fails():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self):
            return hasattr(self, "x")

    with pytest.raises(DmlRepoError, match=r"hasattr\(self, \.\.\.\)"):
        Example()


def test_method_del_self_attr_fails():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self):
            del self.x
            return 1

    with pytest.raises(DmlRepoError, match="del self"):
        Example()


def test_augmented_assignment_counts_as_dependency():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self, a):
            self.x += a.value()
            return self.x

    obj = Example()
    assert obj.main.kwargs["prepop"] == {"x": api.ref("x")}


def test_conditional_assignment_on_one_path_keeps_dependency():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self, a, cond):
            if cond:
                self.x = a
            return self.x.value()

    obj = Example()
    assert obj.main.kwargs["prepop"] == {"x": api.ref("x")}


def test_conditional_assignment_on_all_paths_avoids_dependency():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self, a, b, cond):
            if cond:
                self.x = a
            else:
                self.x = b
            return self.x.value()

    obj = Example()
    assert obj.main.kwargs["prepop"] == {}


def test_method_dependency_cycle_fails():
    @api.dagclass
    class Example:
        def left(self):
            return self.right()

        def right(self):
            return self.left()

    with pytest.raises(DmlRepoError, match="dependency cycle"):
        Example()


def test_reserved_field_name_fails():
    @api.dagclass
    class Example:
        dag: Any = 2

    with pytest.raises(DmlRepoError, match="reserved names: dag"):
        Example()


def test_reserved_method_name_fails():
    @api.dagclass
    class Example:
        def commit(self):
            return 1

    with pytest.raises(DmlRepoError, match="reserved names: commit"):
        Example()


def test_nested_function_definition_fails():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self):
            def helper():
                return self.x

            return helper()

    with pytest.raises(DmlRepoError, match="statement type: FunctionDef"):
        Example()


def test_lambda_capturing_self_fails():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self):
            return (lambda: self.x)()

    with pytest.raises(DmlRepoError, match="dynamic or deferred self-capturing constructs"):
        Example()


def test_comprehension_capturing_self_fails():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self):
            return [self.x for _ in [1]]

    with pytest.raises(DmlRepoError, match="dynamic or deferred self-capturing constructs"):
        Example()


def test_async_method_fails():
    @api.dagclass
    class Example:
        async def main(self):
            return 1

    with pytest.raises(DmlRepoError, match="must be a single function"):
        Example()


def test_staticmethod_member_fails():
    @api.dagclass
    class Example:
        @staticmethod
        def main():
            return 1

    with pytest.raises(DmlRepoError, match="unsupported descriptor type: staticmethod"):
        Example()


def test_explicit_funkify_method_is_not_recompiled():
    @api.dagclass
    class Example:
        x: Any = 2

        @api.funkify(uri="script", adapter="local")
        def main(dag):
            return dag.x.value()

    obj = Example()
    assert isinstance(obj.main, api.DelayedRunnable)
    assert obj.main.kwargs.get("prepop") in (None, {})
