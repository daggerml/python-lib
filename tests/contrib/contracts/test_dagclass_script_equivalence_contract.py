from __future__ import annotations

import pytest

from daggerml import Runnable, Uri
from daggerml._core import DmlRepoError
from daggerml.contrib import api
from daggerml.contrib.codecs import DelayedRef, DelayedRunnable
from daggerml.contrib.executors.script import ScriptExecutor


@api.funkify
def preprocess(dag, raw):
    return raw


@api.funkify
def summarize(dag, values):
    return values


@api.dagclass
class DagclassPipeline:
    preprocess = preprocess
    summarize = summarize

    def main(self, raw):
        return self.summarize(self.preprocess(raw))


@api.dagclass
class ChildPipeline:
    preprocess = preprocess
    summarize = summarize

    def main(self, raw):
        return self.summarize(self.preprocess(raw))


@api.dagclass
class ParentPipeline:
    child = ChildPipeline()

    def main(self, raw):
        return self.child(raw)


@api.dagclass
class ComprehensionPipeline:
    summarize = summarize

    def main(self, raw_dict):
        return {name: self.summarize(raw) for name, raw in raw_dict.items()}


@api.dagclass
class DefinedMemberPipeline:
    value = None

    def main(self, raw):
        self.value = raw
        return self.value


@api.dagclass
class NestedComprehensionPipeline:
    child = ChildPipeline()

    def main(self, raw_dict):
        return {name: self.child(raw) for name, raw in raw_dict.items()}


@api.dagclass
class ConfiguredPipeline:
    offset: int
    scale: int

    def adjust(self, raw):
        return self.offset.value() + raw.value()

    def main(self, raw):
        return self.adjust(raw) * self.scale.value()


@api.dagclass
class DecoratedMethodPipeline:
    offset: int
    scale: int
    image: str

    @api.funkify(name="docker", kwargs={"image": api.ref("image")})
    def main(self, raw):
        return (self.offset.value() + raw.value()) * self.scale.value()


@api.funkify(prepop={"offset": api.ref("offset")})
def configured_funk(dag, raw):
    return dag.put(offset.value() + raw.value())  # noqa: F821 - injected by prepop


@api.dagclass
class ExternalFunkPipeline:
    offset: int
    adjusted = configured_funk

    def main(self, raw):
        return self.adjusted(raw)


@api.funkify(prepop={"missing": api.ref("missing")})
def invalid_funk(dag, raw):
    return raw


@api.dagclass
class InvalidExternalFunkPipeline:
    invalid = invalid_funk


@api.dagclass
class CyclicPipeline:
    def left(self, raw):
        return self.right(raw)

    def right(self, raw):
        return self.left(raw)


@api.dagclass
class DagOperationPipeline:
    def main(self, raw):
        self.put(raw, name="output")
        return self.argv


@api.dagclass
class DagNamedMemberPipeline:
    dag = preprocess

    def main(self, raw):
        return self.dag(raw)


@api.dagclass
class UndeclaredAssignmentPipeline:
    def main(self, raw):
        self.output = raw
        return self.output


@api.dagclass
class ConditionalAssignmentPipeline:
    output = None

    def main(self, raw):
        if raw:
            self.output = raw
        return self.output


@api.dagclass
class LaterAssignmentPipeline:
    output = None

    def main(self, raw):
        value = self.output
        self.output = raw
        return value


@api.dagclass
class UnknownReferencePipeline:
    def main(self, raw):
        return self.missing(raw)


@api.dagclass
class ReservedAssignmentPipeline:
    def main(self, raw):
        self.put = raw
        return raw


@api.dagclass
class ItemAccessPipeline:
    transform = preprocess

    def main(self, raw):
        return self["transform"](raw)


@api.dagclass
class ItemAssignmentPipeline:
    output = 1

    def main(self, raw):
        self["output"] = raw
        return self.output


@api.funkify(prepop={"summarize": api.ref("summarize"), "preprocess": api.ref("preprocess")})
def main(self, raw):
    return self.summarize(self.preprocess(raw))


def test_contrib_dagclass_001__matching_funkify_definition_renders_identical_script():
    dagclass_main = DagclassPipeline().main

    assert isinstance(dagclass_main, DelayedRunnable)
    dagclass_kwargs, dagclass_script = ScriptExecutor._script_kwargs(dagclass_main.kwargs)
    funkify_kwargs, funkify_script = ScriptExecutor._script_kwargs(main.kwargs)

    assert dagclass_kwargs["fn_name"] == funkify_kwargs["fn_name"]
    assert dagclass_script == funkify_script


def test_contrib_dagclass_002__nested_instance_embeds_child_member_graph():
    child = ParentPipeline().__dagclass_members__["child"]

    assert isinstance(child, DelayedRunnable)
    assert isinstance(child.kwargs["prepop"]["preprocess"], DelayedRunnable)
    assert isinstance(child.kwargs["prepop"]["summarize"], DelayedRunnable)


def test_contrib_dagclass_003__comprehension_collects_member_dependency():
    main = ComprehensionPipeline().main

    assert main.kwargs["prepop"] == {"summarize": summarize}


def test_contrib_dagclass_004__member_defined_before_read_is_not_dependency():
    main = DefinedMemberPipeline().main

    assert main.kwargs["prepop"] == {}


def test_contrib_dagclass_005__comprehension_captures_nested_dagclass():
    pipeline = NestedComprehensionPipeline()
    main = pipeline.main

    assert main.kwargs["prepop"] == {"child": pipeline.__dagclass_members__["child"]}
    assert isinstance(pipeline.__dagclass_members__["child"], DelayedRunnable)


def test_contrib_dagclass_006__direct_method_closes_over_constructor_attributes_and_methods():
    pipeline = ConfiguredPipeline(offset=1, scale=2)

    assert pipeline.__dagclass_compiled__ is True
    assert pipeline.main.kwargs["prepop"] == {"adjust": pipeline.adjust, "scale": 2}
    assert pipeline.adjust.kwargs["prepop"] == {"offset": 1}


def test_contrib_dagclass_007__external_funk_ref_binds_to_namespace_attribute():
    pipeline = ExternalFunkPipeline(offset=7)

    assert pipeline.adjusted.kwargs["prepop"] == {"offset": 7}
    assert pipeline.main.kwargs["prepop"] == {"adjusted": pipeline.adjusted}


def test_contrib_dagclass_008__concrete_runnable_refs_bind_recursively():
    @api.dagclass
    class ConcreteRunnablePipeline:
        config: int
        wrapped = Runnable(target=Uri("target"), kwargs={"config": DelayedRef("config")})

        def main(self, raw):
            return raw

    pipeline = ConcreteRunnablePipeline(config=11)

    assert pipeline.wrapped.kwargs == {"config": 11}


def test_contrib_dagclass_009__unknown_external_funk_ref_fails_at_instantiation():
    with pytest.raises(DmlRepoError, match="Unknown dagclass member reference: missing"):
        InvalidExternalFunkPipeline()


def test_contrib_dagclass_010__method_cycle_fails_at_instantiation():
    with pytest.raises(DmlRepoError, match="dagclass member dependency cycle detected"):
        CyclicPipeline()


def test_contrib_dagclass_011__run_rejects_uncompiled_dagclass_object():
    pipeline = object.__new__(ConfiguredPipeline)

    with pytest.raises(DmlRepoError, match="api.run instance is not compiled"):
        api.run(pipeline, 3)


def test_contrib_dagclass_012__decorated_method_refs_share_dagclass_namespace():
    pipeline = DecoratedMethodPipeline(offset=1, scale=2, image="python:3.10")

    assert pipeline.main.kwargs["prepop"] == {"offset": 1, "scale": 2}
    assert pipeline.main.kwargs["kwargs"] == {"image": "python:3.10"}


def test_contrib_dagclass_013__dag_resolved_names_are_not_dependencies():
    assert api._DAGCLASS_RESERVED_NAMES == {
        "argv",
        "call",
        "cancel",
        "commit",
        "dml",
        "freeze",
        "keys",
        "message",
        "name",
        "put",
        "ref",
        "require",
        "result",
        "tags",
        "token",
        "unfreeze",
        "values",
    }
    assert DagOperationPipeline().main.kwargs["prepop"] == {}


def test_contrib_dagclass_014__dag_is_a_normal_member_name():
    pipeline = DagNamedMemberPipeline()

    assert pipeline.main.kwargs["prepop"] == {"dag": preprocess}


def test_contrib_dagclass_015__any_attribute_assignment_removes_dependency():
    assert UndeclaredAssignmentPipeline().main.kwargs["prepop"] == {}
    assert ConditionalAssignmentPipeline().main.kwargs["prepop"] == {}
    assert LaterAssignmentPipeline().main.kwargs["prepop"] == {}


def test_contrib_dagclass_016__unknown_final_edge_fails_compilation():
    with pytest.raises(DmlRepoError, match="Unknown dagclass member reference: self.missing"):
        UnknownReferencePipeline()


def test_contrib_dagclass_017__reserved_assignment_fails_compilation():
    with pytest.raises(DmlRepoError, match="Cannot assign to reserved dagclass names: put"):
        ReservedAssignmentPipeline()


def test_contrib_dagclass_018__reserved_class_member_fails_compilation():
    @api.dagclass
    class ReservedMemberPipeline:
        put = preprocess

        def main(self, raw):
            return raw

    with pytest.raises(DmlRepoError, match="dagclass uses reserved names: put"):
        ReservedMemberPipeline()


def test_contrib_dagclass_019__item_access_is_opaque_to_compilation():
    assert ItemAccessPipeline().main.kwargs["prepop"] == {}
    assert ItemAssignmentPipeline().main.kwargs["prepop"] == {"output": 1}

    @api.dagclass
    class UnknownItemPipeline:
        def main(self, raw):
            return self["missing"](raw)

    assert UnknownItemPipeline().main.kwargs["prepop"] == {}
