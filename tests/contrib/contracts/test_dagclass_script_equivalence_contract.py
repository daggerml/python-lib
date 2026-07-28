from __future__ import annotations

from daggerml.contrib import api
from daggerml.contrib.codecs import DelayedRunnable
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


@api.funkify(prepop={"summarize": api.ref("summarize"), "preprocess": api.ref("preprocess")})
def main(self, raw):
    return self.summarize(self.preprocess(raw))


def test_contrib_dagclass_001__matching_funkify_definition_renders_identical_script():
    dagclass_main = DagclassPipeline().main

    assert isinstance(dagclass_main, DelayedRunnable)
    dagclass_kwargs, dagclass_script = ScriptExecutor._script_kwargs(dagclass_main.kwargs)
    funkify_kwargs, funkify_script = ScriptExecutor._script_kwargs(main.kwargs)

    assert dagclass_kwargs == funkify_kwargs
    assert dagclass_script == funkify_script


def test_contrib_dagclass_002__nested_instance_embeds_child_member_graph():
    child = ParentPipeline().__dagclass_members__["child"]

    assert isinstance(child, DelayedRunnable)
    assert isinstance(child.kwargs["prepop"]["preprocess"], DelayedRunnable)
    assert isinstance(child.kwargs["prepop"]["summarize"], DelayedRunnable)


def test_contrib_dagclass_003__comprehension_collects_member_dependency():
    main = ComprehensionPipeline().main

    assert main.kwargs["prepop"] == {"summarize": api.ref("summarize")}


def test_contrib_dagclass_004__member_defined_before_read_is_not_dependency():
    main = DefinedMemberPipeline().main

    assert main.kwargs["prepop"] == {}


def test_contrib_dagclass_005__comprehension_captures_nested_dagclass():
    pipeline = NestedComprehensionPipeline()
    main = pipeline.main

    assert main.kwargs["prepop"] == {"child": api.ref("child")}
    assert isinstance(pipeline.__dagclass_members__["child"], DelayedRunnable)
