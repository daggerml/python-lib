from __future__ import annotations

from hypothesis import strategies as st

from daggerml._core.db import Ref
from daggerml._core.types import Error, Runnable, Uri

_SEGMENT_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789._-"
_FIRST_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789"


ref_segments = st.builds(
    lambda first, rest: first + rest,
    st.sampled_from(tuple(_FIRST_CHARS)),
    st.text(alphabet=_SEGMENT_CHARS, min_size=0, max_size=8),
).filter(lambda value: value not in {".", ".."})

nested_ref_names = st.lists(ref_segments, min_size=1, max_size=3).map("/".join)
project_segments = ref_segments
branch_selectors = nested_ref_names.map(lambda name: f"#{name}")
tag_selectors = nested_ref_names.map(lambda name: f"@{name}")
project_uris = st.builds(
    lambda owner, project, selector: f"dml://{owner}/{project}{selector}",
    project_segments,
    project_segments,
    st.one_of(st.just(""), branch_selectors, tag_selectors),
)

hex_commits = st.text(alphabet="0123456789abcdef", min_size=64, max_size=64)
revision_selectors = st.one_of(
    st.just("HEAD"),
    st.integers(min_value=1, max_value=25).map(lambda n: f"HEAD~{n}"),
    hex_commits,
    hex_commits.map(lambda commit: f"commit:{commit}"),
    nested_ref_names,
    project_uris,
)

finite_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-1000, max_value=1000),
    st.floats(allow_nan=False, allow_infinity=False, width=32),
    st.text(max_size=20),
)

runnable_unroll_values = st.recursive(
    finite_scalars,
    lambda children: st.one_of(
        st.lists(children, max_size=3),
        st.dictionaries(st.text(min_size=1, max_size=8), children, max_size=3),
    ),
    max_leaves=8,
)

runnables = st.recursive(
    st.builds(
        Runnable,
        target=st.builds(Uri, st.sampled_from(["file:///tool.py", "s3://bucket/tool", "daggerml:list"])),
        sub=st.none(),
        kwargs=st.dictionaries(st.text(min_size=1, max_size=8), runnable_unroll_values, max_size=3),
        adapter=st.text(max_size=8),
    ),
    lambda children: st.builds(
        Runnable,
        target=st.builds(Uri, st.sampled_from(["file:///nested.py", "s3://bucket/nested"])),
        sub=children,
        kwargs=st.dictionaries(st.text(min_size=1, max_size=8), runnable_unroll_values, max_size=3),
        adapter=st.text(max_size=8),
    ),
    max_leaves=4,
)


def serde_values():
    leaf = st.one_of(
        finite_scalars,
        st.builds(Ref, st.sampled_from(["datum-scalar:x", "node-literal:y", "commit:z"])),
        st.builds(Uri, st.sampled_from(["file:///tmp/data", "s3://bucket/key", "daggerml:list"])),
        st.builds(
            Error,
            st.text(max_size=20),
            st.just("dml"),
            st.just("test"),
            st.lists(st.dictionaries(st.text(max_size=5), st.text(max_size=10), max_size=2), max_size=2),
        ),
    )
    containers = st.recursive(
        leaf,
        lambda children: st.one_of(
            st.lists(children, max_size=3),
            st.dictionaries(st.text(min_size=1, max_size=8), children, max_size=3),
        ),
        max_leaves=8,
    )
    return st.one_of(containers, runnables)
