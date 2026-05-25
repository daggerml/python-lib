"""Contract tests for DML JSON serde helpers."""

from __future__ import annotations

import json

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from daggerml._internal import Ref
from daggerml._internal.serde import dml_dumps, dml_loads
from daggerml._internal.types import Error, Runnable, Uri
from tests.contracts.internal.support.test_db_support import REF_ALPHABET, STR_ALPHABET, _refs


def _scalar_strategy():
    return st.one_of(
        st.none(),
        st.booleans(),
        st.integers(min_value=-(2**63), max_value=2**63 - 1),
        st.floats(allow_nan=False, allow_infinity=False),
        st.text(alphabet=STR_ALPHABET, max_size=24),
    )


def _ref_strategy():
    return st.one_of(
        _refs("datum", "scalar"),
        _refs("datum", "list"),
        _refs("datum", "dict"),
        _refs("datum", "uri"),
        _refs("datum", "runnable"),
        _refs("node", "literal"),
        _refs("commit"),
        _refs("dag"),
    )


def _uri_strategy():
    return st.builds(
        Uri,
        uri=st.text(alphabet=REF_ALPHABET + ":/_-.", min_size=1, max_size=48),
    )


def _error_strategy():
    return st.builds(
        Error,
        message=st.text(alphabet=STR_ALPHABET, max_size=24),
        origin=st.text(alphabet=STR_ALPHABET, max_size=16),
        type=st.text(alphabet=STR_ALPHABET, max_size=16),
        stack=st.lists(
            st.dictionaries(
                st.text(alphabet=STR_ALPHABET, min_size=1, max_size=8),
                st.one_of(
                    st.text(alphabet=STR_ALPHABET, max_size=12),
                    st.integers(min_value=-(2**31), max_value=2**31 - 1),
                    st.none(),
                ),
                max_size=3,
            ),
            max_size=3,
        ),
    )


def _dict_strategy(children):
    return st.dictionaries(
        st.text(alphabet=STR_ALPHABET, min_size=1, max_size=12),
        children,
        max_size=4,
    )


def _serde_value_strategy():
    @st.composite
    def _runnable_strategy(draw, children):
        target = draw(_uri_strategy())
        sub = draw(st.one_of(st.none(), children.filter(lambda value: isinstance(value, Runnable))))
        kwargs = draw(_dict_strategy(children))
        adapter = draw(st.text(alphabet=STR_ALPHABET, max_size=16))
        return Runnable(target=target, sub=sub, kwargs=kwargs, adapter=adapter)

    return st.recursive(
        st.one_of(_scalar_strategy(), _ref_strategy(), _uri_strategy()),
        lambda children: st.one_of(
            st.lists(children, max_size=4),
            _dict_strategy(children),
            _error_strategy(),
            _runnable_strategy(children),
        ),
        max_leaves=16,
    )


@given(value=_serde_value_strategy())
@settings(max_examples=100, deadline=None)
def test_dml_json_roundtrip(value):
    assert dml_loads(dml_dumps(value)) == value


def test_dml_json_exports_string_form():
    payload = {"ref": Ref("datum-scalar:abc"), "uri": Uri("file:///tmp/x")}
    encoded = dml_dumps(payload)
    assert isinstance(encoded, str)
    assert dml_loads(encoded) == payload


def test_dml_json_uses_universal_array_envelope():
    payload = {"foo": Ref("bar:baz")}
    assert dml_dumps(payload) == (
        '["dict",{"foo":["ref","bar:baz"]}]'
    )


def test_dml_json_allows_plain_user_dict_keys_that_used_to_be_reserved():
    payload = {
        "type": "user-data",
        "value": "still-user-data",
        "dml": {"t": "Ref", "to": "foo:bar"},
        "__dml__": 7,
    }
    assert dml_loads(dml_dumps(payload)) == payload


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_dml_json_rejects_nonfinite_float(value):
    with pytest.raises(TypeError, match="non-finite float"):
        dml_dumps(value)


def test_dml_json_rejects_unsupported_type():
    with pytest.raises(TypeError, match="unsupported type"):
        dml_dumps((1, 2, 3))


def test_dml_json_rejects_non_envelope_input_on_load():
    with pytest.raises(ValueError, match="array of length 2"):
        dml_loads(json.dumps(["foo"]))
