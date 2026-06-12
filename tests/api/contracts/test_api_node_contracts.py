from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import daggerml.api as api
from daggerml._core import DmlRepoError, Error


def test_api_node_001__value_load_type_equality_hash_and_repr(dag, fake_dml, refs):
    node = api.Node(dag, refs.imported, _info={"data_type": "scalar"})

    assert node.value() == "imported"
    fake_dml.dag.get_node.assert_called_with(refs.imported, recursive=True)

    loaded = node.load()
    assert loaded.dml is fake_dml
    assert loaded.ref == refs.dag2
    assert node.type == "scalar"
    assert node == api.Node(dag, refs.imported)
    assert node != api.Node(dag, refs.scalar)
    assert node.__eq__(object()) is NotImplemented
    assert hash(node) == hash(refs.imported)
    assert repr(node) == f"Node({refs.imported.to})"


def test_api_node_002__load_requires_import_or_fn_dag(dag, fake_dml, refs):
    fake_dml.dag.describe_node.return_value = {"id": refs.scalar, "type": "LiteralNode"}

    with pytest.raises(DmlRepoError, match="is not an `import` nor `fn` node"):
        api.Node(dag, refs.scalar).load()


def test_api_node_003__plain_node_is_not_callable(dag, refs):
    node = api.Node(dag, refs.scalar, _info={"data_type": "int"})

    with pytest.raises(TypeError, match="Node of type 'int' is not callable"):
        node(1)


def test_api_node_004__runnable_node_delegates_to_dag_call(dag, refs):
    node = api.RunnableNode(dag, refs.runnable, _info={"data_type": "runnable"})
    result = api.Node(dag, refs.result)

    with patch.object(dag, "call", return_value=result) as call:
        assert node(1, name="out", sleep="sleep", timeout=5) is result

    call.assert_called_once_with(node, 1, name="out", sleep="sleep", timeout=5)


def test_api_node_005__collection_contains_and_len_delegate_to_builtin(dag, refs):
    collection = api.ListNode(dag, refs.list, _info={"data_type": "list", "length": 2})
    item = api.Node(dag, refs.scalar)

    with patch.object(dag, "_call_builtin", return_value=refs.result) as builtin:
        result = collection.contains(item, name="has-item")

    builtin.assert_called_once_with("daggerml:contains", refs.list, refs.scalar, name="has-item")
    assert isinstance(result, api.ScalarNode)
    assert len(collection) == 2

    with patch.object(collection, "contains", return_value=MagicMock(value=MagicMock(return_value=True))):
        assert item in collection


def test_api_node_006__list_index_slice_iter_conj_and_append(dag, refs):
    node = api.ListNode(dag, refs.list, _info={"data_type": "list", "length": 3})

    with patch.object(dag, "_call_builtin", return_value=refs.scalar) as builtin:
        assert node[1].ref == refs.scalar
        assert node[1:3].ref == refs.scalar
        assert node[:].ref == refs.scalar

    builtin.assert_any_call("daggerml:get", refs.list, 1)
    builtin.assert_any_call("daggerml:get", refs.list, [1, 3])
    builtin.assert_any_call("daggerml:get", refs.list, [0, 3])

    with pytest.raises(ValueError, match="Slice step is not supported"):
        node[0:3:2]

    with patch.object(
        api.ListNode,
        "__getitem__",
        side_effect=[api.Node(dag, refs.scalar), api.Node(dag, refs.result), api.Node(dag, refs.imported)],
    ):
        assert [item.ref for item in node] == [refs.scalar, refs.result, refs.imported]

    with patch.object(dag, "_call_builtin", return_value=refs.list) as builtin:
        assert node.conj(api.Node(dag, refs.scalar), name="more").ref == refs.list
        assert node.append("raw").ref == refs.list

    builtin.assert_any_call("daggerml:conj", refs.list, refs.scalar, name="more")
    builtin.assert_any_call("daggerml:conj", refs.list, "raw", name=None)


def test_api_node_007__dict_index_keys_iter_get_items_values_assoc_update(dag, refs):
    node = api.DictNode(dag, refs.dict, _info={"data_type": "dict", "length": 2, "keys": ["a", "b"]})

    keys = node.keys()
    keys.append("mutated")
    assert node.keys() == ["a", "b"]
    assert list(node) == ["a", "b"]

    with patch.object(dag, "_call_builtin", return_value=refs.scalar) as builtin:
        assert node["a"].ref == refs.scalar
        assert node.get("missing", "default", name="fallback").ref == refs.scalar

    builtin.assert_any_call("daggerml:get", refs.dict, "a")
    builtin.assert_any_call("daggerml:get", refs.dict, "missing", "default", name="fallback")

    with patch.object(
        api.DictNode,
        "__getitem__",
        side_effect=[api.Node(dag, refs.scalar), api.Node(dag, refs.result)],
    ):
        assert [(key, value.ref) for key, value in node.items()] == [("a", refs.scalar), ("b", refs.result)]

    with patch.object(
        api.DictNode,
        "__getitem__",
        side_effect=[api.Node(dag, refs.scalar), api.Node(dag, refs.result)],
    ):
        assert [value.ref for value in node.values()] == [refs.scalar, refs.result]

    with patch.object(dag, "_call_builtin", return_value=refs.dict) as builtin:
        assert node.assoc("c", api.Node(dag, refs.scalar), name="updated").ref == refs.dict
        assert node.assoc("d", 4).ref == refs.dict

    builtin.assert_any_call("daggerml:assoc", refs.dict, "c", refs.scalar, name="updated")
    builtin.assert_any_call("daggerml:assoc", refs.dict, "d", 4, name=None)

    first = api.DictNode(dag, refs.scalar, _info={"data_type": "dict", "length": 1, "keys": ["x"]})
    second = api.DictNode(dag, refs.result, _info={"data_type": "dict", "length": 2, "keys": ["x", "y"]})

    def assoc_side_effect(self, key, value):
        return first if (self, key, value) == (node, "x", 1) else second

    with patch.object(api.DictNode, "assoc", autospec=True, side_effect=assoc_side_effect) as assoc:
        assert node.update({"x": 1, "y": 2}) is second
    assoc.assert_any_call(node, "x", 1)
    assoc.assert_any_call(first, "y", 2)


def test_api_node_008__dict_items_rejects_non_dict_type(dag, refs):
    node = api.DictNode(dag, refs.dict, _info={"data_type": "list", "length": 0, "keys": []})

    with pytest.raises(Error, match="Cannot iterate items of type: list"):
        list(node.items())
