"""Comprehensive tests for types.py module with Hypothesis property-based testing."""

from collections import defaultdict

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from daggerml._internal.types import (
    DEFAULT_HEAD,
    NAMESPACES,
    NONE,
    ArgvNode,
    Collection,
    Commit,
    Dag,
    Deletable,
    DictDatum,
    DmlBase,
    DmlRepoError,
    Error,
    FnNode,
    ImportNode,
    KwargvNode,
    ListDatum,
    LiteralNode,
    MaybeRefCollection,
    MaybeRefScalar,
    RefCollection,
    RunnableDatum,
    Scalar,
    ScalarDatum,
    Tree,
    Uri,
    _register_dml_obj,
)
from tests.contracts.internal.support.test_db_support import REF_ALPHABET, STR_ALPHABET, _refs


def _scalar_value_strategy():
    """Strategy for scalar values."""
    return st.one_of(
        st.integers(min_value=-(2**63), max_value=2**63 - 1),
        st.floats(allow_nan=False, allow_infinity=False),
        st.booleans(),
        st.text(),
        st.none(),
    )


def _error_strategy():
    return st.builds(
        Error,
        message=st.text(alphabet=STR_ALPHABET, max_size=16),
        origin=st.text(alphabet=STR_ALPHABET, max_size=16),
        type=st.text(alphabet=STR_ALPHABET, max_size=16),
        stack=st.lists(
            st.dictionaries(
                st.text(alphabet=REF_ALPHABET, max_size=8),
                st.text(alphabet=REF_ALPHABET, max_size=8),
                max_size=3,
            ),
            max_size=3,
        ),
    )


def _dag_strategy():
    @st.composite
    def _draw_dag(draw):
        nodes = draw(st.lists(_node_ref, max_size=4))
        result = error = argv = None
        if nodes:
            names = draw(
                st.dictionaries(
                    st.text(alphabet=REF_ALPHABET, min_size=1, max_size=8),
                    st.sampled_from(nodes),
                    max_size=4,
                )
            )
            tmp = draw(st.one_of(st.none(), st.sampled_from(nodes), _refs("error")))
            if tmp is not None and tmp.ns() == "error":
                error = tmp
            else:
                result = tmp
            argv_nodes = [n for n in nodes if n.ns() == "node-argv"]
            argv = draw(st.one_of(st.none(), st.sampled_from(argv_nodes) if argv_nodes else st.none()))
        else:
            names = {}
            error = draw(st.one_of(st.none(), _refs("error")))
        return Dag(nodes=nodes, names=names, result=result, argv=argv, error=error)

    return _draw_dag()


def _tree_strategy():
    return st.builds(
        Tree,
        dags=st.dictionaries(
            st.text(alphabet=REF_ALPHABET, min_size=1, max_size=8),
            _refs("dag"),
            max_size=4,
        ),
    )


def _commit_strategy():
    return st.builds(
        Commit,
        parents=st.lists(_refs("commit"), max_size=3),
        tree=_refs("tree"),
        author=st.text(alphabet=REF_ALPHABET, max_size=16),
        message=st.text(alphabet=REF_ALPHABET, max_size=64),
        dag=st.one_of(st.none(), _refs("dag")),
    )
def _deletable_strategy():
    return st.builds(
        Deletable,
        uri=st.text(alphabet=REF_ALPHABET + ":/", min_size=1, max_size=32),
    )


def _uri_strategy():
    return st.builds(
        Uri,
        uri=st.text(alphabet=REF_ALPHABET + ":/", min_size=1, max_size=64),
    )


def _runnable_strategy():
    return st.builds(
        RunnableDatum,
        target=_refs("datum", "uri"),
        sub=st.one_of(st.none(), _refs("datum", "runnable")),
        kwargs=_refs("datum", "dict"),
        adapter=st.text(alphabet=REF_ALPHABET, min_size=1, max_size=16),
    )


def _literal_node_strategy():
    return st.builds(LiteralNode, value=_datum_ref)


def _argv_node_strategy():
    return st.builds(ArgvNode, value=_datum_ref)


def _kwargv_node_strategy():
    return st.builds(KwargvNode, value=_datum_ref)


_node_ref = st.one_of(*[_refs("node", t) for t in ["literal", "argv", "kwargv", "import", "fn"]])
_datum_ref = st.one_of(*[_refs("datum", t) for t in ["scalar", "list", "dict", "uri", "runnable"]])


def _import_node_strategy():
    return st.builds(ImportNode, dag=_refs("dag"), node=_node_ref)


def _fn_node_strategy():
    return st.builds(
        FnNode,
        dag=_refs("dag"),
        argv=st.lists(_node_ref, max_size=3),
    )


def _node_strategy():
    return st.one_of(
        _literal_node_strategy(),
        _argv_node_strategy(),
        _kwargv_node_strategy(),
        _import_node_strategy(),
        _fn_node_strategy(),
    )


def _scalar_datum_strategy():
    return st.builds(ScalarDatum, data=_scalar_value_strategy())


def _list_datum_strategy():
    return st.builds(ListDatum, data=st.lists(_datum_ref, max_size=3))


def _dict_datum_strategy():
    return st.builds(DictDatum, data=st.dictionaries(st.text(max_size=8), _datum_ref, max_size=3))


def _datum_strategy():
    return st.one_of(
        _scalar_datum_strategy(),
        _list_datum_strategy(),
        _dict_datum_strategy(),
        _uri_strategy(),
        _runnable_strategy(),
    )


def _dml_obj_strategy():
    return st.one_of(
        _datum_strategy(),
        _error_strategy(),
        _deletable_strategy(),
        _dag_strategy(),
        _tree_strategy(),
        _commit_strategy(),
        _node_strategy(),
    )


class TestDmlObjDecorator:
    """Test the dml_obj decorator functionality."""

    def test_register_dml_obj_registration(self):
        """Test that dml_obj decorator registers classes in NAMESPACES."""
        initial_namespaces = len(NAMESPACES)

        @_register_dml_obj
        class TestClass:
            pass

        assert "testclass" in NAMESPACES
        assert NAMESPACES["testclass"] is TestClass
        assert hasattr(TestClass, "_ns")
        assert TestClass._ns == "testclass"
        assert len(NAMESPACES) == initial_namespaces + 1

    @given(st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=1, max_size=20))
    def test_register_dml_obj_lowercase_conversion(self, class_name):
        """Test decorator converts class names to lowercase for namespace."""

        @_register_dml_obj
        class TempClass:
            pass

        # Temporarily set the class name
        TempClass.__name__ = class_name
        expected_ns = class_name.lower()

        # Re-register to test the name conversion
        obj = _register_dml_obj(TempClass)
        assert obj._ns == expected_ns


class TestDmlBase:
    """Test base class functionality."""

    def test_to_dict_excludes_private(self):
        """Test that to_dict excludes private attributes."""
        from dataclasses import dataclass

        @dataclass
        class TestClass(DmlBase):
            public: str
            _private: str = "hidden"

        obj = TestClass(public="visible", _private="hidden")
        result = obj.to_dict()
        assert "public" in result
        assert "_private" not in result
        assert result["public"] == "visible"

    @given(
        st.dictionaries(
            st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=1, max_size=10).filter(
                lambda key: key not in {"self", "cls"}
            ),
            st.one_of(st.text(max_size=20), st.integers()),
            min_size=1,
            max_size=5,
        )
    )
    def test_from_dict_creates_instance(self, field_data):
        """Test that from_dict creates correct instance with arbitrary data."""

        @_register_dml_obj
        class TestClass(DmlBase):
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)

        instance = TestClass.from_dict(field_data)
        for key, value in field_data.items():
            assert getattr(instance, key) == value


class TestDataClasses:
    """Test basic data class functionality with property-based testing."""

    def test_error_from_exception(self):
        """Test Error.from_ex creates Error from exception."""
        try:
            raise ValueError("test error")
        except Exception as e:
            error = Error.from_ex(e)
            assert error.message == "test error"
            assert error.origin == "python"
            assert error.type == "valueerror"
            assert len(error.stack) > 0

    @given(_dag_strategy().filter(lambda d: d.names), _refs("node"))
    def test_dag_nameof(self, dag, node_ref):
        """Test DAG nameof method with generated data."""
        assume(node_ref not in dag.names.values())
        reverse_map = defaultdict(list)
        for name, ref in dag.names.items():
            reverse_map[ref].append(name)
        for ref, names in reverse_map.items():
            assert dag.nameof(ref) in names
        # Test with non-existent ref
        if node_ref not in dag.names.values():
            assert dag.nameof(node_ref) is None

    @given(_uri_strategy())
    def test_deletable_from_uri(self, uri_datum):
        """Test Deletable.from_uri creates deletable."""
        deletable = Deletable.from_uri(uri_datum)
        assert deletable.uri == uri_datum.uri
        assert isinstance(deletable, Deletable)

    @given(_dml_obj_strategy())
    def test_registered_type_roundtrips_via_to_dict_from_dict(self, obj):
        """Test that all registered types can roundtrip through to_dict/from_dict."""
        obj_dict = obj.to_dict()
        restored = type(obj).from_dict(obj_dict)
        assert obj == restored

    @given(_dag_strategy().filter(lambda d: d.argv is None))
    def test_dag_cache_key_requires_argv(self, temp_bo, dag):
        """Test that cache_key requires argv.

        Uses a real transaction context from the `temp_bo` fixture to exercise
        `Dag.cache_key` with a real `TxnContext` instead of a casted mock.
        """
        with pytest.raises(DmlRepoError, match="Cannot compute cache key for DAG without argv"):
            with temp_bo._tx(readonly=True) as txn:
                dag.cache_key(txn)


class TestNodeTypes:
    """Test node type registration and serialization."""

    @given(_refs("dag"), _node_ref, _refs("datum", "scalar"))
    @settings(max_examples=1)
    def test_import_node_datum_ref(self, temp_bo, dag_ref, node_ref, datum_ref):
        """ImportNode.datum_ref reads imported node value via ops."""
        node = ImportNode(dag=dag_ref, node=node_ref)
        with temp_bo._tx() as txn:
            datum_ref = txn.put(ScalarDatum(data=123), to=datum_ref)
            txn.put(LiteralNode(value=datum_ref), to=node_ref)
            assert node.datum_ref(txn) == datum_ref


class TestConstants:
    """Test module constants."""

    def test_constants_defined(self):
        """Test that required constants are defined."""
        assert NONE is not None
        assert DEFAULT_HEAD == "main"

    """Test type alias definitions."""

    def test_type_aliases_importable(self):
        """Test that type aliases can be imported and used."""
        # Just test that they're importable - type checking is done by mypy
        assert Scalar is not None
        assert MaybeRefScalar is not None
        assert Collection is not None
        assert MaybeRefCollection is not None
        assert RefCollection is not None


class TestRegistries:
    """Test namespace and nodetype registries."""

    def test_registries_populated(self):
        """Test that registries contain expected entries."""
        # Check that _register_dml_obj classes are registered
        expected_namespaces = {
            "commit",
            "dag",
            "datum-scalar",
            "datum-list",
            "datum-dict",
            "datum-uri",
            "datum-runnable",
            "deletable",
            "error",
            "tree",
        }
        for namespace in expected_namespaces:
            assert namespace in NAMESPACES

    @given(st.sampled_from(list(NAMESPACES.keys())))
    def test_namespace_classes_have_ns_attribute(self, namespace):
        """Test that registered classes have correct _ns attribute."""
        cls = NAMESPACES[namespace]
        if hasattr(cls, "_ns"):
            assert cls._ns == namespace
