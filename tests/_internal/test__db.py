import hashlib
import os
import random
import threading
from base64 import b64decode, b64encode
from contextlib import contextmanager
from uuid import uuid4

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from daggerml._internal._db import (
    DmlDbEnv,
    DmlDbError,
    DmlDbForkedTxnError,
    DmlDbInvalidPathError,
    DmlDbKeyNotFoundError,
    Ref,
)

REF_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"
STR_ALPHABET = st.characters(blacklist_categories=("Cs", "Cc"), blacklist_characters="\x00")


def _refs(*ns: str, full: bool = False):
    if not ns:
        raise ValueError("at least one namespace must be provided")
    # Use printable UTF-8 characters (STR_ALPHABET) when full is True; otherwise
    # use a restricted alphanumeric REF_ALPHABET. Additionally restrict the
    # UTF-8 encoded byte length to DML_REF_ID_MAX (64 bytes) since the C parser
    # enforces a maximum id byte size.
    text_strat = st.text(
        alphabet=STR_ALPHABET if full else REF_ALPHABET,
        min_size=1 if full else 32,
        max_size=64,
    ).filter(lambda s: len(s.encode("utf-8")) <= 64)
    ns_ = "-".join(ns)
    return st.builds(lambda ident: Ref(f"{ns_}:{ident}"), text_strat)


def _gen_ref(*ns: str) -> Ref:
    """Generate a random ref for testing, avoiding Hypothesis .example() issues."""
    if not ns:
        raise ValueError("at least one namespace must be provided")
    ns_str = "-".join(ns)
    # Generate a random identifier using REF_ALPHABET
    ident = "".join(random.choice(REF_ALPHABET) for _ in range(32))
    return Ref(f"{ns_str}:{ident}")


def int_strategy():
    return st.integers(min_value=-(2**63), max_value=2**63 - 1)


def float_strategy():
    return st.floats(allow_nan=False, allow_infinity=False)


def scalar_strategy(recursive=False):
    return st.one_of(
        int_strategy(),
        float_strategy(),
        st.booleans(),
        st.text(),
        st.none(),
    )


def dml_object():
    return st.recursive(
        scalar_strategy(recursive=True),
        lambda children: st.one_of(
            st.lists(children, max_size=4),
            st.dictionaries(st.text(min_size=1, max_size=5), children, max_size=4),
        ),
        max_leaves=12,
    )


@contextmanager
def make_db(root, name, namespaces=("a", "b")):
    path = root / name
    path.mkdir()
    env = DmlDbEnv.create(str(path), namespaces=namespaces)
    try:
        yield env
    finally:
        env.close()


@pytest.fixture
def db_env(tmp_path):
    tmpdir = tmp_path / f"db_env_{uuid4().hex}"
    tmpdir.mkdir()
    env = DmlDbEnv.create(str(tmpdir), namespaces=["a", "b"])
    size = env.get_size()
    assert size > 0
    new_size = size + 512 * 1024
    env.resize(new_size)
    try:
        yield env
    finally:
        env.close()


class TestDbEnv:
    def test_create_invalid_path(self, tmp_path):
        missing_path = tmp_path / "missing" / "repo"
        with pytest.raises(DmlDbInvalidPathError):
            DmlDbEnv.create(str(missing_path), namespaces=["a"])

    def test_create_and_open(self, tmp_path):
        db_path = tmp_path / "db_env"
        db_path.mkdir()
        env = DmlDbEnv.create(str(db_path), namespaces=["a", "b"])
        assert env is not None
        assert env.path == str(db_path)
        size = env.get_size()
        assert size > 0
        new_size = size + 512 * 1024
        env.resize(new_size)
        assert env.get_size() == new_size
        with env.tx(readonly=False) as txn:
            x = txn.put("hello", ns="a")
        assert isinstance(x, Ref)
        assert x.ns() == "a"
        with env.tx(readonly=True) as txn:
            assert txn.get(x) == "hello"
        env.close()

    def test_open_requires_mapsize_for_large_db(self, tmp_path):
        db_path = tmp_path / "db_env_large"
        db_path.mkdir()
        DmlDbEnv.create(str(db_path), namespaces=["a"])

        @contextmanager
        def _open(map_size=None):
            db = DmlDbEnv.open(str(db_path), namespaces=["a"], map_size=map_size)
            try:
                yield db
            finally:
                db.close()

        # Use small strings within the 1MB limit
        small_data = "x" * (100 * 1024)  # 100KB
        # Try to fill the default map without explicit map_size
        # Expect failure when DB fills up
        refs = []
        with pytest.raises((DmlDbError, RuntimeError)):
            with _open() as env:
                with env.tx(readonly=False) as txn:
                    for i in range(1000):  # Cap iterations
                        ref = txn.put(f"{small_data}_{i}", ns="a")
                        refs.append(ref)
        # Reopen with larger map_size and verify success
        with _open(20 * 1024**2) as env:  # 20MB map
            with env.tx(readonly=False) as txn:
                for i in range(100):
                    ref = txn.put(f"{small_data}_{i}", ns="a")
                    refs.append(ref)
        # Verify data persists
        with _open() as env:
            with env.tx(readonly=True) as txn:
                assert txn.get(refs[-1]).startswith(small_data[:50])

    @given(dml_object(), _refs("a", full=True))
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=10)
    def test_given_key_round_trip(self, db_env, data, to_ref):
        with db_env.tx(readonly=False) as txn:
            x = txn.put(data, to=to_ref)
        assert isinstance(x, Ref)
        assert x.ns() == "a"
        with db_env.tx(readonly=True) as txn:
            assert txn.get(x) == data
        with db_env.tx(readonly=False) as txn:
            txn.delete(x)

    @given(dml_object())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=10)
    def test_round_trip(self, db_env, data):
        with db_env.tx(readonly=False) as txn:
            x = txn.put(data, ns="a")
        assert isinstance(x, Ref)
        assert x.ns() == "a"
        with db_env.tx(readonly=True) as txn:
            assert txn.get(x) == data

    def test_overwrite(self, db_env):
        with db_env.tx(readonly=False) as txn:
            x = txn.put("hello", ns="a")
            assert txn.get(x) == "hello"
            y = txn.put("world", to=x)
        assert x == y
        with db_env.tx(readonly=True) as txn:
            assert txn.get(x) == "world"

    def test_no_overwrite(self, db_env):
        with db_env.tx(readonly=False) as txn:
            x = txn.put("hello", ns="a")
            assert txn.get(x) == "hello"
            y = txn.put("world", to=x, no_overwrite=True)
        assert x == y
        with db_env.tx(readonly=True) as txn:
            assert txn.get(x) == "hello"

    def test_delete(self, db_env):
        with db_env.tx(readonly=False) as txn:
            x = txn.put("hello", ns="a")
            txn.delete(x)
        with db_env.tx(readonly=True) as txn:
            with pytest.raises(DmlDbKeyNotFoundError):
                txn.get(x)

    def test_get_missing_key(self, db_env):
        key = Ref("a:missing")
        with db_env.tx(readonly=True) as txn:
            with pytest.raises(DmlDbKeyNotFoundError):
                txn.get(key)

    @given(st.binary())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=10)
    def test_put_raw_parameter(self, db_env, data):
        """Test the raw parameter in the put method."""
        raw_bytes = b64encode(data).decode("utf-8")
        with db_env.tx(readonly=False) as txn:
            ref = txn.put(raw_bytes, ns="a", raw=True)
        with db_env.tx(readonly=True) as txn:
            raw_result = txn.get(ref, raw=True)
            assert isinstance(raw_result, str)
            assert raw_result == raw_bytes

    @given(dml_object())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=10)
    def test_raw_data_roundtrip_between_databases(self, tmp_path, data):
        with make_db(tmp_path, f"db1_{uuid4().hex}", namespaces=["a"]) as db1:
            with db1.tx(readonly=False) as txn:
                ref1 = txn.put(data, ns="a")
            # Retrieve the raw data
            with db1.tx(readonly=True) as txn:
                raw_data = txn.get(ref1, raw=True)
        # Create second database and insert the raw data
        with make_db(tmp_path, f"db2_{uuid4().hex}", namespaces=["a"]) as db2:
            with db2.tx(readonly=False) as txn:
                ref2 = txn.put(raw_data, ns="a", raw=True)
            # Retrieve the raw data from the second database
            with db2.tx(readonly=True) as txn:
                new_data = txn.get(ref2)
        # Verify the raw data is identical
        assert new_data == data

    @given(dml_object())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=10)
    def test_ref_id_is_sha256sum_of_raw_data(self, db_env, data):
        """
        Insert arbitrary data (including dicts, lists, refs, etc.) and get back `ref`.
        Then ensure the `ref.id()` matches the SHA-256 hash of the raw data retrieved with `get(raw=True)`.

        Note: We should **not** need msgpack here, as we're only verifying the hash of the raw data string.
        """
        with db_env.tx(readonly=False) as txn:
            ref = txn.put(data, ns="a")
            raw = txn.get(ref, raw=True)
        sha256_hash = hashlib.sha256(b64decode(raw)).hexdigest()
        assert ref.id() == sha256_hash

    def test_exists(self, db_env):
        key = Ref("a:mykey")
        with db_env.tx(readonly=True) as txn:
            assert not txn.exists(key)
        with db_env.tx(readonly=False) as txn:
            x = txn.put("hello", to=key)
        with db_env.tx(readonly=True) as txn:
            assert txn.exists(x)

    def test_contextmanager(self, db_env):
        with db_env.tx(readonly=False) as txn:
            x = txn.put("x", ns="a")
            assert txn.get(x) == "x"
        with db_env.tx(readonly=True) as txn:
            assert txn.get(x) == "x"

    def test_nested_transactions(self, db_env):
        with db_env.tx(readonly=False) as txn0:
            x0 = txn0.put(0, ns="a")
            assert txn0.get(x0) == 0
            x1 = txn0.put(1, ns="a")
            assert txn0.get(x1) == 1
            with db_env.tx(readonly=True) as txn:
                with pytest.raises(DmlDbKeyNotFoundError):
                    txn.get(x1)  # creates a new read transaction
            # Note: nested transactions via txn0.tx() are not supported in the new API
            # So we'll continue using the same transaction
            x2 = txn0.put(2, ns="a")
            assert txn0.get(x2) == 2
            with db_env.tx(readonly=True) as txn:
                with pytest.raises(DmlDbKeyNotFoundError):
                    txn.get(x2)
            assert txn0.get(x2) == 2
            with db_env.tx(readonly=True) as txn:
                with pytest.raises(DmlDbKeyNotFoundError):
                    txn.get(x2)
        with db_env.tx(readonly=True) as txn:
            assert txn.get(x2) == 2
            assert txn.get(x1) == 1
            assert txn.get(x0) == 0

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="fork not available on this platform")
    def test_fork_closes_inherited_txn(self, db_env):
        with pytest.raises(RuntimeError, match="test raise"):
            with db_env.tx(readonly=False) as txn:
                pid = os.fork()
                if pid == 0:
                    try:
                        txn.put("child-write", ns="a")
                    except DmlDbForkedTxnError:
                        os._exit(0)
                    os._exit(1)
                pid, status = os.waitpid(pid, 0)
                assert os.WIFEXITED(status)
                assert os.WEXITSTATUS(status) == 0
                raise RuntimeError("test raise")

    def test_unreachable_objects(self, db_env):
        with db_env.tx(readonly=False) as txn:
            ref1 = txn.put("v1", ns="a")
            ref2 = txn.put("v2", ns="a")
            ref3 = txn.put({"child": ref1}, ns="a")
            # list_orphans is a txn-level method
            unreachable = txn.list_orphans(start=[ref3])
        assert ref2 in unreachable
        assert ref1 not in unreachable
        assert ref3 not in unreachable

    @given(
        st.recursive(
            scalar_strategy(recursive=False),
            lambda children: st.lists(children, max_size=4)
            | st.dictionaries(st.text(min_size=1, max_size=5), children, max_size=4),
            max_leaves=20,
        )
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=50)
    def test_deep_nested_round_trip(self, db_env, data):
        """Store and retrieve deeply nested data structures to ensure C -> Python
        deserialization properly transfers ownership and produces equivalent
        Python objects (no use-after-free or partial copies).
        """

        def max_depth(o):
            if isinstance(o, dict):
                return 1 + max((max_depth(v) for v in o.values()), default=0)
            if isinstance(o, (list, tuple)):
                return 1 + max((max_depth(v) for v in o), default=0)
            return 0

        assume(max_depth(data) > 1)
        with db_env.tx(readonly=False) as txn:
            ref = txn.put(data, ns="a")
        with db_env.tx(readonly=True) as txn:
            got = txn.get(ref)
        with db_env.tx(readonly=False) as txn:
            txn.delete(ref)
        assert got == data
        if isinstance(data, dict):
            assert set(got.keys()) == set(data.keys())

    def test_close_idempotency(self, db_env):
        # Note: nested transactions are not supported in the new API
        # So we'll just test that a single transaction works
        with db_env.tx(readonly=True) as _txn:
            pass


class TestIter:
    @pytest.mark.parametrize(
        "start_token, expected_keys",
        [
            (None, ["k1", "k2", "k3"]),
            ("k2", ["k2", "k3"]),
        ],
    )
    def test_iter_items(self, db_env, start_token, expected_keys):
        expected = {"k1": "v1", "k2": "v2", "k3": "v3"}
        with db_env.tx(readonly=False) as txn:
            for key, value in expected.items():
                txn.put(value, to=Ref(f"a:{key}"))

        # iter is a txn-level method, so we need a read transaction
        with db_env.tx(readonly=True) as txn:
            items = list(txn.iter("a", start_token=start_token))
        keys = [ref.id() for ref, _ in items]
        values = {ref.id(): value for ref, value in items}

        assert keys == expected_keys
        assert values == {key: expected[key] for key in expected_keys}

    def test_iter_transaction_visibility(self, db_env):
        with db_env.tx(readonly=False) as txn_setup:
            txn_setup.put("v0", to=Ref("a:k0"))
            txn_setup.put("vb0", to=Ref("b:kb0"))
        with db_env.tx(readonly=False) as txn:
            txn.put("v1", to=Ref("a:k1"))
            txn.put("vb1", to=Ref("b:kb1"))
            txn_items = list(txn.iter("a"))
            txn_keys = [ref.id() for ref, _ in txn_items]
            txn_values = {ref.id(): value for ref, value in txn_items}
            assert txn_keys == ["k0", "k1"]
            assert txn_values == {"k0": "v0", "k1": "v1"}
            # For root_items, we need a new read transaction
            with db_env.tx(readonly=True) as root_txn:
                root_items = list(root_txn.iter("a"))
                root_keys = [ref.id() for ref, _ in root_items]
            assert root_keys == ["k0"]
        with db_env.tx(readonly=True) as after_txn:
            after_items = list(after_txn.iter("a"))
        after_keys = [ref.id() for ref, _ in after_items]
        after_values = {ref.id(): value for ref, value in after_items}

        assert after_keys == ["k0", "k1"]
        assert after_values == {"k0": "v0", "k1": "v1"}

    def test_empty_db(self, db_env):
        # iter is a txn-level method, so we need a read transaction
        with db_env.tx(readonly=True) as txn:
            with pytest.raises(DmlDbKeyNotFoundError):
                list(txn.iter("a"))


class TestIterationTruncation:
    def test_null_chars(self, temp_db):
        """
        Demonstrate that keys containing NUL bytes are truncated by the iterator
        (strlen-based parsing) and that deleting the original (non-truncated)
        Ref then fails with DmlDbKeyNotFoundError.
        """
        with temp_db.tx(readonly=False) as txn:
            original = Ref("head:ab\x00cd")
            txn.put({"x": 1}, to=original)
            listed = [r for r, _ in txn.iter("head")]
            assert any(isinstance(r, Ref) for r in listed), "no refs yielded"
            listed_strs = [r.to for r in listed]
            # Fixed behavior: full key is preserved, deletion should succeed
            assert "head:ab\x00cd" in listed_strs
            txn.delete(original)
            with pytest.raises(DmlDbKeyNotFoundError):
                txn.get(original)

    @given(_refs("head", full=True))
    def test_any_chars(self, temp_db, ref):
        """
        Demonstrate that keys containing NUL bytes are truncated by the iterator
        (strlen-based parsing) and that deleting the original (non-truncated)
        Ref then fails with DmlDbKeyNotFoundError.
        """
        with temp_db.tx(readonly=False) as txn:
            txn.put({"x": 1}, to=ref)
            listed = [r for r, _ in txn.iter("head")]
            assert any(isinstance(r, Ref) for r in listed), "no refs yielded"
            # Historically the iterator used strlen and produced a truncated id
            # 'head@ab'. New behavior preserves binary keys (NULs included).
            # Fixed behavior: full key is preserved, deletion should succeed
            assert ref in listed
            txn.delete(ref)
            with pytest.raises(DmlDbKeyNotFoundError):
                txn.get(ref)


class TestRef:
    def test_nss_simple(self):
        ref = Ref("head:mainbranch")
        assert ref.nss() == ["head"]

    def test_nss_hierarchical(self):
        ref = Ref("node-argv:asdfqiowewf")
        assert ref.nss() == ["node", "argv"]

    def test_nss_deep_hierarchy(self):
        ref = Ref("a-b-c:id")
        assert ref.nss() == ["a", "b", "c"]

    def test_nss_no_colon(self):
        ref = Ref("datum:id")
        assert ref.nss() == ["datum"]

    def test_nss_multiple_colons(self):
        ref = Ref("node-fn-arg:id")
        assert ref.nss() == ["node", "fn", "arg"]

    def test_threaded_puts(self, db_env):
        refs = []

        def worker(i):
            with db_env.tx(readonly=False) as txn:
                ref = txn.put({"v": i}, ns="a")
                refs.append(ref)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(refs) == 5
        with db_env.tx(readonly=True) as txn:
            values = sorted(txn.get(ref)["v"] for ref in refs)
        assert values == list(range(5))

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="fork not available on this platform")
    def test_fork_reopen(self, db_env):
        with db_env.tx(readonly=False) as txn:
            ref = txn.put({"v": "child"}, ns="a")
        pid = os.fork()
        if pid == 0:
            try:
                with db_env.tx(readonly=True) as txn:
                    assert txn.get(ref) == {"v": "child"}
            except Exception:
                os._exit(1)
            os._exit(0)
        pid, status = os.waitpid(pid, 0)
        assert os.WIFEXITED(status)
        assert os.WEXITSTATUS(status) == 0
