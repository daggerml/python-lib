from __future__ import annotations

import os
import threading

import pytest

from daggerml._core.db import DmlDb as RawDmlDB
from daggerml._core.db import DmlDbForkedTxnError, Ref
from daggerml._core.types import NAMESPACES, ScalarDatum
from tests._core.helpers import make_db, run_parallel


def test_db_concurrent_thread_writes_remain_readable(tmp_path) -> None:
    db = make_db(tmp_path)
    db.init()

    def write_scalar(i: int) -> Ref:
        ref = Ref(f"datum-scalar:thread-{i}")
        with db.tx() as txn:
            return txn.put(ScalarDatum(f"value-{i}"), to=ref)

    refs = run_parallel(4, write_scalar)

    assert len(set(refs)) == 4
    with db.tx(readonly=True) as txn:
        for i, ref in enumerate(refs):
            assert txn.get(ref) == ScalarDatum(f"value-{i}")


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_db_forked_child_reopen_writes_visible_to_parent(tmp_path) -> None:
    db = make_db(tmp_path)
    db.init()
    child_ref = Ref("datum-scalar:fork-child")
    parent_ref = Ref("datum-scalar:fork-parent")
    read_fd, write_fd = os.pipe()

    pid = os.fork()
    if pid == 0:
        os.close(read_fd)
        try:
            with db.tx() as txn:
                txn.put(ScalarDatum("child"), to=child_ref)
            os.write(write_fd, b"ok")
            os._exit(0)
        except BaseException as exc:
            os.write(write_fd, repr(exc).encode("utf-8", errors="replace"))
            os._exit(1)

    os.close(write_fd)
    try:
        with db.tx() as txn:
            txn.put(ScalarDatum("parent"), to=parent_ref)
        child_message = os.read(read_fd, 4096)
        _child_pid, status = os.waitpid(pid, 0)
    finally:
        os.close(read_fd)

    assert os.WIFEXITED(status), f"child did not exit cleanly: {status}"
    assert os.WEXITSTATUS(status) == 0, child_message.decode("utf-8", errors="replace")

    with db.tx(readonly=True) as txn:
        assert txn.get(child_ref) == ScalarDatum("child")
        assert txn.get(parent_ref) == ScalarDatum("parent")


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_db_forked_child_inherited_transaction_remains_invalid(tmp_path) -> None:
    db = make_db(tmp_path)
    db.init()
    ref = Ref("datum-scalar:fork-txn")

    with db.tx() as txn:
        txn.put(ScalarDatum("parent"), to=ref)

    read_fd, write_fd = os.pipe()
    with db.tx(readonly=True) as txn:
        pid = os.fork()
        if pid == 0:
            os.close(read_fd)
            try:
                try:
                    txn.get(ref)
                except DmlDbForkedTxnError:
                    os.write(write_fd, b"ok")
                    os._exit(0)
                os.write(write_fd, b"expected inherited transaction to fail after fork")
                os._exit(1)
            except BaseException as exc:
                os.write(write_fd, repr(exc).encode("utf-8", errors="replace"))
                os._exit(1)

        os.close(write_fd)
        try:
            child_message = os.read(read_fd, 4096)
            _child_pid, status = os.waitpid(pid, 0)
        finally:
            os.close(read_fd)

    assert os.WIFEXITED(status), f"child did not exit cleanly: {status}"
    assert os.WEXITSTATUS(status) == 0, child_message.decode("utf-8", errors="replace")


@pytest.mark.slow
@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_db_adopts_map_resized_by_another_process(tmp_path) -> None:
    path = tmp_path / "db"
    path.mkdir()
    db = RawDmlDB(str(path), namespaces=sorted(NAMESPACES), map_size_headroom=2 * 1024**2, max_map_size=32 * 1024**2)
    with db.tx(create_if_missing=True):
        pass
    read_fd, write_fd = os.pipe()

    with db.tx(readonly=True):
        pid = os.fork()
        if pid == 0:
            os.close(read_fd)
            try:
                child_db = RawDmlDB(
                    str(path), namespaces=sorted(NAMESPACES), map_size_headroom=2 * 1024**2, max_map_size=32 * 1024**2
                )

                def grow(txn):
                    for i in range(12):
                        txn.put(f"{i:05d}" + "x" * (900 * 1024 - 5), ns="datum-scalar")

                child_db.write_with_growth(grow)
                os.write(write_fd, b"ok")
                os._exit(0)
            except BaseException as exc:
                os.write(write_fd, repr(exc).encode("utf-8", errors="replace"))
                os._exit(1)

        os.close(write_fd)
        child_message = os.read(read_fd, 4096)
        _child_pid, status = os.waitpid(pid, 0)
        assert os.WIFEXITED(status), f"child did not exit cleanly: {status}"
        assert os.WEXITSTATUS(status) == 0, child_message.decode("utf-8", errors="replace")

        acquired = threading.Event()

        def open_after_resize() -> None:
            with db.tx(readonly=True):
                acquired.set()

        worker = threading.Thread(target=open_after_resize)
        worker.start()
        worker.join(timeout=0.1)
        assert not acquired.is_set()

    worker.join(timeout=5)
    os.close(read_fd)
    assert acquired.is_set()
