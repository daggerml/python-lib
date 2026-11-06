"""S3 graph packs, commit manifests, direct refs, and execution graph storage."""

import gzip
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import InitVar, dataclass, field, fields, is_dataclass
from queue import Empty, Full, Queue
from threading import Event
from typing import TYPE_CHECKING, Any, Literal, Mapping, Optional, overload
from urllib.parse import quote, unquote

from daggerml._core.commit import CommitOps
from daggerml._core.db import Ref
from daggerml._core.s3_cas import CasItem, CasItemConflict, S3Remote
from daggerml._core.types import (
    NAMESPACES,
    Commit,
    DmlBase,
    DmlDB,
    DmlRepoError,
    FnNode,
    ImportNode,
    Tree,
    TxnWithValid,
)
from daggerml._core.util import uuid7

if TYPE_CHECKING:
    import boto3

logger = logging.getLogger(__name__)

_MANIFEST_SCALAR_TYPES = (type(None), str, int, float, bool)
_SERDE_SCALAR = "scalar"
_SERDE_LIST = "list"
_SERDE_DICT = "dict"
_SERDE_REF = "ref"
_REMOTE_DESCRIPTOR = {
    "schema": 1,
    "hash": "sha256",
    "layout": "dagmanifest+dagpack+commits+refs+split-execution",
    "refs_prefix": "refs",
    "io_prefix": "io",
    "packs_prefix": "packs",
    "manifests_prefix": "manifests",
    "commits_prefix": "commits",
    "pack_schema": 1,
    "compression": "gzip",
    "execution_prefix": "../exec",
}


def get_remote_descriptor() -> dict[str, str | int]:
    """Return an independent copy of the supported remote storage descriptor."""
    return _REMOTE_DESCRIPTOR.copy()


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, allow_nan=False)


def _immutable_ref(value: str, namespace: str | None = None) -> Ref:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9-]*:[0-9a-f]{64}", value):
        raise ValueError("Remote object reference must contain a namespace and SHA256 identity")
    ref = Ref(value)
    if namespace is not None and ref.ns() != namespace:
        raise ValueError(f"Expected {namespace} reference, got {ref}")
    return ref


def _encode_cas_value(obj: Any) -> list[Any]:
    if isinstance(obj, _MANIFEST_SCALAR_TYPES):
        return [_SERDE_SCALAR, obj]
    if isinstance(obj, Ref):
        return [_SERDE_REF, obj.to]
    if isinstance(obj, list):
        return [_SERDE_LIST, [_encode_cas_value(value) for value in obj]]
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for key, value in obj.items():
            out[key] = _encode_cas_value(value)
        return [_SERDE_DICT, out]
    raise TypeError(f"Unsupported type for remote CAS serialization: {type(obj).__name__}")


def _decode_cas_value(obj: Any) -> Any:
    if not isinstance(obj, list):
        raise TypeError(f"Expected remote CAS envelope array, got {type(obj).__name__}")
    if len(obj) != 2:
        raise ValueError("Expected remote CAS envelope array of length 2")
    type_name, value = obj
    if type_name == _SERDE_SCALAR:
        if not isinstance(value, _MANIFEST_SCALAR_TYPES):
            raise TypeError("Remote CAS scalar envelope must carry a JSON scalar")
        return value
    if type_name == _SERDE_REF:
        if not isinstance(value, str):
            raise TypeError("Remote CAS ref envelope must carry a string")
        return Ref(value)
    if type_name == _SERDE_LIST:
        if not isinstance(value, list):
            raise TypeError("Remote CAS list envelope must carry a list")
        return [_decode_cas_value(item) for item in value]
    if type_name == _SERDE_DICT:
        if not isinstance(value, dict):
            raise TypeError("Remote CAS dict envelope must carry a dict")
        out: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("Remote CAS dict envelope keys must be strings")
            out[key] = _decode_cas_value(item)
        return out
    raise ValueError(f"Unknown remote CAS envelope type: {type_name!r}")


@dataclass
class Remote:
    root_uri: str
    n_workers: int
    client: InitVar["boto3.client"]
    _store: S3Remote = field(init=False)
    prune_age_seconds: int = 24 * 3600
    initialize: bool = True

    def __post_init__(self, client):
        self._store = S3Remote(self.root_uri.rstrip("/") + "/dml", client)
        if self.initialize:
            self._ensure_remote_descriptor()
        else:
            self._inspect_remote_descriptor()

    def _ensure_remote_descriptor(self) -> None:
        descriptor_key = self._store._key_for("dml.json")
        try:
            descriptor = json.loads(self._store._get(descriptor_key))
        except self._store.client.exceptions.NoSuchKey as exc:
            endpoint = S3Remote(self.root_uri.rstrip("/"), self._store.client)
            if endpoint._has_any(endpoint._key_for("")):
                raise DmlRepoError("Remote root is not empty and has no supported descriptor") from exc
            try:
                self._store._put_js(descriptor_key, _REMOTE_DESCRIPTOR, overwrite=False)
            except CasItemConflict:
                try:
                    descriptor = json.loads(self._store._get(descriptor_key))
                    self._validate_remote_descriptor(descriptor)
                except (TypeError, ValueError, json.JSONDecodeError, DmlRepoError) as invalid:
                    raise DmlRepoError(
                        "Unsupported remote descriptor; migrate this remote root before use"
                    ) from invalid
            return
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise DmlRepoError("Unsupported remote descriptor; migrate this remote root before use") from exc
        self._validate_remote_descriptor(descriptor)

    @staticmethod
    def _validate_remote_descriptor(descriptor: object) -> None:
        if (
            not isinstance(descriptor, dict)
            or type(descriptor.get("schema")) is not int
            or descriptor != _REMOTE_DESCRIPTOR
        ):
            raise DmlRepoError("Unsupported remote descriptor; migrate this remote root before use")

    def _inspect_remote_descriptor(self) -> None:
        descriptor_key = self._store._key_for("dml.json")
        try:
            descriptor = json.loads(self._store._get(descriptor_key))
        except self._store.client.exceptions.NoSuchKey as exc:
            endpoint = S3Remote(self.root_uri.rstrip("/"), self._store.client)
            if endpoint._has_any(endpoint._key_for("")):
                raise DmlRepoError("Remote root is not empty and has no supported descriptor") from exc
            return
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise DmlRepoError("Unsupported remote descriptor; migrate this remote root before use") from exc
        self._validate_remote_descriptor(descriptor)

    def _cas_key(self, oid: str) -> str:
        aa = oid[:2]
        bb = oid[2:4]
        return self._store._key_for(f"cas/sha256/{aa}/{bb}/{oid}")

    def _build_ref_payload(self, ref: Ref, metadata: dict | None = None) -> dict:
        return {"ref": {"to": ref.to}, "created": int(time.time()), "metadata": metadata or {}}

    def _validate_ref_payload(
        self,
        payload: dict,
        *,
        expected_root_ns: str | None = None,
        required_metadata: tuple[str, ...] = (),
    ) -> Ref:
        if not isinstance(payload, dict) or set(payload) != {"ref", "created", "metadata"}:
            raise ValueError("Remote ref payload must contain exactly ref, created, and metadata")
        ref_data = payload.get("ref")
        if not isinstance(ref_data, dict) or set(ref_data) != {"to"}:
            raise ValueError("Remote ref payload missing object field 'ref'")
        ref_to = ref_data.get("to")
        if not isinstance(ref_to, str):
            raise ValueError("Remote ref payload missing string field 'ref.to'")
        created = payload.get("created")
        if not isinstance(created, int) or isinstance(created, bool):
            raise ValueError("Remote ref payload missing integer field 'created'")
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            raise ValueError("Remote ref payload missing object field 'metadata'")
        for key in required_metadata:
            if key not in metadata:
                raise ValueError(f"Remote ref payload missing required metadata field {key!r}")
        root_ref = Ref(ref_to)
        if expected_root_ns is not None and root_ref.ns() != expected_root_ns:
            raise ValueError(
                f"Remote ref root namespace mismatch: expected {expected_root_ns!r}, got {root_ref.ns()!r}"
            )
        return root_ref

    def _dump_cas_object(self, obj: DmlBase) -> str:
        return json.dumps(_encode_cas_value(obj.to_dict()), separators=(",", ":"), sort_keys=True, allow_nan=False)

    def _load_cas_object(self, ref: Ref, payload: str) -> DmlBase:
        cls = NAMESPACES.get(ref.ns())
        if cls is None:
            raise ValueError(f"Unknown namespace for remote CAS object: {ref.ns()}")
        data = _decode_cas_value(json.loads(payload))
        if not isinstance(data, dict):
            raise TypeError(f"Remote CAS payload for {ref} must decode to a dict")
        obj = cls.from_dict(data)
        if not isinstance(obj, DmlBase):
            raise TypeError(f"Remote CAS payload for {ref} did not decode to a DmlBase object")
        obj._validate()
        return obj

    def _pack_key(self, ref: Ref) -> str:
        _immutable_ref(ref.to)
        kind = {"dag": "dag", "node-argv": "argv"}.get(ref.ns())
        if kind is None:
            raise ValueError(f"Only DAG and argv roots can be packed: {ref}")
        return self._store._key_for(f"packs/{kind}/{ref.id()}.json.gz")

    def _commit_key(self, ref: Ref) -> str:
        _immutable_ref(ref.to, "commit")
        return self._store._key_for(f"commits/{ref.id()}.json")

    def _manifest_key(self, ref: Ref) -> str:
        self._pack_key(ref)
        return self._store._key_for(f"manifests/{ref.ns()}/{ref.id()}.json")

    def _body_refs(self, obj) -> tuple[set[Ref], set[Ref], set[Ref]]:
        if isinstance(obj, ImportNode):
            return set(), set(), {_immutable_ref(obj.dag.to, "dag")}
        deps: set[Ref] = set()
        self._collect_direct_refs(obj, deps)
        functions = set()
        if isinstance(obj, FnNode):
            deps.discard(obj.dag)
            functions.add(_immutable_ref(obj.dag.to, "dag"))
        if any(ref.ns() in {"dag", "commit", "tree", "index", "frozenindex", "deletable"} for ref in deps):
            raise ValueError("Unexpected DAG/history/mutable edge in graph body")
        return deps, functions, set()

    def _partition(self, root: Ref, get) -> tuple[dict[Ref, DmlBase], set[Ref], set[Ref]]:
        objects, functions, imports = {}, set(), set()
        pending = [root]
        while pending:
            ref = pending.pop()
            if ref in objects:
                continue
            _immutable_ref(ref.to)
            obj = get(ref)
            objects[ref] = obj
            deps, children, imported = self._body_refs(obj)
            pending.extend(deps - objects.keys())
            functions.update(children)
            imports.update(imported)
        return objects, functions, imports

    def _encode_body(self, root: Ref, objects: dict[Ref, DmlBase]) -> bytes:
        self._pack_key(root)
        return gzip.compress(_json({"schema": 1, "root": root.to,
                                    "objects": {ref.to: self._dump_cas_object(obj)
                                                for ref, obj in objects.items()}}).encode(), compresslevel=6, mtime=0)

    def _encode_pack(self, root: Ref, txn: TxnWithValid) -> bytes:
        objects, _, _ = self._partition(root, txn.get)
        return self._encode_body(root, objects)

    def _decode_pack(self, root: Ref, raw: bytes) -> dict[Ref, DmlBase]:
        self._pack_key(root)
        data = json.loads(gzip.decompress(raw))
        if (not isinstance(data, dict) or set(data) != {"schema", "root", "objects"}
                or type(data["schema"]) is not int or data["schema"] != 1 or data["root"] != root.to
                or not isinstance(data["objects"], dict) or root.to not in data["objects"]):
            raise ValueError("Invalid graph pack envelope")
        objects = {}
        for value, payload in data["objects"].items():
            ref = _immutable_ref(value)
            if ref.ns() in {"commit", "tree", "index", "frozenindex", "deletable"}:
                raise ValueError("Graph pack contains history or mutable objects")
            if not isinstance(payload, str):
                raise ValueError("Packed object payload must be serialized JSON")
            objects[ref] = self._load_cas_object(ref, payload)
        try:
            reached, _, _ = self._partition(root, objects.__getitem__)
        except KeyError as exc:
            raise ValueError(f"Graph body is missing {exc}") from exc
        if set(reached) != set(objects):
            raise ValueError("Graph pack contains unreachable objects")
        return objects

    def _read_commit(self, ref: Ref, raw: str) -> tuple[Commit, Tree]:
        data = json.loads(raw)
        if (not isinstance(data, dict) or set(data) != {"schema", "commit", "tree", "dags"}
                or type(data["schema"]) is not int or data["schema"] != 1):
            raise ValueError("Invalid commit manifest envelope")
        commit_record, tree_record = data["commit"], data["tree"]
        if (not isinstance(commit_record, list) or len(commit_record) != 2 or commit_record[0] != ref.to
                or not isinstance(commit_record[1], str)):
            raise ValueError("Invalid commit manifest record")
        commit = self._load_cas_object(ref, commit_record[1])
        if (not isinstance(commit, Commit) or not isinstance(tree_record, list) or len(tree_record) != 2
                or tree_record[0] != commit.tree.to or not isinstance(tree_record[1], str)):
            raise ValueError("Invalid commit tree record")
        _immutable_ref(commit.tree.to, "tree")
        for parent in commit.parents:
            _immutable_ref(parent.to, "commit")
        tree = self._load_cas_object(commit.tree, tree_record[1])
        if not isinstance(tree, Tree) or data["dags"] != {name: dag.to for name, dag in tree.dags.items()}:
            raise ValueError("Commit manifest DAG map does not match its tree")
        for dag in tree.dags.values():
            _immutable_ref(dag.to, "dag")
        return commit, tree

    def _put_immutable(self, key: str, value: str | bytes) -> None:
        try:
            self._store._put(key, value, overwrite=False)
        except CasItemConflict:
            existing = self._store._get_bytes(key) if isinstance(value, bytes) else self._store._get(key)
            if key.endswith(".json.gz") and isinstance(existing, bytes) and isinstance(value, bytes):
                # Compressed bytes vary across zlib builds; the content is what must match.
                try:
                    existing, value = gzip.decompress(existing), gzip.decompress(value)
                except (OSError, EOFError):
                    pass
            if existing != value:
                raise DmlRepoError(f"Conflicting immutable remote object: {key}") from None

    def _collect_local_objects(
        self,
        root_ref: Ref,
        db: DmlDB,
        missing_commits: set[Ref] | None = None,
    ) -> tuple[dict[str, str], set[Ref]]:
        objects: dict[str, str] = {}
        encountered_missing: set[Ref] = set()
        visited: set[Ref] = set()
        pending = [root_ref]
        with db.tx(readonly=True) as txn:
            while pending:
                ref = pending.pop()
                if ref in visited:
                    continue
                visited.add(ref)
                if not txn.exists(ref) and ref in (missing_commits or set()):
                    encountered_missing.add(ref)
                    continue
                obj = txn.get(ref)
                objects[self._cas_key(ref.id())] = self._dump_cas_object(obj)
                deps: set[Ref] = set()
                self._collect_direct_refs(obj, deps)
                pending.extend(deps - visited)
        return objects, encountered_missing

    def _plan_upload(self, objs: dict[str, str]) -> dict[str, str]:
        with ThreadPoolExecutor(max_workers=self.n_workers) as pool:
            futures = {pool.submit(self._store._exists, key): key for key in objs}
            return {key: objs[key] for fut, key in futures.items() if not fut.result()}

    def _upload_objects(self, objects: Mapping[str, str | bytes]) -> None:
        logger.info(f"Uploading {len(objects)} objects...")
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=self.n_workers) as pool:
            futures = [pool.submit(self._put_immutable, key, data) for key, data in objects.items()]
            for future in futures:
                future.result()
        t1 = time.time()
        logger.info(f"Uploaded {len(objects)} objects in {t1 - t0:.2f} seconds")

    def _upload_packs(self, roots: set[Ref], db: DmlDB) -> None:
        # A manifest is published after all its dependencies and is the remote
        # completeness boundary. A bare pack can be an interrupted upload.
        edges = {}
        published: set[Ref] = set()
        manifest_roots = set(roots)
        pending = set(roots)
        remote_dependencies = {}
        with ThreadPoolExecutor(max_workers=self.n_workers) as pool:
            while pending:
                batch = sorted(pending)[:self.n_workers]
                pending.difference_update(batch)
                present = list(pool.map(self._store._exists, [self._manifest_key(ref) for ref in batch]))
                published.update(ref for ref, exists in zip(batch, present, strict=True) if exists)
                missing = [ref for ref, exists in zip(batch, present, strict=True) if not exists]
                if not missing:
                    continue
                packs_present = list(pool.map(self._store._exists, [self._pack_key(ref) for ref in missing]))
                writes = []
                with db.tx(readonly=True) as txn:
                    for root, pack_present in zip(missing, packs_present, strict=True):
                        objects, children, imports = self._partition(root, txn.get)
                        edges[root] = (children, imports)
                        manifest_roots.update(imports)
                        pending.update(children | imports)
                        if not pack_present:
                            writes.append(pool.submit(self._put_immutable, self._pack_key(root),
                                                      self._encode_body(root, objects)))
                pending.difference_update(edges.keys() | published)
                for future in writes:
                    future.result()

            manifests = {}
            for root in manifest_roots - published:
                flat, imports = set(), set(edges[root][1])
                children = list(edges[root][0])
                while children:
                    child = children.pop()
                    if child in flat:
                        continue
                    flat.add(child)
                    if child in published:
                        # Reuse an already-published function's flattened set;
                        # do not inspect its local body or walk its descendants.
                        if child not in remote_dependencies:
                            raw = pool.submit(self._store._get_bytes, self._manifest_key(child)).result()
                            remote_dependencies[child] = self._read_graph_manifest(child, raw)
                        descendants, imported = remote_dependencies[child]
                        flat.update(descendants)
                        # That manifest was published after its imports too.
                        published.update(imported)
                    else:
                        descendants, imported = edges[child]
                        children.extend(descendants - flat)
                    imports.update(imported)
                manifests[root] = (flat, imports)

            # Publish imported manifests before their users, so observing a
            # manifest is sufficient to stop a later upload at that boundary.
            while manifests:
                ready = [ref for ref, (_, imports) in manifests.items() if imports <= published]
                if not ready:
                    raise ValueError("Cannot publish cyclic graph imports")
                for offset in range(0, len(ready), self.n_workers):
                    batch = ready[offset:offset + self.n_workers]
                    writes = []
                    for root in batch:
                        flat, imports = manifests[root]
                        payload = _json(dict(schema=1, root=root.to, subdags=sorted(ref.to for ref in flat),
                                             imports=sorted(ref.to for ref in imports)))
                        writes.append(pool.submit(self._put_immutable, self._manifest_key(root), payload))
                    for future in writes:
                        future.result()
                    published.update(batch)
                for root in ready:
                    del manifests[root]

    def _put_cas(
        self,
        ref: Ref,
        ref_path: str | CasItem | None,
        db: DmlDB,
        exists_ok: bool = True,
        meta=None,
        *,
        missing_commits: set[Ref] | None = None,
        allow_shallow: bool = False,
    ) -> None:
        if ref.ns() != "commit":
            self._upload_packs({ref}, db)
        else:
            # New/forced refs still require complete *local* ancestry. Only
            # shallow repositories need this policy check; normal incremental
            # uploads stop at the first existing remote commit.
            if missing_commits and not allow_shallow:
                pending_history, checked = [ref], set()
                with db.tx(readonly=True) as txn:
                    while pending_history:
                        current = pending_history.pop()
                        if current in checked:
                            continue
                        checked.add(current)
                        if current in missing_commits and not txn.exists(current):
                            raise DmlRepoError(
                                "Cannot publish shallow history; fetch with greater depth or --unshallow"
                            )
                        pending_history.extend(txn.get(current).parents)

            manifests: dict[Ref, str] = {}
            parents: dict[Ref, set[Ref]] = {}
            roots: set[Ref] = set()
            published: set[Ref] = set()
            pending = {ref}
            with ThreadPoolExecutor(max_workers=self.n_workers) as pool:
                while pending:
                    batch = sorted(pending)[:self.n_workers]
                    pending.difference_update(batch)
                    present = list(pool.map(self._store._exists, [self._commit_key(item) for item in batch]))
                    published.update(item for item, exists in zip(batch, present, strict=True) if exists)
                    missing = [item for item, exists in zip(batch, present, strict=True) if not exists]
                    if not missing:
                        continue
                    with db.tx(readonly=True) as txn:
                        for current in missing:
                            if current in (missing_commits or set()) and not txn.exists(current):
                                raise DmlRepoError("Remote is missing the shallow history boundary")
                            commit = txn.get(current)
                            if not isinstance(commit, Commit):
                                raise ValueError("Expected a commit")
                            tree = txn.get(commit.tree)
                            if not isinstance(tree, Tree):
                                raise ValueError("Expected a commit tree")
                            manifests[current] = _json({
                                "schema": 1, "commit": [current.to, self._dump_cas_object(commit)],
                                "tree": [commit.tree.to, self._dump_cas_object(tree)],
                                "dags": {name: dag.to for name, dag in tree.dags.items()},
                            })
                            parents[current] = set(commit.parents)
                            roots.update(tree.dags.values())
                            pending.update(commit.parents)
                    pending.difference_update(manifests.keys() | published)
                if roots:
                    self._upload_packs(roots, db)
                # Existing commit manifests must imply durable history too.
                # Never publish a child until all its parents are on the remote.
                while manifests:
                    ready = [item for item in manifests if parents[item] <= published]
                    if not ready:
                        raise ValueError("Cannot publish cyclic commit history")
                    for offset in range(0, len(ready), self.n_workers):
                        batch = ready[offset:offset + self.n_workers]
                        writes = [pool.submit(self._put_immutable, self._commit_key(item), manifests[item])
                                  for item in batch]
                        for future in writes:
                            future.result()
                        published.update(batch)
                    for item in ready:
                        del manifests[item]
        if ref_path is not None:
            ref_key = ref_path if isinstance(ref_path, CasItem) else self._store._key_for(ref_path)
            self._store._put_js(
                ref_key,
                self._build_ref_payload(ref, meta),
                overwrite=exists_ok,
            )

    def _ref_key(self, kind: Literal["tag", "branch"], name: str) -> str:
        kind_dir = "tags" if kind == "tag" else "heads"
        return f"refs/{kind_dir}/{quote(name, safe='')}.json"

    def _tombstone_key(self) -> str:
        return f"refs/tombstone/{uuid7().hex}.json"

    def _collect_direct_refs(self, obj, deps: set[Ref]) -> None:
        if isinstance(obj, Ref):
            deps.add(obj)
            return
        if isinstance(obj, dict):
            for value in obj.values():
                self._collect_direct_refs(value, deps)
            return
        if isinstance(obj, list):
            for value in obj:
                self._collect_direct_refs(value, deps)
            return
        if is_dataclass(obj):
            for field_def in fields(obj):
                self._collect_direct_refs(getattr(obj, field_def.name), deps)
            return
        if isinstance(obj, _MANIFEST_SCALAR_TYPES):
            return
        raise ValueError(f"Unsupported object type in remote object graph: {type(obj)!r}")

    def _fetch_manifest_objects(
        self,
        manifest: dict,
        db: DmlDB,
        *,
        expected_root_ns: str | None = None,
        required_metadata: tuple[str, ...] = (),
    ) -> tuple[Ref, list[tuple[Ref, DmlBase]]]:
        """Fetch the manifest closure before replayable local materialization."""
        root_ref = self._validate_ref_payload(
            manifest,
            expected_root_ns=expected_root_ns,
            required_metadata=required_metadata,
        )
        if root_ref.ns() == "commit":
            _, objects, _, _ = self._fetch_project_commit_objects(manifest, db)
            return root_ref, objects
        with db.tx(readonly=True) as txn:
            if self._local_graph_complete(root_ref, txn):
                return root_ref, []
        objects = self._fetch_packs({root_ref}, db)
        return root_ref, list(objects.items())

    def _local_graph_complete(self, root: Ref, txn: TxnWithValid, complete: set[Ref] | None = None) -> bool:
        if complete is None:
            complete = set()
        reached, pending = set(), [root]
        while pending:
            ref = pending.pop()
            if ref in reached or ref in complete:
                continue
            if not txn.exists(ref):
                return False
            reached.add(ref)
            deps: set[Ref] = set()
            self._collect_direct_refs(txn.get(ref), deps)
            pending.extend(deps - reached)
        complete.update(reached)
        return True

    @contextmanager
    def _download_stream(self, roots: set[Ref], key):
        """Transport only in workers; bounded completion-order consumption on caller."""
        ready = Queue(maxsize=2 * self.n_workers)
        stop = Event()

        def download(ref):
            if stop.is_set():
                return
            try:
                item = (ref, self._store._get_bytes(key(ref)), None)
            except Exception as exc:
                item = (ref, None, exc)
            while not stop.is_set():
                try:
                    ready.put(item, timeout=0.1)
                    return
                except Full:
                    pass

        def completed():
            remaining = len(roots)
            while remaining:
                batch = [ready.get()]
                while len(batch) < min(self.n_workers, remaining):
                    try:
                        batch.append(ready.get_nowait())
                    except Empty:
                        break
                remaining -= len(batch)
                for _, _, error in batch:
                    if error is not None:
                        raise error
                yield [(ref, raw) for ref, raw, _ in batch]

        with ThreadPoolExecutor(max_workers=self.n_workers) as pool:
            for ref in sorted(roots):
                pool.submit(download, ref)
            try:
                yield completed()
            finally:
                stop.set()

    def _read_graph_manifest(self, root: Ref, raw: bytes) -> tuple[set[Ref], set[Ref]]:
        data = json.loads(raw)
        if (not isinstance(data, dict) or set(data) != {"schema", "root", "subdags", "imports"}
                or type(data["schema"]) is not int or data["schema"] != 1 or data["root"] != root.to):
            raise ValueError("Invalid graph manifest")
        for field_name in ("subdags", "imports"):
            values = data[field_name]
            if (not isinstance(values, list) or any(not isinstance(value, str) for value in values)
                    or values != sorted(set(values))):
                raise ValueError("Manifest dependencies must be sorted unique refs")
        return ({_immutable_ref(value, "dag") for value in data["subdags"]},
                {_immutable_ref(value, "dag") for value in data["imports"]})

    def _merge_objects(self, destination, objects):
        for ref, obj in objects.items():
            if ref in destination and self._dump_cas_object(destination[ref]) != self._dump_cas_object(obj):
                raise ValueError(f"Remote CAS object identity mismatch: conflicting packed object {ref}")
            destination[ref] = obj

    def _fetch_packs(self, roots: set[Ref], db: DmlDB) -> dict[Ref, DmlBase]:
        manifests = {}
        pending = set(roots)
        while pending:
            with self._download_stream(pending, self._manifest_key) as completed:
                for batch in completed:
                    for root, raw in batch:
                        manifests[root] = self._read_graph_manifest(root, raw)
            pending = set().union(*(imports for _, imports in manifests.values())) - manifests.keys()
        functions = set().union(*(children for children, _ in manifests.values()))
        # A function DAG can also be a named/imported DAG. Download its body
        # once and delay it until final validation like other manifest roots.
        edges = {}
        objects = {}
        available: set[Ref] = set()
        complete: set[Ref] = set()

        def record_edges(root, get):
            _, children, imports = self._partition(root, get)
            if not children <= functions or not imports <= manifests.keys():
                raise ValueError("Manifest omits graph dependencies")
            edges[root] = (children, imports)

        # Resolve the full plan first, then filter every body kind once. Mere
        # root existence is insufficient after an interrupted streaming fetch.
        with db.tx(readonly=True) as txn:
            for root in sorted(functions | manifests.keys()):
                if self._local_graph_complete(root, txn, complete):
                    available.add(root)
                    record_edges(root, txn.get)

        phases = (functions - manifests.keys() - available, set(manifests) - available)
        for stage, phase in enumerate(phases):
            with self._download_stream(phase, self._pack_key) as completed:
                for batch in completed:
                    decoded = {}
                    for root, raw in batch:
                        body = self._decode_pack(root, raw)
                        record_edges(root, body.__getitem__)
                        self._merge_objects(decoded, body)
                    if stage == 0:
                        self._materialize_objects(list(decoded.items()), db)
                    else:
                        self._merge_objects(objects, decoded)
        for root, expected in manifests.items():
            flat, imports = set(), set(edges[root][1])
            pending_children = list(edges[root][0])
            while pending_children:
                child = pending_children.pop()
                if child in flat:
                    continue
                flat.add(child)
                pending_children.extend(edges[child][0] - flat)
                imports.update(edges[child][1])
            if expected != (flat, imports):
                raise ValueError("Manifest disagrees with graph dependencies")
        # Validate selected import nodes and all cross-pack edges before
        # returning publishable root bodies. Partial function writes are
        # harmless: subsequent imports check their closure before reuse.
        with db.tx(readonly=True) as txn:
            reached = set()
            pending_refs = list(roots)
            while pending_refs:
                ref = pending_refs.pop()
                if ref in reached or ref in complete:
                    continue
                reached.add(ref)
                if ref not in objects and not txn.exists(ref):
                    raise ValueError(f"Missing cross-pack dependency: {ref}")
                obj = objects[ref] if ref in objects else txn.get(ref)
                deps: set[Ref] = set()
                self._collect_direct_refs(obj, deps)
                pending_refs.extend(deps - reached)
        return objects

    def _materialize_objects(self, objects: list[tuple[Ref, DmlBase]], db: DmlDB) -> None:
        def write_objects(txn: TxnWithValid) -> None:
            for ref, obj in objects:
                if txn.exists(ref):
                    if self._dump_cas_object(txn.get(ref)) != self._dump_cas_object(obj):
                        raise ValueError(f"Remote CAS object identity mismatch: {ref}")
                    continue
                local_ref = txn.put(obj)
                if local_ref != ref:
                    raise ValueError(f"Remote CAS object identity mismatch: expected {ref}, got {local_ref}")

        db.write_with_growth(write_objects)

    def materialize_manifest(
        self,
        manifest: dict,
        db: DmlDB,
        *,
        expected_root_ns: str | None = None,
        required_metadata: tuple[str, ...] = (),
    ) -> Ref:
        """Materialize already-fetched remote objects with a replayable local write."""
        root_ref, objects = self._fetch_manifest_objects(
            manifest,
            db,
            expected_root_ns=expected_root_ns,
            required_metadata=required_metadata,
        )

        self._materialize_objects(objects, db)
        return root_ref

    def _fetch_project_commit_objects(
        self,
        manifest: dict,
        db: DmlDB,
        *,
        depth: int | None = None,
        unshallow: bool = False,
    ) -> tuple[Ref, list[tuple[Ref, DmlBase]], set[Ref], set[Ref]]:
        if depth is not None and depth <= 0:
            raise ValueError("depth must be a positive integer")
        if depth is not None and unshallow:
            raise ValueError("depth and unshallow cannot be selected together")
        root_ref = self._validate_ref_payload(manifest, expected_root_ns="commit")
        objects: dict[Ref, DmlBase] = {}
        dags: set[Ref] = set()
        available_commits: set[Ref] = set()
        omitted_commits: set[Ref] = set()

        with ThreadPoolExecutor(max_workers=self.n_workers) as pool, db.tx(readonly=True) as txn:
            root_exists = txn.exists(root_ref)
            pending_commits = [] if root_exists and depth is None and not unshallow else [(root_ref, 1)]
            seen_commits: set[Ref] = set()
            while pending_commits:
                batch = pending_commits[:self.n_workers]
                del pending_commits[:self.n_workers]
                reads = {}
                for ref, generation in batch:
                    if ref not in seen_commits:
                        seen_commits.add(ref)
                        reads[ref] = (generation, None if txn.exists(ref) else
                                      pool.submit(self._store._get, self._commit_key(ref)))
                for commit_ref, (generation, future) in reads.items():
                    if future is None:
                        commit_obj = txn.get(commit_ref)
                        tree = txn.get(commit_obj.tree)
                    else:
                        commit_obj, tree = self._read_commit(commit_ref, future.result())
                        objects[commit_ref] = commit_obj
                        if (commit_obj.tree in objects and
                                self._dump_cas_object(objects[commit_obj.tree]) != self._dump_cas_object(tree)):
                            raise ValueError(f"Conflicting commit tree: {commit_obj.tree}")
                        objects[commit_obj.tree] = tree
                    available_commits.add(commit_ref)
                    dags.update(dag for dag in tree.dags.values() if not self._local_graph_complete(dag, txn))
                    for parent in commit_obj.parents:
                        parent_available = txn.exists(parent) or parent in objects
                        if depth is not None and generation >= depth:
                            (available_commits if parent_available else omitted_commits).add(parent)
                        elif depth is None and not unshallow and parent_available:
                            available_commits.add(parent)
                        else:
                            pending_commits.append((parent, generation + 1))
        self._merge_objects(objects, self._fetch_packs(dags, db))

        return root_ref, list(objects.items()), available_commits, omitted_commits - available_commits

    def materialize_project_commit(
        self,
        manifest: dict,
        db: DmlDB,
        *,
        depth: int | None = None,
        unshallow: bool = False,
    ) -> tuple[Ref, set[Ref], set[Ref]]:
        """Materialize a project commit with optional bounded ancestry."""
        root_ref, objects, available_commits, omitted_commits = self._fetch_project_commit_objects(
            manifest,
            db,
            depth=depth,
            unshallow=unshallow,
        )

        self._materialize_objects(objects, db)
        return root_ref, available_commits, omitted_commits

    def materialize_project_commit_ref(
        self,
        ref: Ref,
        db: DmlDB,
        *,
        depth: int | None = None,
    ) -> tuple[Ref, set[Ref], set[Ref]]:
        if ref.ns() != "commit":
            raise ValueError(f"Expected commit ref, got {ref}")
        return self.materialize_project_commit(self._build_ref_payload(ref), db, depth=depth)

    def upload_object_graph(self, ref: Ref, db: DmlDB) -> None:
        """Upload a typed object graph without publishing a remote ref."""
        self._put_cas(ref, None, db)

    def materialize_ref(self, ref: Ref, db: DmlDB) -> Ref:
        """Materialize a typed root referenced by execution state."""
        return self.materialize_manifest(self._build_ref_payload(ref), db, expected_root_ns=ref.ns())

    def _read_ref(self, ref_path: str):
        return json.loads(self._store._get(self._store._key_for(ref_path)))

    def _get_ref_snapshot(self, ref_path: str, db: DmlDB, *, expected_ns: str) -> tuple[Ref, CasItem] | None:
        try:
            item = self._store._get(self._store._key_for(ref_path), cas=True)
        except self._store.client.exceptions.NoSuchKey:
            return None
        manifest = json.loads(item.data)
        self._validate_ref_payload(manifest, expected_root_ns=expected_ns)
        ref = self.materialize_manifest(manifest, db, expected_root_ns=expected_ns)
        return ref, item

    @overload
    def _get_path(
        self,
        ref_path: str,
        db: DmlDB,
        *,
        expected_ns: str | None = None,
        required_metadata: tuple[str, ...] = (),
        raw: Literal[False] = False,
    ) -> Ref | None: ...
    @overload
    def _get_path(
        self,
        ref_path: str,
        db: DmlDB | None = None,
        *,
        expected_ns: str | None = None,
        required_metadata: tuple[str, ...] = (),
        raw: Literal[True] = True,
    ) -> dict | None: ...
    def _get_path(
        self,
        ref_path: str,
        db: Optional[DmlDB] = None,
        *,
        expected_ns: str | None = None,
        required_metadata: tuple[str, ...] = (),
        raw: bool = False,
    ) -> Ref | dict | None:
        try:
            manifest = self._read_ref(ref_path)
        except self._store.client.exceptions.NoSuchKey:
            return None
        self._validate_ref_payload(manifest, expected_root_ns=expected_ns, required_metadata=required_metadata)
        if raw:
            return manifest
        assert db is not None, "DmlDB instance required to materialize manifest"
        return self.materialize_manifest(
            manifest,
            db,
            expected_root_ns=expected_ns,
            required_metadata=required_metadata,
        )

    def _del(self, ref_path: str) -> bool:
        full_key = self._store._key_for(ref_path)
        if not self._store._exists(full_key):
            return False
        data = self._store._get(full_key)
        self._store._put(self._store._key_for(self._tombstone_key()), data)
        return self._store._delete(full_key)

    def _get_live_keys(self, roots: set[Ref]) -> set[str]:
        """Mark physical keys from dependency metadata, without reading packs."""
        live: set[str] = set()
        pending = list(roots)
        while pending:
            ref = pending.pop()
            if ref.ns() == "commit":
                key = self._commit_key(ref)
                if key in live:
                    continue
                live.add(key)
                manifest = json.loads(self._store._get(key))
                commit = json.loads(manifest["commit"][1])
                pending.extend(_decode_cas_value(commit[1]["parents"]))
                pending.extend(Ref(value) for value in manifest["dags"].values())
            else:
                key = self._manifest_key(ref)
                if key in live:
                    continue
                live.add(key)
                live.add(self._pack_key(ref))
                manifest = json.loads(self._store._get(key))
                live.update(self._pack_key(Ref(value)) for value in manifest["subdags"])
                pending.extend(Ref(value) for value in manifest["imports"])
        return live

    def _delete_objects(self, keys: list[str]) -> None:
        for offset in range(0, len(keys), 1000):
            response = self._store.client.delete_objects(
                Bucket=self._store.bucket,
                Delete={"Objects": [{"Key": key} for key in keys[offset:offset + 1000]], "Quiet": True},
            )
            if errors := response.get("Errors"):
                details = "; ".join(
                    f"s3://{self._store.bucket}/{error['Key']}: {error.get('Code')} {error.get('Message', '')}"
                    for error in errors
                )
                raise DmlRepoError(f"Remote GC deletion failed: {details}")

    def put_ref(
        self,
        commit: Ref,
        kind: Literal["tag", "branch"],
        name: str,
        db: DmlDB,
        *,
        force: bool = False,
        missing_commits: set[Ref] | None = None,
    ) -> str:
        """Publish a direct project ref, protecting non-forced branch updates."""
        ref_path = self._ref_key(kind, name)
        if force:
            self._put_cas(commit, ref_path, db, missing_commits=missing_commits)
            return ref_path
        if kind == "tag":
            try:
                self._put_cas(
                    commit,
                    ref_path,
                    db,
                    exists_ok=False,
                    missing_commits=missing_commits,
                )
            except CasItemConflict as exc:
                raise DmlRepoError(f"Remote tag already exists: {name}") from exc
            return ref_path
        snapshot = self._get_ref_snapshot(ref_path, db, expected_ns="commit")
        try:
            if snapshot is None:
                self._put_cas(
                    commit,
                    ref_path,
                    db,
                    exists_ok=False,
                    missing_commits=missing_commits,
                )
            else:
                remote_commit, ref_item = snapshot
                if not CommitOps().is_ancestor(
                    remote_commit,
                    commit,
                    db=db,
                    missing_commits=missing_commits,
                ):
                    raise DmlRepoError("Cannot push non-fast-forward branch update; pull and merge or push with force")
                self._put_cas(
                    commit,
                    ref_item,
                    db,
                    missing_commits=missing_commits,
                    allow_shallow=True,
                )
        except CasItemConflict as exc:
            raise DmlRepoError("Remote branch was updated concurrently; fetch and retry") from exc
        return ref_path

    @overload
    def get_ref(
        self,
        kind: Literal["tag", "branch"],
        name: str,
        db: DmlDB,
        raw: Literal[False] = False,
    ) -> Ref | None: ...
    @overload
    def get_ref(
        self,
        kind: Literal["tag", "branch"],
        name: str,
        db: None = None,
        raw: Literal[True] = True,
    ) -> dict | None: ...
    def get_ref(
        self,
        kind: Literal["tag", "branch"],
        name: str,
        db: DmlDB | None = None,
        raw: bool = False,
    ) -> Ref | dict | None:
        if raw:
            return self._get_path(self._ref_key(kind, name), expected_ns="commit", raw=True)
        assert db is not None, "DmlDB instance required when raw=False"
        return self._get_path(self._ref_key(kind, name), db, expected_ns="commit", raw=False)

    def get_project_commit_ref(
        self,
        kind: Literal["tag", "branch"],
        name: str,
        db: DmlDB,
        *,
        depth: int | None = None,
        unshallow: bool = False,
    ) -> tuple[Ref, set[Ref], set[Ref]] | None:
        try:
            manifest = self._read_ref(self._ref_key(kind, name))
        except self._store.client.exceptions.NoSuchKey:
            return None
        return self.materialize_project_commit(manifest, db, depth=depth, unshallow=unshallow)

    def delete_ref(self, kind: Literal["tag", "branch"], name: str) -> bool:
        return self._del(self._ref_key(kind, name))

    def list_refs(self, kind: Literal["tag", "branch"] = "branch") -> list[str]:
        kind_dir = "tags" if kind == "tag" else "heads"
        prefix = f"refs/{kind_dir}/"
        names = []
        for key in self._store._iter(self._store._key_for(prefix)):
            name = unquote(key[len(self._store._key_for(prefix)) :][:-5])
            names.append(name)
        return sorted(names)

    def list_ref_tips(self, kind: Literal["tag", "branch"] = "branch") -> list[tuple[str, Ref]]:
        """List exact commit tips without materializing remote objects."""
        kind_dir = "tags" if kind == "tag" else "heads"
        prefix = f"refs/{kind_dir}/"
        full_prefix = self._store._key_for(prefix)
        tips = []
        for key in self._store._iter(full_prefix):
            if not key.endswith(".json"):
                raise DmlRepoError(f"Invalid remote {kind} ref path: {key}")
            name = unquote(key[len(full_prefix) : -5])
            try:
                payload = json.loads(self._store._get(key))
                if not isinstance(payload, dict):
                    raise ValueError("Remote ref payload must be an object")
                tip = self._validate_ref_payload(payload, expected_root_ns="commit")
                if re.fullmatch(r"[0-9a-f]{64}", tip.id()) is None:
                    raise ValueError("Remote commit ref must use a 64-character lowercase hexadecimal id")
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise DmlRepoError(f"Invalid remote {kind} ref: {name}") from exc
            tips.append((name, tip))
        return sorted(tips, key=lambda item: item[0])

    def gc(self, unrecognized: Literal["raise", "ignore"] = "raise") -> dict[str, int]:
        """Collect unreachable packs and manifests.

        ``unrecognized`` selects the response to keys under the immutable
        prefixes that DaggerML did not write: ``raise`` fails before anything is
        deleted, naming the URI; ``ignore`` logs the URI and never deletes it.
        """
        if unrecognized not in ("raise", "ignore"):
            raise ValueError(f"unrecognized must be 'raise' or 'ignore', got {unrecognized!r}")
        roots: set[Ref] = set()
        t1 = time.time()
        total_refs = 0
        # Capture candidates before marking, and give unpublished uploads a
        # grace period. As with execution-record maintenance, operators must
        # quiesce writers for a complete, consistent remote GC cycle.
        candidates = []
        for relative in ("packs/", "manifests/", "commits/"):
            for page in self._store.client.get_paginator("list_objects_v2").paginate(
                Bucket=self._store.bucket, Prefix=self._store._key_for(relative),
            ):
                candidates.extend(page.get("Contents", []))
        recognized = re.compile(r"packs/(?:dag|argv)/[0-9a-f]{64}\.json\.gz|"
                                r"manifests/(?:dag|node-argv)/[0-9a-f]{64}\.json|commits/[0-9a-f]{64}\.json")
        listed, candidates = candidates, []
        for candidate in listed:
            relative = candidate["Key"][len(self._store._key_for("")):]
            if recognized.fullmatch(relative):
                candidates.append(candidate)
                continue
            uri = f"s3://{self._store.bucket}/{candidate['Key']}"
            if unrecognized == "raise":
                raise DmlRepoError(f"Unrecognized immutable storage object: {uri}")
            logger.warning(f"Ignoring unrecognized immutable storage object: {uri}")
        refs_prefix = self._store._key_for("refs/")
        tombstone_prefix = self._store._key_for("refs/tombstone/")
        expired_tombstones = []
        for page in self._store.client.get_paginator("list_objects_v2").paginate(
            Bucket=self._store.bucket, Prefix=refs_prefix,
        ):
            for item in page.get("Contents", []):
                key = item["Key"]
                if (key.startswith(tombstone_prefix)
                        and item["LastModified"].timestamp() <= t1 - self.prune_age_seconds):
                    expired_tombstones.append(key)
                    continue
                payload = json.loads(self._store._get(key))
                roots.add(Ref(payload["ref"]["to"]))
                total_refs += 1
        exec_store = S3Remote(self.root_uri.rstrip("/") + "/exec", client=self._store.client)
        execution_prefix = exec_store._key_for("execution/")
        execution_keys = list(exec_store._iter(execution_prefix))
        parts_by_id: dict[str, set[str]] = {}
        for key in execution_keys:
            relative = key[len(execution_prefix) :]
            match = re.fullmatch(r"([^/]+)/(metadata|state|driver)\.json", relative)
            if match is None:
                raise DmlRepoError(f"Unsupported execution object: {key}")
            parts_by_id.setdefault(match.group(1), set()).add(match.group(2))
        records: dict[str, tuple[Any, Any, Any, tuple[CasItem, CasItem, CasItem]]] = {}
        for execution_id, parts in parts_by_id.items():
            if parts != {"metadata", "state", "driver"}:
                raise DmlRepoError(f"Incomplete execution record: {execution_id}")
            try:
                metadata_item = exec_store._get(
                    exec_store._key_for(f"execution/{execution_id}/metadata.json"), cas=True
                )
                state_item = exec_store._get(exec_store._key_for(f"execution/{execution_id}/state.json"), cas=True)
                driver_item = exec_store._get(exec_store._key_for(f"execution/{execution_id}/driver.json"), cas=True)
                metadata = json.loads(metadata_item.data)
                state = json.loads(state_item.data)
                driver = json.loads(driver_item.data)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise DmlRepoError(f"Invalid execution record: {execution_id}") from exc
            records[execution_id] = (metadata, state, driver, (metadata_item, state_item, driver_item))

        retained_ids: set[str] = set()
        for execution_id, (metadata, state, driver, _) in records.items():
            cache_key = metadata.get("cache_key")
            current_execution = None
            if cache_key is not None:
                try:
                    current_execution = exec_store._get(exec_store._key_for(f"cache/{cache_key}"))
                except Exception as exc:
                    if not exec_store._is_missing_error(exc):
                        raise
            if (
                current_execution == execution_id
                or cache_key is None
                or driver.get("lock") is not None
                or state.get("cancelation") is not None
                or state.get("invalidation") is not None
            ):
                retained_ids.add(execution_id)
        pending = list(retained_ids)
        while pending:
            _, state, _, _ = records[pending.pop()]
            for related in state["spawned_execution_ids"] + state["child_execution_ids"]:
                if related in records and related not in retained_ids:
                    retained_ids.add(related)
                    pending.append(related)

        for execution_id, (metadata, state, _driver, items) in records.items():
            if execution_id not in retained_ids:
                # Delete semantic state first so concurrent result publication
                # conflicts before immutable metadata can become partial.
                metadata_item, state_item, driver_item = items
                deleted_items: list[CasItem] = []
                for item in (state_item, driver_item, metadata_item):
                    if exec_store._delete(item):
                        deleted_items.append(item)
                        continue
                    # The three files form one logical record. Restore any parts
                    # already deleted when a later CAS detects concurrent mutation.
                    for deleted_item in reversed(deleted_items):
                        try:
                            exec_store._put(deleted_item.key, deleted_item.data, overwrite=False)
                        except CasItemConflict:
                            pass
                    break
                else:
                    continue
                try:
                    metadata = json.loads(
                        exec_store._get(exec_store._key_for(f"execution/{execution_id}/metadata.json"))
                    )
                    state = json.loads(exec_store._get(exec_store._key_for(f"execution/{execution_id}/state.json")))
                except Exception as exc:
                    if isinstance(exc, DmlRepoError) or exec_store._is_missing_error(exc):
                        raise DmlRepoError(f"Execution record changed during GC: {execution_id}") from exc
                    raise
            for value in (metadata["argv_ref"], state["result_ref"]):
                if value is not None:
                    roots.add(Ref(value))
            total_refs += 1
        live_keys = self._get_live_keys(roots)
        t2 = time.time()
        deletions = []
        cas_kept_live = 0
        retained_recent = 0
        for candidate in candidates:
            key = candidate["Key"]
            if key in live_keys:
                cas_kept_live += 1
                continue
            if candidate["LastModified"].timestamp() > t1 - self.prune_age_seconds:
                retained_recent += 1
                continue
            deletions.append(key)
        self._delete_objects(deletions + expired_tombstones)
        t3 = time.time()
        return {
            "tombstones-deleted": len(expired_tombstones),
            "cas-deleted": len(deletions),
            "cas-retained": cas_kept_live + retained_recent,
            "total-refs": total_refs,
            "gc-time": int(t3 - t1),
            "ref-enumeration-time": int(t2 - t1),
            "cas-enumeration-time": int(t3 - t2),
        }
