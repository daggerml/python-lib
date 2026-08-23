"""Remote operations for CAS, direct project refs, and cache backed by S3."""

import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import InitVar, dataclass, field, fields, is_dataclass
from typing import TYPE_CHECKING, Any, Literal, Optional, overload
from urllib.parse import quote, unquote

from daggerml._core.commit import CommitOps
from daggerml._core.db import Ref
from daggerml._core.s3_cas import CasItem, CasItemConflict, S3Remote
from daggerml._core.types import NAMESPACES, Commit, DmlBase, DmlDB, DmlRepoError, TxnWithValid
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
    "schema": 2,
    "hash": "sha256",
    "layout": "one-project-cas+refs+split-execution",
    "refs_prefix": "refs",
    "io_prefix": "io",
    "cas_prefix": "cas/sha256",
    "execution_prefix": "../exec",
}


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
            if descriptor != _REMOTE_DESCRIPTOR:
                raise DmlRepoError("Unsupported remote descriptor; migrate this remote root before use")
        except self._store.client.exceptions.NoSuchKey as exc:
            if any(self._store._iter(self._store._key_for(""))):
                raise DmlRepoError("Remote root is not empty and has no supported descriptor") from exc
            try:
                self._store._put_js(descriptor_key, _REMOTE_DESCRIPTOR, overwrite=False)
            except CasItemConflict as conflict:
                descriptor = json.loads(self._store._get(descriptor_key))
                if descriptor != _REMOTE_DESCRIPTOR:
                    raise DmlRepoError(
                        "Unsupported remote descriptor; migrate this remote root before use"
                    ) from conflict

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
        if descriptor != _REMOTE_DESCRIPTOR:
            raise DmlRepoError("Unsupported remote descriptor; migrate this remote root before use")

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
        ref_data = payload.get("ref")
        if not isinstance(ref_data, dict):
            raise ValueError("Remote ref payload missing object field 'ref'")
        ref_to = ref_data.get("to")
        if not isinstance(ref_to, str):
            raise ValueError("Remote ref payload missing string field 'ref.to'")
        created = payload.get("created")
        if not isinstance(created, int):
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
        return obj

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

    def _upload_objects(self, objects: dict[str, str]) -> None:
        logger.info(f"Uploading {len(objects)} objects...")
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=self.n_workers) as pool:
            futures = [pool.submit(self._store._put, key, data) for key, data in objects.items()]
            for future in futures:
                future.result()
        t1 = time.time()
        logger.info(f"Uploaded {len(objects)} objects in {t1 - t0:.2f} seconds")

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
        objs, encountered_missing = self._collect_local_objects(ref, db, missing_commits)
        if encountered_missing and not allow_shallow:
            raise DmlRepoError("Cannot publish shallow history; fetch with greater depth or --unshallow")
        uploads = self._plan_upload(objs)
        if uploads:
            self._upload_objects(uploads)
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
        objects: list[tuple[Ref, DmlBase]] = []
        with db.tx(readonly=True) as txn:
            pending = {root_ref}
            visited: set[Ref] = set()
            while pending:
                ref = pending.pop()
                if ref in visited:
                    continue
                visited.add(ref)
                if txn.exists(ref):
                    continue
                obj = self._load_cas_object(ref, self._store._get(self._cas_key(ref.id())))
                # Retain fetched objects across write retries. Locally present
                # objects are intentionally not traversed; see sharp bits.
                objects.append((ref, obj))
                deps: set[Ref] = set()
                self._collect_direct_refs(obj, deps)
                pending.update(deps - visited)
        return root_ref, objects

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

        def write_objects(txn: TxnWithValid) -> Ref:
            for ref, obj in objects:
                if txn.exists(ref):
                    continue
                local_ref = txn.put(obj)
                if local_ref != ref:
                    raise ValueError(f"Remote CAS object identity mismatch: expected {ref}, got {local_ref}")
            return root_ref

        return db.write_with_growth(write_objects)

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
        objects: list[tuple[Ref, DmlBase]] = []
        loaded: dict[Ref, DmlBase] = {}
        available_commits: set[Ref] = set()
        omitted_commits: set[Ref] = set()

        with db.tx(readonly=True) as txn:
            def load(ref: Ref) -> tuple[DmlBase, bool]:
                if ref in loaded:
                    return loaded[ref], False
                if txn.exists(ref):
                    return txn.get(ref), True
                obj = self._load_cas_object(ref, self._store._get(self._cas_key(ref.id())))
                loaded[ref] = obj
                objects.append((ref, obj))
                return obj, False

            def fetch_complete_closure(start: Ref) -> None:
                pending = [start]
                visited: set[Ref] = set()
                while pending:
                    ref = pending.pop()
                    if ref in visited:
                        continue
                    visited.add(ref)
                    if txn.exists(ref):
                        continue
                    obj, _ = load(ref)
                    deps: set[Ref] = set()
                    self._collect_direct_refs(obj, deps)
                    pending.extend(deps - visited)

            root_exists = txn.exists(root_ref)
            pending_commits = [] if root_exists and depth is None and not unshallow else [(root_ref, 1)]
            seen_commits: set[Ref] = set()
            while pending_commits:
                commit_ref, generation = pending_commits.pop(0)
                if commit_ref in seen_commits:
                    continue
                seen_commits.add(commit_ref)
                commit_obj, _ = load(commit_ref)
                if not isinstance(commit_obj, Commit):
                    raise TypeError(f"Remote project commit decoded as {type(commit_obj).__name__}")
                available_commits.add(commit_ref)
                fetch_complete_closure(commit_obj.tree)
                for parent in commit_obj.parents:
                    parent_available = txn.exists(parent) or parent in loaded
                    if depth is not None and generation >= depth:
                        if parent_available:
                            available_commits.add(parent)
                        else:
                            omitted_commits.add(parent)
                        continue
                    if depth is None and not unshallow and parent_available:
                        available_commits.add(parent)
                        continue
                    pending_commits.append((parent, generation + 1))

        return root_ref, objects, available_commits, omitted_commits

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

        def write_objects(txn: TxnWithValid) -> Ref:
            for ref, obj in objects:
                if txn.exists(ref):
                    continue
                local_ref = txn.put(obj)
                if local_ref != ref:
                    raise ValueError(f"Remote CAS object identity mismatch: expected {ref}, got {local_ref}")
            return root_ref

        db.write_with_growth(write_objects)
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

    def _raw_ref_view(self, payload: dict) -> dict:
        if "meta" in payload:
            return payload
        return {**payload, "meta": payload["metadata"]}

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
            return self._raw_ref_view(manifest)
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

    def _get_live_oids(self, root_ref: Ref) -> set[str]:
        live_oids: set[str] = set()
        visited: set[Ref] = set()
        pending = [root_ref]
        while pending:
            ref = pending.pop()
            if ref in visited:
                continue
            visited.add(ref)
            live_oids.add(ref.id())
            try:
                raw = self._store._get(self._cas_key(ref.id()))
            except self._store.client.exceptions.NoSuchKey:
                logger.warning(f"Missing CAS object for live ref {ref}")
                continue
            deps: set[Ref] = set()
            self._collect_direct_refs(self._load_cas_object(ref, raw), deps)
            pending.extend(deps - visited)
        return live_oids

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

    def gc(self) -> dict[str, int]:
        live_oids: set[str] = set()
        tombstones_deleted = 0
        t1 = time.time()
        total_refs = 0
        refs_prefix = self._store._key_for("refs/")
        tombstone_prefix = self._store._key_for("refs/tombstone/")
        for key in self._store._iter(refs_prefix):
            if key.startswith(tombstone_prefix):
                continue
            payload = json.loads(self._store._get(key))
            root_ref = self._validate_ref_payload(payload)
            live_oids.update(self._get_live_oids(root_ref))
            total_refs += 1
        exec_store = S3Remote(self.root_uri.rstrip("/") + "/exec", client=self._store.client)
        execution_prefix = exec_store._key_for("execution/")
        execution_ids = {
            key[len(execution_prefix) :].removesuffix("/metadata.json")
            for key in exec_store._iter(execution_prefix)
            if key.endswith("/metadata.json")
        }
        for execution_id in execution_ids:
            metadata_item = exec_store._get(exec_store._key_for(f"execution/{execution_id}/metadata.json"), cas=True)
            state_item = exec_store._get(exec_store._key_for(f"execution/{execution_id}/state.json"), cas=True)
            driver_item = exec_store._get(exec_store._key_for(f"execution/{execution_id}/driver.json"), cas=True)
            metadata = json.loads(metadata_item.data)
            state = json.loads(state_item.data)
            driver = json.loads(driver_item.data)
            cache_key = metadata.get("cache_key")
            current_execution = None
            if cache_key is not None:
                try:
                    current_execution = exec_store._get(exec_store._key_for(f"cache/{cache_key}"))
                except Exception as exc:
                    if not exec_store._is_missing_error(exc):
                        raise
            retained = (
                current_execution == execution_id
                or cache_key is None
                or driver.get("lock") is not None
                or state.get("cancelation") is not None
                or state.get("invalidation") is not None
            )
            if not retained:
                # Delete semantic state first so concurrent result publication
                # conflicts before immutable metadata can become partial.
                if all(exec_store._delete(item) for item in (state_item, driver_item, metadata_item)):
                    continue
                try:
                    metadata = json.loads(
                        exec_store._get(exec_store._key_for(f"execution/{execution_id}/metadata.json"))
                    )
                except Exception as exc:
                    if not exec_store._is_missing_error(exc):
                        raise
                try:
                    state = json.loads(
                        exec_store._get(exec_store._key_for(f"execution/{execution_id}/state.json"))
                    )
                except Exception as exc:
                    if not exec_store._is_missing_error(exc):
                        raise
            for ref_field in ("argv_ref", "result_ref"):
                value = metadata.get(ref_field) if ref_field == "argv_ref" else state.get(ref_field)
                if value is not None:
                    live_oids.update(self._get_live_oids(Ref(value)))
            total_refs += 1
        t2 = time.time()
        cas_prefix = self._store._key_for("cas/sha256/")
        cas_deleted = 0
        cas_kept_live = 0
        for key in self._store._iter(cas_prefix):
            oid = key.rsplit("/", 1)[-1]
            if oid in live_oids:
                cas_kept_live += 1
                continue
            if self._store._delete(key):
                cas_deleted += 1
        t3 = time.time()
        return {
            "tombstones-deleted": tombstones_deleted,
            "cas-deleted": cas_deleted,
            "cas-retained": cas_kept_live,
            "total-refs": total_refs,
            "gc-time": int(t3 - t1),
            "ref-enumeration-time": int(t2 - t1),
            "cas-enumeration-time": int(t3 - t2),
        }
