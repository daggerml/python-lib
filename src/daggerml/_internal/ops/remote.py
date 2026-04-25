"""Remote operations for CAS + refs backed by S3.

This module provides RemoteOps, a class that handles pushing and pulling
repository state to/from S3-backed remote storage.
"""

import base64
import hashlib
import json
import re
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field, fields, is_dataclass
from functools import wraps
from typing import Any, Literal

from daggerml._internal._db import Ref
from daggerml._internal.ops.base_ops import BaseOps
from daggerml._internal.types import Commit, DmlRepoError, Head, Tree

try:
    import boto3
except ImportError:
    boto3 = None


def _get_s3_client():
    if boto3 is None:
        raise ImportError("boto3 is required for RemoteOps but is not installed.")
    return boto3.client("s3")


class RemoteError(Exception):
    """Base exception for remote operations."""

    pass


class RefAlreadyExists(Exception):
    """Raised when attempting to create a ref that already exists."""

    pass


class InvalidOid(Exception):
    """Raised when an object ID is invalid."""

    pass


class InvalidManifest(Exception):
    """Raised when a manifest is invalid."""

    pass


class InvalidRef(Exception):
    """Raised when a ref is invalid."""

    pass


class MissingCasObject(Exception):
    """Raised when a CAS object is missing."""

    pass


class ShaMismatch(Exception):
    """Raised when SHA256 verification fails."""

    pass


@dataclass(frozen=True)
class _ManifestFetchResult:
    manifest_oid: str
    manifest: dict


@dataclass(frozen=True)
class _DagRefFetchResult:
    dag_id: str
    manifest_oid: str


@dataclass(frozen=True)
class _CasFetchResult:
    ns: str
    oid: str
    raw_bytes: bytes


_REMOTE_FETCH_WORKERS = 32


def _remote_boundary(action: str):
    """Convert public remote-operation failures into DmlRepoError."""

    def _decorate(fn):
        @wraps(fn)
        def _wrapped(self, *args, **kwargs):
            try:
                return fn(self, *args, **kwargs)
            except DmlRepoError:
                raise
            except Exception as exc:
                raise DmlRepoError(f"Remote {action} failed: {exc}") from exc

        return _wrapped

    return _decorate


@dataclass
class RemoteOps(BaseOps):
    """Remote operations for CAS + refs backed by S3.

    This class provides methods to push and pull repository state
    between local storage and remote S3-backed storage.
    """

    bucket: str
    prefix: str
    client: Any = field(default_factory=_get_s3_client)
    _IO_INVOKE_PRUNE_AGE_SECONDS: int = 24 * 3600

    @_remote_boundary("initialization")
    def __post_init__(self):
        if not isinstance(self.bucket, str) or not self.bucket:
            raise ValueError("Remote bucket is required")
        if not isinstance(self.prefix, str):
            raise ValueError("Remote prefix must be a string")
        self._ensure_remote_descriptor()
        super().__post_init__()

    def __put(self, key, value, **kwargs):
        self.client.put_object(Bucket=self.bucket, Key=key, Body=value, **kwargs)
        self.client.get_waiter("object_exists").wait(Bucket=self.bucket, Key=key)

    def _prefixed_key(self, relative_key: str) -> str:
        """Join configured prefix and relative key without leading slash when prefix is empty."""
        return f"{self.prefix}/{relative_key}" if self.prefix else relative_key

    def _ensure_remote_descriptor(self) -> None:
        """Ensure the remote prefix has a valid dml.json descriptor file.

        Creates the descriptor if missing.
        If present but invalid, this is a hard failure.
        """
        descriptor_key = f"{self.prefix}/dml.json" if self.prefix else "dml.json"
        expected_descriptor = {
            "schema": 0,
            "hash": "sha256",
            "layout": "cas+refs",
            "refs_prefix": "refs",
            "io_prefix": "io",
            "cas_prefix": "cas/sha256",
        }
        try:
            # Try to get existing descriptor
            response = self.client.get_object(Bucket=self.bucket, Key=descriptor_key)
            descriptor = json.loads(response["Body"].read().decode("utf-8"))
            # Check if it matches expected descriptor
            if descriptor != expected_descriptor:
                raise InvalidRef("Invalid remote descriptor")
        except self.client.exceptions.NoSuchKey:
            # Descriptor doesn't exist, create it
            descriptor_json = json.dumps(expected_descriptor, separators=(",", ":"), sort_keys=True)
            self.__put(descriptor_key, descriptor_json.encode("utf-8"), ContentType="application/json")

    @staticmethod
    def _validate_cache_key(cache_key: str) -> str:
        if not isinstance(cache_key, str) or not cache_key:
            raise ValueError("Invalid cache key: must be a non-empty string")
        if cache_key in {".", ".."} or "/" in cache_key or "\\" in cache_key:
            raise ValueError(f"Invalid cache key: {cache_key!r}")
        return cache_key

    @staticmethod
    def _validate_manifest_oid(manifest_oid: str) -> str:
        if not isinstance(manifest_oid, str) or not re.match(r"^[0-9a-f]{64}$", manifest_oid):
            raise InvalidOid(f"Invalid OID: must be 64 lowercase hex characters, got {manifest_oid!r}")
        return manifest_oid

    @staticmethod
    def _validate_dag_id(dag_id: str) -> str:
        if not isinstance(dag_id, str) or not re.match(r"^[0-9a-f]{64}$", dag_id):
            raise ValueError(f"Invalid DAG id: must be 64 lowercase hex characters, got {dag_id!r}")
        return dag_id

    def _dag_ref_path(self, dag_id: str) -> str:
        dag_id = self._validate_dag_id(dag_id)
        return f"dags/{dag_id}.json"

    def _dag_ref_key(self, dag_id: str) -> str:
        return self._prefixed_key(f"refs/{self._dag_ref_path(dag_id)}")

    def _cache_ref_path(self, cache_key: str) -> str:
        cache_key = self._validate_cache_key(cache_key)
        return f"cache/{cache_key}.json"

    def _cas_key(self, oid: str) -> str:
        """Generate CAS key for object ID with sharding.

        Parameters
        ----------
        oid : str
            Object ID (64-character lowercase hex string)

        Returns
        -------
        str
            S3 key for the CAS object

        Raises
        ------
        InvalidOid
            If oid is not a valid 64-character lowercase hex string
        """
        if not re.match(r"^[0-9a-f]{64}$", oid):
            raise InvalidOid(f"Invalid OID: must be 64 lowercase hex characters, got {oid!r}")
        aa = oid[:2]
        bb = oid[2:4]
        return self._prefixed_key(f"cas/sha256/{aa}/{bb}/{oid}")

    def _ref_key(self, ref_path: str) -> str:
        """Generate ref key for reference path.

        Parameters
        ----------
        ref_path : str
            Reference path

        Returns
        -------
        str
            S3 key for the ref

        Raises
        ------
        ValueError
            If ref_path contains path traversal sequences
        """
        if ref_path.startswith("/"):
            raise ValueError(f"Invalid ref path: cannot start with '/', got {ref_path!r}")
        segments = ref_path.split("/")
        if not segments or any(seg == "" for seg in segments):
            raise ValueError(f"Invalid ref path: empty path segment in {ref_path!r}")
        if any(seg in {".", ".."} for seg in segments):
            raise ValueError(f"Invalid ref path: forbidden path segment in {ref_path!r}")
        if any("\\" in seg for seg in segments):
            raise ValueError(f"Invalid ref path: path segments must not contain '\\\\': {ref_path!r}")

        # Only tags/cache refs are valid protocol refs.
        root = segments[0]
        if root not in {"tags", "cache"}:
            raise ValueError(f"Invalid ref path root: expected 'tags' or 'cache', got {root!r}")

        if root == "tags":
            if len(segments) != 3 or not segments[2].endswith(".json"):
                raise ValueError("Invalid tags ref path: expected tags/<name>/<version>.json")
            name = segments[1]
            version = segments[2][: -len(".json")]
            seg_re = r"^[a-z0-9][a-z0-9._-]{0,127}$"
            if not re.match(seg_re, name):
                raise ValueError(f"Invalid tag name: {name!r}")
            if not re.match(seg_re, version):
                raise ValueError(f"Invalid tag version: {version!r}")
        else:
            if len(segments) != 2 or not segments[1].endswith(".json"):
                raise ValueError("Invalid cache ref path: expected cache/<key>.json")
            cache_key = segments[1][: -len(".json")]
            self._validate_cache_key(cache_key)
        return self._prefixed_key(f"refs/{ref_path}")

    def _remote_has_cas(self, oid: str) -> bool:
        """Check if CAS object exists in remote storage.

        Parameters
        ----------
        oid : str
            Object ID (64-character lowercase hex string)

        Returns
        -------
        bool
            True if the object exists, False otherwise
        """
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._cas_key(oid))
            return True
        except self.client.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "404"):
                return False
            raise

    def _remote_get_cas(self, oid: str) -> bytes:
        """Get CAS object data from remote storage.

        Parameters
        ----------
        oid : str
            Object ID (64-character lowercase hex string)

        Returns
        -------
        bytes
            The object data

        Raises
        ------
        MissingCasObject
            If the object does not exist
        """
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._cas_key(oid))
            return response["Body"].read()
        except self.client.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "404"):
                raise MissingCasObject(f"CAS object {oid} not found") from None
            raise

    def _remote_put_cas(self, oid: str, data: bytes) -> None:
        """Put CAS object data to remote storage.

        Parameters
        ----------
        oid : str
            Object ID (64-character lowercase hex string)
        data : bytes
            The object data to store
        """
        self.__put(self._cas_key(oid), data)

    def _remote_get_ref(self, ref_path: str) -> bytes:
        """Get ref data from remote storage.

        Parameters
        ----------
        ref_path : str
            Reference path

        Returns
        -------
        bytes
            The ref data

        Raises
        ------
        RemoteError
            If the ref does not exist
        """
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._ref_key(ref_path))
            return response["Body"].read()
        except self.client.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "404"):
                raise RemoteError(f"Ref {ref_path} not found") from None
            raise

    def _remote_get_dag_ref(self, dag_id: str) -> bytes:
        dag_id = self._validate_dag_id(dag_id)
        ref_path = self._dag_ref_path(dag_id)
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._dag_ref_key(dag_id))
            return response["Body"].read()
        except self.client.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "404"):
                raise RemoteError(f"Ref {ref_path} not found") from None
            raise

    def _remote_put_ref(self, ref_path: str, data: bytes) -> None:
        """Put ref data to remote storage.

        Parameters
        ----------
        ref_path : str
            Reference path
        data : bytes
            The ref data to store

        Raises
        ------
        RefAlreadyExists
            If the ref already exists
        """
        # Check if ref already exists
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._ref_key(ref_path))
            raise RefAlreadyExists(f"Ref {ref_path} already exists")
        except self.client.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code not in ("NoSuchKey", "404"):
                raise

        self.__put(self._ref_key(ref_path), data, ContentType="application/json")

    def _remote_put_dag_ref(self, dag_id: str, data: bytes) -> None:
        dag_id = self._validate_dag_id(dag_id)
        ref_path = self._dag_ref_path(dag_id)
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._dag_ref_key(dag_id))
            raise RefAlreadyExists(f"Ref {ref_path} already exists")
        except self.client.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code not in ("NoSuchKey", "404"):
                raise

        self.__put(self._dag_ref_key(dag_id), data, ContentType="application/json")

    def _remote_delete_ref(self, ref_path: str) -> None:
        """Delete ref from remote storage.

        Parameters
        ----------
        ref_path : str
            Reference path
        """
        self.client.delete_object(Bucket=self.bucket, Key=self._ref_key(ref_path))

    def _decode_ref(self, data: bytes) -> dict:
        """Decode and validate ref data from bytes.

        Parameters
        ----------
        data : bytes
            JSON-encoded ref data

        Returns
        -------
        dict
            Decoded and validated ref object

        Raises
        ------
        InvalidRef
            If the data is not a valid ref
        """
        o = json.loads(data)
        if o.get("kind") != "ref":
            raise InvalidRef("Invalid ref: kind must be 'ref'")
        if o.get("schema") != 0:
            raise InvalidRef("Invalid ref: schema must be 0")
        target = o.get("target")
        if not isinstance(target, str) or not re.match(r"^[0-9a-f]{64}$", target):
            raise InvalidRef("Invalid ref: target must be 64 lowercase hex characters")
        created_at = o.get("created_at")
        if not isinstance(created_at, int):
            raise InvalidRef("Invalid ref: created_at must be an integer")
        targets = o.get("targets")
        if targets is not None:
            if not isinstance(targets, dict):
                raise InvalidRef("Invalid ref: targets must be an object")
            if set(targets) != {"dag"}:
                raise InvalidRef("Invalid ref: targets supports only the 'dag' namespace")
            dag_targets = targets["dag"]
            if not isinstance(dag_targets, list):
                raise InvalidRef("Invalid ref: targets.dag must be a sorted unique list of 64 lowercase hex ids")
            if dag_targets != sorted(dag_targets) or len(dag_targets) != len(set(dag_targets)):
                raise InvalidRef("Invalid ref: targets.dag must be a sorted unique list of 64 lowercase hex ids")
            for dag_id in dag_targets:
                if not isinstance(dag_id, str) or not re.match(r"^[0-9a-f]{64}$", dag_id):
                    raise InvalidRef("Invalid ref: targets.dag must be a sorted unique list of 64 lowercase hex ids")
        return o

    def _decode_manifest(self, data: bytes) -> dict:
        """Decode and validate manifest data from bytes.

        Parameters
        ----------
        data : bytes
            JSON-encoded manifest data

        Returns
        -------
        dict
            Decoded and validated manifest object

        Raises
        ------
        InvalidManifest
            If the data is not a valid manifest
        """
        o = json.loads(data)
        if o.get("kind") != "manifest":
            raise InvalidManifest("Invalid manifest: kind must be 'manifest'")
        if o.get("schema") != 0:
            raise InvalidManifest("Invalid manifest: schema must be 0")
        if "root-ns" not in o or "root-id" not in o:
            raise InvalidManifest("Invalid manifest: must have 'root-ns' and 'root-id'")
        closure = o.get("closure")
        if not isinstance(closure, dict):
            raise InvalidManifest("Invalid manifest: 'closure' must be a dict")
        for kind, ids in closure.items():
            if not isinstance(ids, list):
                raise InvalidManifest(f"Invalid manifest: closure['{kind}'] must be a list")
            if ids != sorted(ids):
                raise InvalidManifest(f"Invalid manifest: closure['{kind}'] must be sorted")
            if len(ids) != len(set(ids)):
                raise InvalidManifest(f"Invalid manifest: closure['{kind}'] must have no duplicates")
            for oid in ids:
                if not isinstance(oid, str) or not re.match(r"^[0-9a-f]{64}$", oid):
                    raise InvalidManifest(f"Invalid manifest: oid '{oid}' must be 64 lowercase hex characters")
        return o

    def _closure_union(self, closure: dict[str, list[str]]) -> set[str]:
        """Compute the union of all OIDs across all closure kinds.

        Parameters
        ----------
        closure : dict[str, list[str]]
            Closure mapping from kind to list of OIDs

        Returns
        -------
        set[str]
            Set of all unique OIDs across all kinds
        """
        union_oids = set()
        for oids in closure.values():
            union_oids.update(oids)
        return union_oids

    def _local_dump_dict(self, txn, root_ref) -> dict:
        closure: dict[str, dict[str, str]] = {}
        visited: set[Ref] = set()
        to_visit = [root_ref]

        while to_visit:
            ref = to_visit.pop()
            if ref in visited:
                continue
            visited.add(ref)
            closure.setdefault(ref.ns(), {})[ref.id()] = txn.txn.get(ref, raw=True)
            obj = txn.get(ref)
            self._collect_local_manifest_refs(obj, root_ref=root_ref, to_visit=to_visit, visited=visited)

        return {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": root_ref.ns(),
            "root-id": root_ref.id(),
            "closure": closure,
        }

    def _collect_local_manifest_refs(self, obj: Any, *, root_ref: Ref, to_visit: list[Ref], visited: set[Ref]) -> None:
        if isinstance(obj, Ref):
            if obj.ns() == "dag" and obj != root_ref:
                return
            if obj not in visited:
                to_visit.append(obj)
            return
        if isinstance(obj, dict):
            for value in obj.values():
                self._collect_local_manifest_refs(value, root_ref=root_ref, to_visit=to_visit, visited=visited)
            return
        if isinstance(obj, list):
            for value in obj:
                self._collect_local_manifest_refs(value, root_ref=root_ref, to_visit=to_visit, visited=visited)
            return
        if is_dataclass(obj):
            for field_def in fields(obj):
                self._collect_local_manifest_refs(
                    getattr(obj, field_def.name), root_ref=root_ref, to_visit=to_visit, visited=visited
                )

    def _local_has(self, txn, ns: str, id: str) -> bool:
        """Check if a local object exists in the given namespace.

        Parameters
        ----------
        txn : TxnContext
            Transaction context
        ns : str
            Namespace
        id : str
            Object ID

        Returns
        -------
        bool
            True if object exists, False otherwise
        """
        try:
            txn.get(Ref(f"{ns}:{id}"))
            return True
        except DmlRepoError:
            return False

    def _build_remote_manifest(
        self, local_manifest: dict, *, require_commit_root: bool = True, direct_dag_ids: list[str] | None = None
    ) -> tuple[dict, bytes]:
        """Build remote manifest dict and canonical bytes from local manifest.

        Parameters
        ----------
        local_manifest : dict
            Local manifest dictionary with closure as {ns: {id: dump_str}}

        Returns
        -------
        tuple[dict, bytes]
            Remote manifest dict and canonical JSON bytes

        Raises
        ------
        ValueError
            If root-ns is not "commit"
        """
        # Validate root namespace when requested (push requirement)
        root_ns = local_manifest["root-ns"]
        if require_commit_root and root_ns != "commit":
            raise ValueError(f"Cannot push non-commit root namespace: {root_ns!r}")

        root_id = local_manifest["root-id"]

        # Convert closure: {ns: {id: dump_str}} -> {ns: sorted([id...])}
        remote_closure = {}
        for ns, items in local_manifest["closure"].items():
            # Extract IDs, dedupe, and sort
            ids = list(set(items.keys()))
            ids.sort()
            remote_closure[ns] = ids
        if direct_dag_ids is not None:
            if direct_dag_ids:
                remote_closure["dag"] = sorted(set(direct_dag_ids))
            else:
                remote_closure.pop("dag", None)

        # Build remote manifest dict
        manifest_dict = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": root_ns,
            "root-id": root_id,
            "closure": remote_closure,
        }

        # Produce canonical bytes
        manifest_bytes = json.dumps(manifest_dict, separators=(",", ":"), sort_keys=True).encode("utf-8")

        return manifest_dict, manifest_bytes

    def _validate_targets(self, targets: dict[str, list[str]]) -> dict[str, list[str]]:
        if not isinstance(targets, dict):
            raise ValueError("Invalid targets: expected {'dag': [...]} mapping")
        if set(targets) != {"dag"}:
            raise ValueError("Invalid targets: expected only the 'dag' namespace")
        dag_ids = targets["dag"]
        if not isinstance(dag_ids, list):
            raise ValueError("Invalid targets: dag targets must be a sorted unique list of 64 lowercase hex ids")
        validated = [self._validate_dag_id(dag_id) for dag_id in dag_ids]
        if validated != sorted(validated) or len(validated) != len(set(validated)):
            raise ValueError("Invalid targets: dag targets must be a sorted unique list of 64 lowercase hex ids")
        return {"dag": validated}

    def _collect_direct_dag_ids_from_obj(
        self,
        obj: Any,
        *,
        root_ref: Ref,
        to_visit: list[Ref],
        visited: set[Ref],
        dag_ids: set[str],
    ) -> None:
        if isinstance(obj, Ref):
            if obj.ns() == "dag" and obj != root_ref:
                dag_ids.add(obj.id())
                return
            if obj not in visited:
                to_visit.append(obj)
            return
        if isinstance(obj, dict):
            for value in obj.values():
                self._collect_direct_dag_ids_from_obj(
                    value, root_ref=root_ref, to_visit=to_visit, visited=visited, dag_ids=dag_ids
                )
            return
        if isinstance(obj, list):
            for value in obj:
                self._collect_direct_dag_ids_from_obj(
                    value, root_ref=root_ref, to_visit=to_visit, visited=visited, dag_ids=dag_ids
                )
            return
        if is_dataclass(obj):
            for field_def in fields(obj):
                self._collect_direct_dag_ids_from_obj(
                    getattr(obj, field_def.name), root_ref=root_ref, to_visit=to_visit, visited=visited, dag_ids=dag_ids
                )

    def _direct_dag_ids(self, txn, root_ref: Ref) -> list[str]:
        if root_ref.ns() == "commit":
            commit: Commit = txn.get(root_ref)
            tree: Tree = txn.get(commit.tree)
            return sorted({dag_ref.id() for dag_ref in tree.dags.values()})

        dag_ids: set[str] = set()
        visited: set[Ref] = set()
        to_visit: list[Ref] = [root_ref]

        while to_visit:
            ref = to_visit.pop()
            if ref in visited:
                continue
            visited.add(ref)
            obj = txn.get(ref)
            self._collect_direct_dag_ids_from_obj(
                obj, root_ref=root_ref, to_visit=to_visit, visited=visited, dag_ids=dag_ids
            )

        return sorted(dag_ids)

    def _targets_for_root(self, txn, root_ref: Ref) -> dict[str, list[str]]:
        return {"dag": self._direct_dag_ids(txn, root_ref)}

    def _require_manifest_ref_targets(self, ref_obj: dict, ref_path: str) -> dict[str, list[str]]:
        targets = ref_obj.get("targets")
        if targets is None:
            raise InvalidRef(f"Invalid ref: manifest ref {ref_path} must include targets")
        return self._validate_targets(targets)

    def _put_ref_manifest_from_local_manifest(self, local_manifest: dict, root_ref: Ref, txn) -> str:
        direct_dag_ids = self._direct_dag_ids(txn, root_ref)
        for dag_id in direct_dag_ids:
            self._ensure_dag_ref_in_txn(Ref(f"dag:{dag_id}"), txn, ())

        self._push_upload_objects(local_manifest)
        _manifest_dict, manifest_bytes = self._build_remote_manifest(
            local_manifest, require_commit_root=False, direct_dag_ids=direct_dag_ids
        )
        manifest_id = hashlib.sha256(manifest_bytes).hexdigest()
        if not self._remote_has_cas(manifest_id):
            self._remote_put_cas(manifest_id, manifest_bytes)
        # If the root ref is a dag, also write its dag ref pointer file so that
        # _load_remote_dag can resolve it by dag_id.
        if root_ref.ns() == "dag":
            dag_id = self._validate_dag_id(root_ref.id())
            ref_obj = {
                "kind": "ref",
                "schema": 0,
                "target": manifest_id,
                "created_at": int(time.time()),
                "meta": {"dag": {"id": dag_id}},
            }
            ref_bytes = json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
            try:
                self._remote_put_dag_ref(dag_id, ref_bytes)
            except RefAlreadyExists:
                pass
        return manifest_id

    def _ensure_dag_ref_in_txn(self, dag_ref: Ref, txn, stack: tuple[str, ...]) -> bool:
        dag_id = self._validate_dag_id(dag_ref.id())
        if dag_id in stack:
            cycle = " -> ".join([*stack, dag_id])
            raise DmlRepoError(f"Cycle detected in DAG closure: {cycle}")

        try:
            self._remote_get_dag_ref(dag_id)
            return True
        except RemoteError:
            pass

        local_manifest = self._local_dump_dict(txn, dag_ref)
        if local_manifest.get("root-ns") != "dag":
            raise ValueError(f"Expected local dag manifest root namespace 'dag', got {local_manifest.get('root-ns')!r}")

        next_stack = (*stack, dag_id)
        for child_dag_id in self._direct_dag_ids(txn, dag_ref):
            self._ensure_dag_ref_in_txn(Ref(f"dag:{child_dag_id}"), txn, next_stack)

        self._push_upload_objects(local_manifest)
        _manifest_dict, manifest_bytes = self._build_remote_manifest(
            local_manifest, require_commit_root=False, direct_dag_ids=self._direct_dag_ids(txn, dag_ref)
        )
        manifest_oid = hashlib.sha256(manifest_bytes).hexdigest()
        if not self._remote_has_cas(manifest_oid):
            self._remote_put_cas(manifest_oid, manifest_bytes)

        ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": manifest_oid,
            "created_at": int(time.time()),
            "meta": {"dag": {"id": dag_id}},
        }
        ref_bytes = json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
        try:
            self._remote_put_dag_ref(dag_id, ref_bytes)
        except RefAlreadyExists:
            self._decode_ref(self._remote_get_dag_ref(dag_id))
            return True
        return True

    @_remote_boundary("manifest upload")
    def put_ref_manifest(self, root_ref: Ref) -> str:
        with self._tx(readonly=True) as txn:
            local_manifest = self._local_dump_dict(txn, root_ref)
            return self._put_ref_manifest_from_local_manifest(local_manifest, root_ref, txn)

    @_remote_boundary("manifest load")
    def load_ptr(self, manifest_oid: str, *, expected_root_ns: str | None = None) -> Ref:
        """Resolve a manifest OID, materialize closure locally, and return root ref."""
        with self._tx(readonly=False) as txn:
            return self.load_ptr_in_txn(manifest_oid, txn, expected_root_ns=expected_root_ns)

    def _fetch_manifest_result(self, manifest_oid: str) -> _ManifestFetchResult:
        manifest_oid = self._validate_manifest_oid(manifest_oid)
        manifest_bytes = self._remote_get_cas(manifest_oid)
        return _ManifestFetchResult(manifest_oid, self._decode_manifest(manifest_bytes))

    def _fetch_dag_ref_result(self, dag_id: str) -> _DagRefFetchResult:
        dag_ref = self._decode_ref(self._remote_get_dag_ref(dag_id))
        return _DagRefFetchResult(dag_id, dag_ref["target"])

    def _fetch_cas_result(self, ns: str, oid: str) -> _CasFetchResult:
        raw_bytes = self._remote_get_cas(oid)
        computed_hash = hashlib.sha256(raw_bytes).hexdigest()
        if computed_hash != oid:
            raise ShaMismatch(f"SHA256 mismatch for object {oid}: expected {oid}, got {computed_hash}")
        return _CasFetchResult(ns, oid, raw_bytes)

    def _put_local_cas_object(self, txn, ns: str, oid: str, raw_bytes: bytes) -> Ref:
        dump_str = base64.b64encode(raw_bytes).decode("ascii")
        return txn.txn.put(dump_str, ns=ns, raw=True)

    def load_ptr_in_txn(self, manifest_oid: str, txn, *, expected_root_ns: str | None = None) -> Ref:
        """Resolve a manifest OID and materialize closure using a provided transaction."""
        seen_manifests: set[str] = set()
        seen_dag_refs: set[str] = set()
        seen_objects: set[tuple[str, str]] = set()
        pending = set()
        root_ref: Ref | None = None

        def submit_manifest(pool, next_manifest_oid: str) -> None:
            next_manifest_oid = self._validate_manifest_oid(next_manifest_oid)
            if next_manifest_oid in seen_manifests:
                return
            seen_manifests.add(next_manifest_oid)
            pending.add(pool.submit(self._fetch_manifest_result, next_manifest_oid))

        def submit_dag_ref(pool, dag_id: str) -> None:
            if dag_id in seen_dag_refs:
                return
            seen_dag_refs.add(dag_id)
            pending.add(pool.submit(self._fetch_dag_ref_result, dag_id))

        def submit_object(pool, ns: str, oid: str) -> None:
            key = (ns, oid)
            if key in seen_objects:
                return
            if self._local_has(txn, ns, oid):
                seen_objects.add(key)
                return
            seen_objects.add(key)
            pending.add(pool.submit(self._fetch_cas_result, ns, oid))

        with ThreadPoolExecutor(max_workers=_REMOTE_FETCH_WORKERS) as pool:
            submit_manifest(pool, manifest_oid)

            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for fut in done:
                    result = fut.result()

                    if isinstance(result, _ManifestFetchResult):
                        manifest = result.manifest
                        current_root_ref = Ref(f"{manifest['root-ns']}:{manifest['root-id']}")
                        if root_ref is None:
                            root_ref = current_root_ref
                            if expected_root_ns is not None and root_ref.ns() != expected_root_ns:
                                raise ValueError(
                                    "Manifest root namespace mismatch: "
                                    f"expected {expected_root_ns!r}, got {root_ref.ns()!r}"
                                )
                        if manifest["root-ns"] == "dag":
                            submit_object(pool, "dag", manifest["root-id"])
                        for ns, ids in manifest["closure"].items():
                            if ns == "dag":
                                for dag_id in ids:
                                    submit_dag_ref(pool, dag_id)
                            else:
                                for oid in ids:
                                    submit_object(pool, ns, oid)
                        continue

                    if isinstance(result, _DagRefFetchResult):
                        submit_manifest(pool, result.manifest_oid)
                        continue

                    if isinstance(result, _CasFetchResult):
                        inserted_ref = self._put_local_cas_object(txn, result.ns, result.oid, result.raw_bytes)
                        if inserted_ref.ns() != result.ns or inserted_ref.id() != result.oid:
                            raise DmlRepoError(
                                f"Loaded object mismatch: expected {result.ns}:{result.oid}, got {inserted_ref}"
                            )
                        continue

                    raise AssertionError(f"Unhandled remote load result: {type(result)!r}")

        if root_ref is None:
            raise DmlRepoError("Remote manifest load produced no root")
        if not txn.exists(root_ref):
            raise DmlRepoError(f"Remote manifest load did not materialize root object: {root_ref}")
        return root_ref

    @_remote_boundary("cache get")
    def get_cache_ref(self, cache_key: str) -> str | None:
        """Read cache ref target manifest OID for a cache key."""
        ref_path = self._cache_ref_path(cache_key)
        try:
            ref_bytes = self._remote_get_ref(ref_path)
        except RemoteError:
            return None
        ref_obj = self._decode_ref(ref_bytes)
        self._require_manifest_ref_targets(ref_obj, ref_path)
        return ref_obj["target"]

    @_remote_boundary("cache put")
    def put_cache_ref(
        self, cache_key: str, target: str, *, overwrite: bool = False, targets: dict[str, list[str]]
    ) -> None:
        """Create or update a cache ref.

        Create when missing. If present:
        - no-op when existing target matches.
        - conflict unless overwrite=True.
        """
        target = self._validate_manifest_oid(target)
        targets = self._validate_targets(targets)
        ref_path = self._cache_ref_path(cache_key)
        ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": target,
            "created_at": int(time.time()),
            "targets": targets,
            "meta": {},
        }
        ref_bytes = json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
        try:
            self._remote_put_ref(ref_path, ref_bytes)
            return
        except RefAlreadyExists:
            existing_obj = self._decode_ref(self._remote_get_ref(ref_path))
            if existing_obj["target"] == target:
                return
            if not overwrite:
                raise
            self._remote_delete_ref(ref_path)
            self._remote_put_ref(ref_path, ref_bytes)

    @_remote_boundary("cache delete")
    def delete_cache_ref(self, cache_key: str) -> bool:
        """Delete cache ref by cache key."""
        ref_path = self._cache_ref_path(cache_key)
        try:
            self._remote_get_ref(ref_path)
        except RemoteError:
            return False
        self._remote_delete_ref(ref_path)
        return True

    @_remote_boundary("cache list")
    def list_cache_refs(self, limit: int | None = None) -> list[tuple[str, str]]:
        """List cache refs as (cache_key, target_oid) pairs."""
        refs = self.list("cache")
        out: list[tuple[str, str]] = []
        for ref_obj in refs:
            ref_path = ref_obj["ref_path"]
            filename = ref_path.split("/")[-1]
            if not filename.endswith(".json"):
                continue
            cache_key = filename[: -len(".json")]
            out.append((cache_key, ref_obj["target"]))
            if limit is not None and len(out) >= limit:
                break
        return out

    def _push_upload_objects(self, local_manifest: dict) -> None:
        """Upload missing CAS objects from local manifest closure.

        Iterates through all objects in the local manifest's closure,
        verifies SHA256 integrity, and uploads to remote storage if missing.

        Parameters
        ----------
        local_manifest : dict
            Local manifest dictionary with closure containing base64-encoded objects

        Raises
        ------
        ValueError
            If any object's SHA256 hash doesn't match its ID
        """
        closure = local_manifest.get("closure", {})

        for _ns, items in closure.items():
            for id_, dump_str in items.items():
                # Decode base64 to raw bytes
                raw = base64.b64decode(dump_str)

                # Verify SHA256 matches the ID
                computed_hash = hashlib.sha256(raw).hexdigest()
                if computed_hash != id_:
                    raise ShaMismatch(f"SHA256 mismatch for object {id_}: expected {id_}, got {computed_hash}")

                # Upload only if missing
                if not self._remote_has_cas(id_):
                    self._remote_put_cas(id_, raw)

    def _resolve_push_target(self, ref: Ref) -> tuple[Ref, str]:
        """Resolve a pushed ref into the commit root ref and remote ref path."""
        if ref.ns() == "head":
            with self._tx(readonly=True) as txn:
                head: Head = txn.get(ref)
            commit_ref = head.commit
            return commit_ref, f"tags/{ref.id()}/{commit_ref.id()}.json"
        raise ValueError(f"Unsupported ref namespace: {ref.ns()}. Expected 'head'.")

    @_remote_boundary("push")
    def push(self, ref: Ref) -> str:
        """Push a repository reference to remote storage.

        Parameters
        ----------
        ref : Ref
            Reference to push (Ref("head:<name>")).

        Returns
        -------
        str
            The ref path where the reference was published

        Raises
        ------
        ValueError
            If the reference namespace is unsupported
        RefAlreadyExists
            If the ref already exists remotely
        """
        root_ref, ref_path = self._resolve_push_target(ref)

        with self._tx(readonly=True) as txn:
            lm = self._local_dump_dict(txn, root_ref)
            targets = self._targets_for_root(txn, root_ref)
            manifest_dict, _manifest_bytes = self._build_remote_manifest(
                lm, require_commit_root=True, direct_dag_ids=targets["dag"]
            )
            expected_targets = {"dag": sorted(set(manifest_dict["closure"].get("dag", [])))}
            if targets != expected_targets:
                raise ValueError(f"Manifest targets mismatch: expected {expected_targets}, got {targets}")
            manifest_id = self._put_ref_manifest_from_local_manifest(lm, root_ref, txn)

        ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": manifest_id,
            "created_at": int(time.time()),
            "targets": targets,
            "meta": {},  # Optional metadata
        }
        ref_bytes = json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
        self._remote_put_ref(ref_path, ref_bytes)
        return ref_path

    @_remote_boundary("pull")
    def pull(self, ref_path: str) -> None:
        """Pull a repository reference from remote storage.

        Parameters
        ----------
        ref_path : str
            Reference path to pull (e.g., "tags/main/v1.json")

        Raises
        ------
        ValueError
            If the manifest has a non-commit root namespace
        RemoteError
            If the ref or manifest cannot be found
        """
        # Step 1: Get ref bytes
        ref_bytes = self._remote_get_ref(ref_path)

        # Step 2: Decode ref
        ref_obj = self._decode_ref(ref_bytes)
        self._require_manifest_ref_targets(ref_obj, ref_path)

        # Step 3-7: Materialize pointed commit manifest and write pulled head pointer
        remote_name = f"s3://{self.bucket}"
        if self.prefix:
            remote_name = f"s3://{self.bucket}/{self.prefix}"

        with self._tx(readonly=False) as txn:
            root_ref = self.load_ptr_in_txn(ref_obj["target"], txn, expected_root_ns="commit")
            self._local_put_head(txn, remote_name, ref_path, root_ref.id())

    @_remote_boundary("list")
    def list(self, prefix: str) -> list[dict]:
        """List remote refs for a given prefix.

        Parameters
        ----------
        prefix : str
            The prefix to list refs for ("tags" or "cache")

        Returns
        -------
        list[dict]
            List of dictionaries containing decoded ref information including
            meta data and inferred ref_path
        """
        if prefix not in {"tags", "cache"}:
            raise ValueError(f"Invalid list prefix: {prefix!r}. Expected 'tags' or 'cache'.")

        refs = []

        # List objects under refs/<prefix>/
        prefix_key = f"{self.prefix}/refs/{prefix}/" if self.prefix else f"refs/{prefix}/"

        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix_key):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]

                # Only process .json files
                if not key.endswith(".json"):
                    continue

                # Extract ref_path relative to refs/<prefix>/
                if self.prefix:
                    ref_path = key[len(f"{self.prefix}/refs/") :]
                else:
                    ref_path = key[len("refs/") :]

                # Get and decode the ref
                ref_bytes = self._remote_get_ref(ref_path)
                ref_obj = self._decode_ref(ref_bytes)
                self._require_manifest_ref_targets(ref_obj, ref_path)

                # Add inferred ref_path to the result
                ref_obj["ref_path"] = ref_path
                refs.append(ref_obj)

        return refs

    @_remote_boundary("prune")
    def prune(self) -> int:
        """Delete expired invoke transport blobs.

        Returns
        -------
        int
            Number of invoke blobs deleted
        """
        now = int(time.time())
        deleted_count = 0

        # Cleanup applies only to ephemeral invoke transport blobs.
        invoke_prefix = f"{self.prefix}/io/invoke/" if self.prefix else "io/invoke/"

        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=invoke_prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                last_modified = obj["LastModified"]
                age_seconds = now - int(last_modified.timestamp())
                if age_seconds < self._IO_INVOKE_PRUNE_AGE_SECONDS:
                    continue
                self.client.delete_object(Bucket=self.bucket, Key=key)
                deleted_count += 1

        return deleted_count

    def _gc_mark(self, *, malformed: Literal["raise", "warn", "ignore"] = "warn") -> set[str]:
        """Build the set of live OIDs by marking reachable objects from refs.

        Refs are the roots; manifests define reachability through their closure.

        Returns
        -------
        set[str]
            Set of all live OIDs (manifest targets + closure union)
        """
        if malformed not in {"raise", "warn", "ignore"}:
            raise ValueError(f"Invalid malformed policy: {malformed!r}")

        live_oids = set()
        worklist = []
        seen_manifests = set()

        def _malformed_detail(exc: Exception) -> str:
            msg = str(exc)
            for prefix in ("Invalid ref: ", "Invalid manifest: "):
                if msg.startswith(prefix):
                    return msg[len(prefix) :]
            return msg

        def _handle_malformed(message: str, *, delete_ref_path: str | None = None, delete_cas_oid: str | None = None):
            if malformed == "raise":
                raise DmlRepoError(message)
            if malformed == "warn":
                self._logger.warning(message)
            if delete_ref_path is not None:
                _safe_delete_ref(delete_ref_path)
            if delete_cas_oid is not None:
                _safe_delete_cas(delete_cas_oid)

        def _safe_delete_ref(ref_path: str):
            try:
                self._remote_delete_ref(ref_path)
            except Exception:
                pass

        def _safe_delete_cas(oid: str):
            try:
                self.client.delete_object(Bucket=self.bucket, Key=self._cas_key(oid))
            except Exception:
                pass

        def _visit_root_ref(ref_obj: dict):
            manifest_oid = ref_obj["target"]
            live_oids.add(manifest_oid)
            worklist.append(manifest_oid)

        for prefix in ("tags", "cache"):
            prefix_key = f"{self.prefix}/refs/{prefix}/" if self.prefix else f"refs/{prefix}/"
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix_key):
                if "Contents" not in page:
                    continue
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if not key.endswith(".json"):
                        continue
                    ref_path = key[len(f"{self.prefix}/refs/") :] if self.prefix else key[len("refs/") :]
                    try:
                        ref_obj = self._decode_ref(self._remote_get_ref(ref_path))
                        self._require_manifest_ref_targets(ref_obj, ref_path)
                        _visit_root_ref(ref_obj)
                    except InvalidRef as exc:
                        _handle_malformed(
                            f"Malformed ref refs/{ref_path}: {_malformed_detail(exc)}", delete_ref_path=ref_path
                        )
                    except MissingCasObject:
                        _safe_delete_ref(ref_path)

        while worklist:
            manifest_oid = worklist.pop()
            if manifest_oid in seen_manifests:
                continue
            seen_manifests.add(manifest_oid)
            try:
                manifest = self._decode_manifest(self._remote_get_cas(manifest_oid))
            except InvalidManifest as exc:
                _handle_malformed(
                    f"Malformed manifest {manifest_oid}: {_malformed_detail(exc)}", delete_cas_oid=manifest_oid
                )
                continue
            except MissingCasObject:
                continue

            if manifest.get("root-ns") == "dag":
                live_oids.add(manifest["root-id"])

            for ns, ids in manifest["closure"].items():
                if ns == "dag":
                    for dag_id in ids:
                        try:
                            dag_ref = self._decode_ref(self._remote_get_dag_ref(dag_id))
                        except RemoteError:
                            continue
                        except InvalidRef as exc:
                            _handle_malformed(
                                f"Malformed ref refs/{self._dag_ref_path(dag_id)}: {_malformed_detail(exc)}",
                                delete_ref_path=self._dag_ref_path(dag_id),
                            )
                            continue
                        child_manifest_oid = dag_ref["target"]
                        live_oids.add(child_manifest_oid)
                        try:
                            self._remote_get_cas(child_manifest_oid)
                        except MissingCasObject:
                            _safe_delete_ref(self._dag_ref_path(dag_id))
                            continue
                        worklist.append(child_manifest_oid)
                    continue
                for oid in ids:
                    live_oids.add(oid)
                    try:
                        raw = self._remote_get_cas(oid)
                    except MissingCasObject:
                        continue
                    if hashlib.sha256(raw).hexdigest() != oid:
                        _handle_malformed(
                            f"Malformed CAS {oid}: sha256 mismatch for stored bytes",
                            delete_cas_oid=oid,
                        )

        return live_oids

    def _gc_sweep(self, live_oids: set[str], min_age_seconds: int) -> dict[str, int]:
        """Perform GC sweep phase: delete unreferenced CAS objects older than safety window.

        Parameters
        ----------
        live_oids : set[str]
            Set of live OIDs that should not be deleted
        min_age_seconds : int
            Minimum age in seconds for objects to be eligible for deletion

        Returns
        -------
        dict[str, int]
            Summary with counts: {"deleted": n, "kept_live": n, "kept_young": n}
        """
        deleted = 0
        kept_live = 0
        kept_young = 0

        # Current time for age calculation
        now = int(time.time())

        # List all CAS objects under cas/sha256/
        cas_prefix = f"{self.prefix}/cas/sha256/" if self.prefix else "cas/sha256/"

        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=cas_prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                last_modified = obj["LastModified"]

                # Extract OID from key: {prefix}/cas/sha256/{aa}/{bb}/{oid}
                # The OID is the last component after the last '/'
                oid = key.split("/")[-1]

                # Validate that this looks like an OID (64 hex chars)
                if not re.match(r"^[0-9a-f]{64}$", oid):
                    raise InvalidOid(f"Invalid CAS key: expected trailing 64-char lowercase hex OID, got {key!r}")

                if oid in live_oids:
                    # Keep live objects
                    kept_live += 1
                    continue

                # Check age
                age_seconds = now - int(last_modified.timestamp())
                if age_seconds < min_age_seconds:
                    # Keep young objects
                    kept_young += 1
                    continue

                # Delete old, unreferenced object
                try:
                    self.client.delete_object(Bucket=self.bucket, Key=key)
                    deleted += 1
                except Exception:
                    # Skip deletion errors (object might have been deleted by another process)
                    pass

        return {
            "deleted": deleted,
            "kept_live": kept_live,
            "kept_young": kept_young,
        }

    @_remote_boundary("gc")
    def gc(
        self, min_age_seconds: int = 24 * 3600, *, malformed: Literal["raise", "warn", "ignore"] = "warn"
    ) -> dict[str, int]:
        """Run garbage collection on the remote storage.

        This performs mark-and-sweep GC where refs are the roots.
        First prunes expired invoke transport blobs under `io/invoke/**`, then marks live objects,
        then sweeps unreferenced objects older than the safety window.

        Parameters
        ----------
        min_age_seconds : int, optional
            Minimum age in seconds for unreferenced objects to be deleted.
            Defaults to 24 hours.
        malformed : {"raise", "warn", "ignore"}, optional
            Handling policy for malformed refs/manifests/CAS encountered during mark.
            Defaults to "warn".

        Returns
        -------
        dict[str, int]
            Summary with counts: {"deleted": n, "kept_live": n, "kept_young": n}
        """
        # First prune expired invoke transport blobs (independent from CAS/ref reachability)
        self.prune()

        # Mark phase: build set of live OIDs
        live_oids = self._gc_mark(malformed=malformed)

        # Sweep phase: delete unreferenced objects older than safety window
        return self._gc_sweep(live_oids, min_age_seconds)

    def _local_put_head(self, txn, remote_name: str, ref_path: str, commit_id: str) -> None:
        """Store head pointer for a remote ref path.

        Parameters
        ----------
        txn : TxnContext
            Transaction context
        remote_name : str
            Remote name (e.g., "s3://bucket/prefix")
        ref_path : str
            Reference path
        commit_id : str
            Commit ID to point to
        """
        txn.put(Head(commit=Ref(f"commit:{commit_id}")), to=Ref(f"head:{remote_name}/{ref_path}"))
