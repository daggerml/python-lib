"""Offline, source-preserving conversion of legacy S3 CAS remotes to graph packs.

PERMANENT POLICY: PREVIOUS VERSION -> CURRENT VERSION ONLY.
This tool must NEVER accumulate support for older versions or skip migration
steps. When the format advances, replace the supported source/destination pair;
do not retain earlier migration paths here. Users who are N versions behind
must install each intermediate version in order and run that version's migration
tool, completing every step of the full migration path before upgrading again.

Run with ``python -m daggerml._core.migrate_remote --help``. All writers,
workers, and GC must be stopped until migration and configuration cutover finish.
No local repository is needed. No source objects are written or deleted.
"""

import argparse
import hashlib
import json
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

from boto3.s3.transfer import TransferConfig

from daggerml._core.aws import get_client
from daggerml._core.exec_state import ExecutionState
from daggerml._core.remote import _REMOTE_DESCRIPTOR, Remote, _immutable_ref
from daggerml._core.s3_cas import S3Remote
from daggerml._core.types import DmlDB, DmlRepoError

_LEGACY_REMOTE_DESCRIPTOR = {
    "schema": 0,
    "hash": "sha256",
    "layout": "one-project-cas+refs+split-execution",
    "refs_prefix": "refs",
    "io_prefix": "io",
    "cas_prefix": "cas/sha256",
    "execution_prefix": "../exec",
}


def _endpoint(uri: str, client) -> S3Remote:
    parsed = urlsplit(uri)
    if (parsed.scheme != "s3" or not parsed.netloc or parsed.query or parsed.fragment
            or parsed.username or parsed.password or ":" in parsed.netloc):
        raise ValueError("Migration endpoints must be s3://bucket/optional-prefix URIs")
    return S3Remote(uri.rstrip("/"), client)


def _inventory(store: S3Remote) -> dict[str, dict]:
    prefix = store._key_for("")
    return {item["Key"][len(prefix):]: item
            for page in store.client.get_paginator("list_objects_v2").paginate(Bucket=store.bucket, Prefix=prefix)
            for item in page.get("Contents", [])}


def _fingerprint(inventory: dict[str, dict]) -> dict[str, tuple]:
    return {key: (item["ETag"], item["Size"], item["LastModified"]) for key, item in inventory.items()}


def _legacy_graph(remote: Remote, root, db: DmlDB) -> None:
    objects: dict = {}
    pending, visited = {root}, set()
    with db.tx(readonly=True) as txn, ThreadPoolExecutor(max_workers=remote.n_workers) as pool:
        while pending:
            batch = []
            while pending and len(batch) < remote.n_workers:
                ref = pending.pop()
                _immutable_ref(ref.to)
                if ref in visited:
                    continue
                visited.add(ref)
                if not txn.exists(ref):
                    batch.append(ref)
            reads = {ref: pool.submit(remote._store._get, remote._cas_key(ref.id())) for ref in batch}
            for ref, future in reads.items():
                obj = remote._load_cas_object(ref, future.result())
                objects[ref] = obj
                deps = set()
                remote._collect_direct_refs(obj, deps)
                pending.update(deps - visited)
    remote._materialize_objects(list(objects.items()), db)


def _verify_destination(remote: Remote, db: DmlDB, roots: set) -> None:
    """Read every physical pack, including argv packs also embedded in DAGs."""
    for kind, namespace in (("dag", "dag"), ("argv", "node-argv")):
        prefix = remote._store._key_for(f"packs/{kind}/")
        for key in remote._store._iter(prefix):
            oid = key[len(prefix):].removesuffix(".json.gz")
            root = _immutable_ref(f"{namespace}:{oid}")
            objects = remote._decode_pack(root, remote._store._get_bytes(key))
            remote._materialize_objects(list(objects.items()), db)
    # Validate every manifest against its bodies and cross-pack dependencies,
    # not just the independently valid bounded packs.
    for namespace in ("dag", "node-argv"):
        prefix = remote._store._key_for(f"manifests/{namespace}/")
        for key in remote._store._iter(prefix):
            root = _immutable_ref(f"{namespace}:" + key[len(prefix):].removesuffix(".json"))
            objects = remote._fetch_packs({root}, db)
            remote._materialize_objects(list(objects.items()), db)
    commits = []
    prefix = remote._store._key_for("commits/")
    for key in remote._store._iter(prefix):
        root = _immutable_ref("commit:" + key[len(prefix):].removesuffix(".json"))
        commit, tree = remote._read_commit(root, remote._store._get(key))
        remote._materialize_objects([(root, commit), (commit.tree, tree)], db)
        commits.append((commit, tree))
    with db.tx(readonly=True) as txn:
        required = set(roots)
        for commit, tree in commits:
            required.update(commit.parents)
            required.update(tree.dags.values())
        if any(not txn.exists(ref) for ref in required):
            raise DmlRepoError("Converted remote has an incomplete graph or history")


def migrate(source_uri: str, destination_uri: str, *, workers: int = 8, dry_run: bool = False,
            client=None) -> dict[str, int]:
    """Convert a quiescent legacy root into a new empty, non-overlapping root.

    The destination descriptor is published only after every graph has been
    re-downloaded and identity-verified, auxiliary files copied, and the source
    inventory checked for mutation. Failure leaves an unpublished staging root;
    use a new empty destination for another attempt. Artifact URIs embedded in
    objects are deliberately not rewritten (which would change their identities).
    """
    if workers < 1:
        raise ValueError("workers must be positive")
    client = client or get_client("s3", max_pool_connections=workers)
    source, destination = _endpoint(source_uri, client), _endpoint(destination_uri, client)
    if source.bucket == destination.bucket:
        a, b = source.prefix.strip("/"), destination.prefix.strip("/")
        if not a or not b or a == b or a.startswith(b + "/") or b.startswith(a + "/"):
            raise DmlRepoError("Source and destination prefixes must not overlap")
    if destination._has_any(destination._key_for("")):
        raise DmlRepoError("Migration destination must be empty")
    before = _inventory(source)
    descriptor = json.loads(source._get(source._key_for("dml/dml.json")))
    if (not isinstance(descriptor, dict) or descriptor != _LEGACY_REMOTE_DESCRIPTOR
            or type(descriptor.get("schema")) is not int):
        raise DmlRepoError("Migration source must have the canonical legacy CAS descriptor")

    # These objects are deliberately not constructed through Remote's normal
    # initializer: legacy reads are migration-only, and staging is unpublished.
    old = object.__new__(Remote)
    old.root_uri, old.n_workers = source.s3_uri, workers
    old._store = S3Remote(source.s3_uri + "/dml", client)
    new = object.__new__(Remote)
    new.root_uri, new.n_workers = destination.s3_uri, workers
    new._store = S3Remote(destination.s3_uri + "/dml", client)
    roots = set()
    refs = {}
    records: dict[str, dict[str, dict]] = {}
    for relative in before:
        if (relative.startswith(("dml/packs/", "dml/manifests/", "dml/commits/"))
                or relative == "migration-in-progress.json"):
            raise DmlRepoError(f"Unexpected staged/new-format object in legacy source: {relative}")
        if relative.startswith("dml/refs/"):
            if not re.fullmatch(r"dml/refs/(heads|tags|tombstone)/[^/]+\.json", relative):
                raise DmlRepoError(f"Unsupported project ref: {relative}")
            payload = json.loads(source._get(source._key_for(relative)))
            root = old._validate_ref_payload(payload, expected_root_ns="commit")
            roots.add(root)
            refs[relative] = payload
        elif relative.startswith("exec/execution/"):
            match = re.fullmatch(r"exec/execution/([^/]+)/(metadata|state|driver)\.json", relative)
            if match is None:
                raise DmlRepoError(f"Unsupported execution record: {relative}")
            records.setdefault(match[1], {})[match[2]] = json.loads(source._get(source._key_for(relative)))
    for execution_id, record in records.items():
        if set(record) != {"metadata", "state", "driver"}:
            raise DmlRepoError(f"Incomplete execution record: {execution_id}")
        metadata = ExecutionState._validate_metadata(record["metadata"], execution_id)
        state = ExecutionState._validate_state(record["state"])
        driver = ExecutionState._validate_driver(record["driver"])
        if (state["lifecycle"] in {"pending", "running", "cancel-pending"} or driver["lock"] is not None
                or driver["adapter_state"] is not None and (driver["cleanup"] or {}).get("status") != "complete"):
            raise DmlRepoError(f"Finish/cancel execution and cleanup before migration: {execution_id}")
        for value in (metadata["argv_ref"], state["result_ref"]):
            if value is not None:
                roots.add(_immutable_ref(value))
    for relative in before:
        if relative.startswith("exec/cache/"):
            execution_id = source._get(source._key_for(relative))
            cache_key = relative.removeprefix("exec/cache/")
            if execution_id not in records or records[execution_id]["metadata"]["cache_key"] != cache_key:
                raise DmlRepoError(f"Cache pointer has no complete execution record: {relative}")

    stats = {"graph_roots": len(roots), "project_refs": len(refs), "executions": len(records), "copied_objects": 0}
    if dry_run:
        return stats
    marker = destination._key_for("migration-in-progress.json")
    destination._put_js(marker, {"source": source.s3_uri, "format": _REMOTE_DESCRIPTOR}, overwrite=False)
    with tempfile.TemporaryDirectory(prefix="dml-migrate-") as directory:
        for name in ("source-db", "verify-db"):
            (Path(directory) / name).mkdir()
        db = DmlDB(str(Path(directory) / "source-db"), 16 * 1024 * 1024, 1024 ** 4)
        verified = DmlDB(str(Path(directory) / "verify-db"), 16 * 1024 * 1024, 1024 ** 4)
        for database in (db, verified):
            with database.tx(create_if_missing=True):
                pass
        for root in sorted(roots):
            _legacy_graph(old, root, db)
            new.upload_object_graph(root, db)
        _verify_destination(new, verified, roots)

    for relative, item in before.items():
        if relative == "dml/dml.json" or relative.startswith("dml/cas/"):
            continue
        # Managed copy supports multipart objects as well as small records.
        client.copy({"Bucket": source.bucket, "Key": item["Key"]}, destination.bucket,
                    destination._key_for(relative), ExtraArgs={"CopySourceIfMatch": item["ETag"]},
                    Config=TransferConfig(max_concurrency=workers))
        copied = client.head_object(Bucket=destination.bucket, Key=destination._key_for(relative))
        if copied["ContentLength"] != item["Size"]:
            raise DmlRepoError(f"Copied object size mismatch: {relative}")
        # Small coordination records are verified byte-for-byte, artifacts stay
        # server-side and retain their URI identities at the untouched source.
        if relative.startswith(("dml/refs/", "exec/cache/", "exec/execution/", "exec/edges/")):
            original = source._get_bytes(item["Key"])
            converted = destination._get_bytes(destination._key_for(relative))
            if hashlib.sha256(original).digest() != hashlib.sha256(converted).digest():
                raise DmlRepoError(f"Copied record mismatch: {relative}")
        stats["copied_objects"] += 1
    if _fingerprint(_inventory(source)) != _fingerprint(before):
        raise DmlRepoError("Source changed during migration; destination remains unpublished")
    # Publish last. The marker keeps failed/incomplete destinations unusable by
    # normal clients even if the process stops before this point.
    new._store._put_js(new._store._key_for("dml.json"), _REMOTE_DESCRIPTOR, overwrite=False)
    destination._delete(marker)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, help="legacy S3 root (never modified)")
    parser.add_argument("--destination", required=True, help="new empty, non-overlapping S3 root")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--dry-run", action="store_true", help="inspect source and destination without writing")
    parser.add_argument("--quiescent", action="store_true", help="confirm all writers, workers, and GC are stopped")
    args = parser.parse_args()
    if not args.dry_run and not args.quiescent:
        parser.error("Stop writers/workers/GC and pass --quiescent before migration")
    print(json.dumps(migrate(args.source, args.destination, workers=args.workers, dry_run=args.dry_run), indent=2))


if __name__ == "__main__":
    main()
