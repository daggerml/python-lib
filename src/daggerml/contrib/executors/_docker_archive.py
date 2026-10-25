"""Identify a single image without changing Docker-save or OCI archive metadata."""

import hashlib
import json
import re
import tarfile
from pathlib import Path
from typing import Any

from daggerml.api import DmlRepoError


def image_id_from_archive(path: Path) -> str:
    """Return the config SHA-256 for a single-image Docker-save or OCI archive.

    Parameters
    ----------
    path : Path
        Docker-save or OCI tar archive, optionally compressed.

    Returns
    -------
    str
        Local Docker image ID, not the registry manifest digest.
    """
    try:
        with tarfile.open(path, "r:*") as archive:
            def read(name: str) -> bytes:
                member = archive.getmember(name)
                if not member.isfile():
                    raise ValueError(f"archive metadata is not a regular file: {name}")
                stream = archive.extractfile(member)
                if stream is None:
                    raise ValueError(f"missing archive metadata: {name}")
                with stream:
                    return stream.read()

            def blob(descriptor: Any) -> bytes:
                digest = descriptor["digest"]
                if not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
                    raise ValueError("expected a SHA-256 OCI descriptor")
                data = read("blobs/sha256/" + digest.removeprefix("sha256:"))
                if "sha256:" + hashlib.sha256(data).hexdigest() != digest:
                    raise ValueError("OCI blob digest mismatch")
                return data

            # Modern Docker saves may contain both formats. Prefer the OCI index,
            # as modern Docker loaders do; do not infer IDs from blob filenames.
            if "oci-layout" in archive.getnames():
                if json.loads(read("oci-layout"))["imageLayoutVersion"] != "1.0.0":
                    raise ValueError("unsupported OCI layout version")
                manifest = json.loads(read("index.json"))
                seen = set()
                while "manifests" in manifest:
                    descriptors = manifest["manifests"]
                    if not isinstance(descriptors, list) or not descriptors:
                        raise ValueError("archive must contain exactly one image/platform")
                    # Several tag annotations can point at the same manifest.
                    if len({descriptor["digest"] for descriptor in descriptors}) != 1:
                        raise ValueError("archive must contain exactly one image/platform")
                    digest = descriptors[0]["digest"]
                    if digest in seen:
                        raise ValueError("cyclic OCI index")
                    seen.add(digest)
                    manifest = json.loads(blob(descriptors[0]))
                config = blob(manifest["config"])
            else:
                manifest = json.loads(read("manifest.json"))
                if not isinstance(manifest, list) or len(manifest) != 1:
                    raise ValueError("archive must contain exactly one image/platform")
                config = read(manifest[0]["Config"])
            # Hash the original bytes, never reserialized JSON.
            if not isinstance(json.loads(config), dict):
                raise ValueError("invalid image configuration")
            return "sha256:" + hashlib.sha256(config).hexdigest()
    except (OSError, tarfile.TarError, ValueError, KeyError, TypeError, IndexError) as exc:
        raise DmlRepoError(f"Cannot identify Docker archive image: {exc}") from exc
