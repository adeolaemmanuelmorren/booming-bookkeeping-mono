"""Archive exact legacy source files without credentials or disposable runtimes."""

import hashlib
import json
from pathlib import Path
import tarfile
from datetime import datetime, timezone


ROOT = Path(__file__).resolve().parents[2]
SOURCES = [
    "tinybird",
    "tinybird-production",
    "cloudflare-workers/bigquery-tinybird-sync",
    "cloudflare-workers/jitsu-tinybird-ingest",
    "identity-gcp-ingest",
    "cloudflare-workers/reverse-proxy",
    "cloudflare-workers/marketing-webhooks",
]
EXCLUDED = {".git", "node_modules", ".wrangler", ".tinyb", ".df-credentials.json", "__pycache__"}


def include(path):
    if any(part in EXCLUDED for part in path.parts):
        return False
    if path.name.startswith(".env") and path.name != ".env.example":
        return False
    if path.name.startswith(".dev.vars"):
        return False
    absolute = ROOT / path
    return absolute.is_file() and not absolute.is_symlink()


def main():
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    destination = ROOT / "archives" / f"tinybird-before-v1-{stamp}"
    destination.mkdir(parents=True, mode=0o700)
    archive = destination / "implementation.tar.gz"
    manifest = []
    with tarfile.open(archive, "w:gz") as bundle:
        for source in SOURCES:
            for path in sorted((ROOT / source).rglob("*")):
                relative = path.relative_to(ROOT)
                if not include(relative):
                    continue
                content = path.read_bytes()
                manifest.append({"path": str(relative), "size": len(content), "sha256": hashlib.sha256(content).hexdigest()})
                bundle.add(path, arcname=str(relative), recursive=False)
    with tarfile.open(archive, "r:gz") as bundle:
        for entry in manifest:
            content = bundle.extractfile(entry["path"]).read()
            if hashlib.sha256(content).hexdigest() != entry["sha256"]:
                raise RuntimeError(f"Archive verification failed: {entry['path']}")
    if not manifest:
        raise RuntimeError("Archive contains no files")
    result = {"created_at": stamp, "files": manifest, "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(), "verified": True}
    (destination / "manifest.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"archive": str(archive), "files": len(manifest), "verified": True}))


if __name__ == "__main__":
    main()
