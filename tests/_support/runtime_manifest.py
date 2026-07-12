from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Iterable


def build_manifest(*, repo_root: Path, roots: Iterable[str | Path]) -> dict[str, dict[str, int | str]]:
    manifest: dict[str, dict[str, int | str]] = {}
    for raw_root in roots:
        root = Path(raw_root)
        absolute_root = root if root.is_absolute() else repo_root / root
        if not absolute_root.exists():
            continue
        for path in sorted(item for item in absolute_root.rglob("*") if item.is_file()):
            stat = path.stat()
            key = path.resolve().relative_to(repo_root.resolve()).as_posix()
            manifest[key] = {
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Record or compare repository runtime file manifests")
    parser.add_argument("--roots", nargs="+")
    parser.add_argument("--output")
    parser.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"))
    args = parser.parse_args()
    if args.compare:
        before = json.loads(Path(args.compare[0]).read_text(encoding="utf-8"))
        after = json.loads(Path(args.compare[1]).read_text(encoding="utf-8"))
        if before != after:
            print(json.dumps({"before": before, "after": after}, indent=2, sort_keys=True))
            return 1
        print("runtime manifests are identical")
        return 0
    if not args.roots or not args.output:
        parser.error("--roots and --output are required unless --compare is used")
    payload = build_manifest(repo_root=Path.cwd(), roots=args.roots)
    Path(args.output).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"recorded {len(payload)} runtime files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
