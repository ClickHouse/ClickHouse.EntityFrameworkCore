#!/usr/bin/env python3
"""Per-file coverage summary from cobertura XML, sorted worst-first."""

import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from glob import glob
from collections import defaultdict


def expand_paths(paths: list[str]) -> list[str]:
    expanded: list[str] = []
    for path in paths:
        matches = glob(path, recursive=True)
        expanded.extend(matches or [path])

    return [path for path in expanded if os.path.isfile(path)]


def main() -> int:
    xml_paths: list[str] = []
    changed_ref: str | None = None
    i = 1

    while i < len(sys.argv):
        if sys.argv[i] == "--changed":
            changed_ref = (
                sys.argv[i + 1]
                if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith("-")
                else "HEAD"
            )
            if changed_ref != "HEAD":
                i += 1
            i += 1
        else:
            xml_paths.append(sys.argv[i])
            i += 1

    if not xml_paths:
        print(f"Usage: {sys.argv[0]} <coverage.xml> ... [--changed [ref]]", file=sys.stderr)
        return 1

    xml_paths = expand_paths(xml_paths)
    if not xml_paths:
        print("No coverage XML files matched.", file=sys.stderr)
        return 1

    xml_path = max(xml_paths, key=os.path.getmtime)
    changed_files: set[str] | None = None

    if changed_ref is not None:
        result = subprocess.run(
            ["git", "diff", "--name-only", changed_ref],
            capture_output=True,
            text=True,
            check=False,
        )
        changed_files = {os.path.basename(f) for f in result.stdout.strip().splitlines()}

    tree = ET.parse(xml_path)
    by_file: defaultdict[str, list[int]] = defaultdict(lambda: [0, 0])

    for cls in tree.getroot().findall(".//class"):
        lines = cls.findall(".//line")
        if not lines:
            continue

        filename = cls.get("filename", "")
        if changed_files is not None and os.path.basename(filename) not in changed_files:
            continue

        by_file[filename][0] += sum(1 for line in lines if int(line.get("hits", 0)) > 0)
        by_file[filename][1] += len(lines)

    if changed_files is not None and not by_file:
        print("No changed files found in coverage report.", file=sys.stderr)
        return 0

    for pct, path, covered, total in sorted(
        [
            (covered / total * 100 if total else 0, path, covered, total)
            for path, (covered, total) in by_file.items()
        ]
    ):
        print(f"{pct:5.1f}%  ({covered:3d}/{total:3d})  {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
