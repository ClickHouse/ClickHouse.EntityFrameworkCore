#!/usr/bin/env python3
"""Find uncovered lines for a specific file from cobertura XML."""

import os
import sys
import xml.etree.ElementTree as ET
from glob import glob


def expand_paths(paths: list[str]) -> list[str]:
    expanded: list[str] = []
    for path in paths:
        matches = glob(path, recursive=True)
        expanded.extend(matches or [path])

    return [path for path in expanded if os.path.isfile(path)]


def main() -> int:
    if len(sys.argv) < 3:
        print(
            f"Usage: {sys.argv[0]} <path/to/coverage.cobertura.xml> ... <filename>",
            file=sys.stderr,
        )
        return 1

    target = sys.argv[-1]
    xml_paths = expand_paths(sys.argv[1:-1])
    if not xml_paths:
        print("No coverage XML files matched.", file=sys.stderr)
        return 1

    xml_path = max(xml_paths, key=os.path.getmtime)

    tree = ET.parse(xml_path)
    found = False

    for cls in tree.getroot().findall(".//class"):
        if target in cls.get("filename", ""):
            found = True
            uncovered = sorted(
                {
                    int(line.get("number", "0"))
                    for line in cls.findall(".//line")
                    if int(line.get("hits", 0)) == 0
                }
            )
            uncovered_lines = [
                str(line)
                for line in uncovered
                if line > 0
            ]

            if uncovered_lines:
                print(cls.get("filename"))
                print(f"  Uncovered lines: {', '.join(uncovered_lines)}")
            else:
                print(f"{cls.get('filename')}: fully covered")

    if not found:
        print(f"No classes matching '{target}' found in coverage report.", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
