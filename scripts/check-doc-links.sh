#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$repo"

python3 - <<'PY'
from pathlib import Path
from urllib.parse import unquote
import re
import sys

root = Path.cwd().resolve()
files = [Path("README.md"), Path("CONTRIBUTING.md"), Path("ROADMAP.md"),
         Path("SECURITY.md"), Path("CHANGELOG.md")]
files += sorted(Path("docs").rglob("*.md"))
files += sorted(Path("crates").rglob("README.md"))
pattern = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
errors = []

for source in files:
    text = source.read_text(encoding="utf-8")
    for raw in pattern.findall(text):
        target = raw.strip().split()[0].strip("<>")
        if not target or target.startswith(("#", "http://", "https://", "mailto:")):
            continue
        path_text = unquote(target.split("#", 1)[0])
        resolved = (source.parent / path_text).resolve()
        try:
            resolved.relative_to(root)
        except ValueError:
            errors.append(f"{source}: link escapes repository: {target}")
            continue
        if not resolved.exists():
            errors.append(f"{source}: missing target: {target}")

if errors:
    print("\n".join(errors), file=sys.stderr)
    raise SystemExit(1)
print(f"checked {len(files)} Markdown files")
PY
