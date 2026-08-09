#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$repo"

python3 - <<'PY'
from pathlib import Path
import re

required = {
    Path("crates/meteordb/src/lib.rs"): [
        "pub const PUBLIC_API_VERSION: u32 = 1;",
        "pub const DATABASE_FORMAT_VERSION: u32 = 1;",
    ],
    Path("README.md"): [
        "public API version `1`",
        "database format generation `1`",
    ],
    Path("docs/file-formats.md"): [
        "database format generation `1`",
        "SSTable | magic `METEOR01`, component version `2`",
    ],
}
errors = []
for path, needles in required.items():
    text = path.read_text(encoding="utf-8")
    for needle in needles:
        if needle not in text:
            errors.append(f"{path}: missing release-contract text: {needle}")

workspace_cargo = Path("Cargo.toml").read_text(encoding="utf-8")
crate_cargo = Path("crates/meteordb/Cargo.toml").read_text(encoding="utf-8")
if not re.search(r'(?m)^version = "1\.[0-9]+\.[0-9]+"$', workspace_cargo):
    errors.append("Cargo.toml: public API v1 requires a 1.x workspace version")
if "version.workspace = true" not in crate_cargo:
    errors.append("crates/meteordb/Cargo.toml: crate must inherit the workspace release version")

public_files = [
    Path("README.md"),
    Path("CHANGELOG.md"),
    Path("CONTRIBUTING.md"),
    Path("ROADMAP.md"),
    Path("SECURITY.md"),
    *sorted(Path("docs").rglob("*.md")),
    *sorted(Path("crates").rglob("README.md")),
]
banned = re.compile(
    r"pre-alpha|API and file-format compatibility are not guaranteed|"
    r"API and persistent formats may change without migration support|"
    r"file format and API may change without migration support",
    re.IGNORECASE,
)
for path in public_files:
    text = path.read_text(encoding="utf-8")
    if match := banned.search(text):
        errors.append(f"{path}: contradictory compatibility text: {match.group(0)}")

builder = Path("crates/meteordb/src/sstable/builder.rs").read_text(encoding="utf-8")
if "format version `1`" in builder:
    errors.append("TableBuilder::finish Rustdoc names stale SSTable format version 1")

if errors:
    raise SystemExit("\n".join(errors))
print(f"checked compatibility contract across {len(public_files)} public documents")
PY
