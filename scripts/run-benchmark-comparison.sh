#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$repo"

engine=
profile=
workload=
output=

usage() {
    cat >&2 <<'EOF'
usage: scripts/run-benchmark-comparison.sh --engine meteordb|rocksdb (--smoke | --workload FILE) [--output IGNORED_JSON]
EOF
    exit 2
}

while (($#)); do
    case $1 in
        --engine)
            (($# >= 2)) || usage
            engine=$2
            shift 2
            ;;
        --smoke)
            [[ -z $profile && -z $workload ]] || usage
            profile=smoke
            shift
            ;;
        --workload)
            (($# >= 2)) || usage
            [[ -z $profile && -z $workload ]] || usage
            workload=$2
            shift 2
            ;;
        --output)
            (($# >= 2)) || usage
            output=$2
            shift 2
            ;;
        *)
            usage
            ;;
    esac
done

[[ $engine == meteordb || $engine == rocksdb ]] || usage
[[ $profile == smoke || -n $workload ]] || usage

if [[ $engine == rocksdb ]]; then
    "$repo/scripts/check-rocksdb-bench-deps.sh"
fi

cargo_args=(run --quiet -p meteordb-rocks-bench)
if [[ $engine == rocksdb ]]; then
    cargo_args+=(--features rocksdb-engine)
fi
cargo_args+=(-- --engine "$engine")
if [[ $profile == smoke ]]; then
    cargo_args+=(--smoke)
else
    cargo_args+=(--workload "$workload")
fi

printf 'running:' >&2
printf ' %q' cargo "${cargo_args[@]}" >&2
printf '\n' >&2

if [[ -n $output ]]; then
    [[ $output != /* && $output != *..* ]] || {
        printf '%s\n' 'error: --output must be a repository-relative path without .. components' >&2
        exit 2
    }
    git check-ignore -q -- "$output" || {
        printf 'error: --output must be ignored by Git (recommended: .bench-results/<name>.json): %s\n' "$output" >&2
        exit 2
    }
    mkdir -p "$(dirname "$output")"
    cargo "${cargo_args[@]}" >"$output"
    printf 'wrote %s\n' "$output" >&2
else
    cargo "${cargo_args[@]}"
fi
