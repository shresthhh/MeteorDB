#!/usr/bin/env bash
set -euo pipefail

missing=0

check_command() {
    local command_name=$1
    local install_hint=$2
    if command -v "$command_name" >/dev/null 2>&1; then
        printf 'ok: %s (%s)\n' "$command_name" "$(command -v "$command_name")"
    else
        printf 'error: %s is required; %s\n' "$command_name" "$install_hint" >&2
        missing=1
    fi
}

check_command cc "install a C compiler (Ubuntu/Debian: sudo apt install build-essential)"
check_command c++ "install a C++ compiler (Ubuntu/Debian: sudo apt install g++)"
check_command cmake "install CMake (Ubuntu/Debian: sudo apt install cmake)"
check_command pkg-config "install pkg-config (Ubuntu/Debian: sudo apt install pkg-config)"

libclang_found=0
if [[ -n ${LIBCLANG_PATH:-} ]] && compgen -G "$LIBCLANG_PATH/libclang.so*" >/dev/null; then
    libclang_found=1
elif command -v pkg-config >/dev/null 2>&1 && pkg-config --exists libclang 2>/dev/null; then
    libclang_found=1
elif command -v llvm-config >/dev/null 2>&1; then
    llvm_libdir=$(llvm-config --libdir 2>/dev/null || true)
    if [[ -n $llvm_libdir ]] && compgen -G "$llvm_libdir/libclang.so*" >/dev/null; then
        libclang_found=1
    fi
fi

if ((libclang_found)); then
    printf 'ok: libclang\n'
else
    printf '%s\n' \
        'error: libclang is required; install libclang-dev and set LIBCLANG_PATH to its library directory if auto-detection fails' >&2
    missing=1
fi

if ((missing)); then
    printf '%s\n' 'error: RocksDB benchmark dependencies are unavailable; Cargo was not invoked' >&2
    exit 1
fi
