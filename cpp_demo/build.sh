#!/usr/bin/env bash
# Build and run cpp_demo/test.cpp against the paimon-c library.
#
# Cross-platform: works on both Linux (GNU ld) and macOS (Apple ld64).
#
# Usage:
#   ./build.sh                              # debug, link shared paimon_c
#   ./build.sh release                      # release, link shared paimon_c
#   ./build.sh release run                  # build then run
#   ./build.sh debug shared                 # explicit shared (= default)
#   ./build.sh debug static                 # link libpaimon_c.a (static)
#   ./build.sh release static run           # static + run
#
# Args (positional, all optional):
#   $1 = profile  : debug | release          (default: debug)
#   $2 = link     : shared | static | run    (default: shared)
#                   "run" alone is a shorthand for "shared run"
#   $3 = action   : run                      (omit to just build)
#
# The script does NOT call cargo. Build the Rust side first, e.g.:
#   source env.sh
#   cargo build -p paimon-c                 # produces both the dylib/.so and .a
#
# Why -p paimon-c (not plain cargo build):
#   The Python bindings crate enables `arrow/pyarrow`, which through Cargo
#   feature unification would pull libpython into libpaimon_c. Building only
#   the C bindings keeps the shared lib / .a free of Python.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ---------------------------------------------------------------------------
# Platform detection. macOS (Darwin) and Linux differ in: the shared-library
# extension, the linker (ld64 vs GNU ld), the system libs/frameworks Rust's
# staticlib leaves unresolved, and the inspection tools (otool/nm vs ldd/nm).
# ---------------------------------------------------------------------------
OS="$(uname -s)"
case "${OS}" in
    Darwin) IS_MACOS=1 ;;
    *)      IS_MACOS=0 ;;
esac

if [[ "${IS_MACOS}" -eq 1 ]]; then
    DYLIB_EXT="dylib"
    DEFAULT_CXX="clang++"
else
    DYLIB_EXT="so"
    DEFAULT_CXX="g++"
fi

PROFILE="${1:-debug}"
case "${PROFILE}" in
    debug|release) ;;
    *)
        echo "Unknown profile '${PROFILE}'. Use 'debug' or 'release'." >&2
        exit 2
        ;;
esac

# Allow `./build.sh debug run` as a shortcut for `./build.sh debug shared run`.
LINK="${2:-shared}"
ACTION="${3:-}"
if [[ "${LINK}" == "run" ]]; then
    ACTION="run"
    LINK="shared"
fi
case "${LINK}" in
    shared|static) ;;
    *)
        echo "Unknown link mode '${LINK}'. Use 'shared' or 'static'." >&2
        exit 2
        ;;
esac

INCLUDE_DIR="${WORKSPACE_ROOT}/bindings/c/include"
LIB_DIR="${WORKSPACE_ROOT}/target/${PROFILE}"
HEADER="${INCLUDE_DIR}/paimon.h"
SHARED_LIB="${LIB_DIR}/libpaimon_c.${DYLIB_EXT}"
STATIC_LIB="${LIB_DIR}/libpaimon_c.a"

if [[ ! -f "${HEADER}" ]]; then
    echo "Header not found: ${HEADER}" >&2
    echo "Run: (cd ${WORKSPACE_ROOT} && source env.sh && cargo build -p paimon-c$( [[ ${PROFILE} == release ]] && echo ' --release'))" >&2
    exit 1
fi

if [[ "${LINK}" == "static" ]]; then
    REQUIRED_LIB="${STATIC_LIB}"
else
    REQUIRED_LIB="${SHARED_LIB}"
fi
if [[ ! -f "${REQUIRED_LIB}" ]]; then
    echo "Library not found: ${REQUIRED_LIB}" >&2
    echo "Run: (cd ${WORKSPACE_ROOT} && source env.sh && cargo build -p paimon-c$( [[ ${PROFILE} == release ]] && echo ' --release'))" >&2
    exit 1
fi

CXX="${CXX:-${DEFAULT_CXX}}"
OUT="${SCRIPT_DIR}/test"

# -O0 -g for debug, -O2 -g for release. Frame pointers stay on either way to
# match the workspace-wide -C force-frame-pointers=yes setting.
if [[ "${PROFILE}" == "release" ]]; then
    OPT_FLAGS="-O2 -g"
else
    OPT_FLAGS="-O0 -g"
fi

# ---------------------------------------------------------------------------
# Native libs that Rust's staticlib does NOT include and that we must satisfy
# at the C++ link step. Discover the canonical list with:
#   1. cargo rustc -p paimon-c --crate-type=staticlib -- \
#          --print=native-static-libs 2>&1 | grep native-static-libs:
#   2. otool -L target/debug/libpaimon_c.dylib   (macOS)
#      ldd     target/debug/libpaimon_c.so       (Linux)
#
# Linux x86_64 needs: pthread / dl / m / rt (libc/runtime) + z (zlib).
# macOS uses Security/SystemConfiguration frameworks instead of OpenSSL
# (rustls/native-tls map onto the system Keychain), plus CoreFoundation and
# libiconv; there is no -lrt and zlib comes via -lz.
# ---------------------------------------------------------------------------
if [[ "${IS_MACOS}" -eq 1 ]]; then
    COMMON_LDFLAGS=(-framework Security -framework SystemConfiguration -framework CoreFoundation -liconv)
    STATIC_EXTRA_LDFLAGS=(-lz)
else
    COMMON_LDFLAGS=(-lpthread -ldl -lm -lrt)
    STATIC_EXTRA_LDFLAGS=(-lz)
fi

echo "Compiling test.cpp -> ${OUT} (os=${OS}, profile=${PROFILE}, link=${LINK}, cxx=${CXX})"

if [[ "${LINK}" == "static" ]]; then
    # Static link notes.
    #
    # The goal on both platforms is the same: link the whole Rust dependency
    # tree out of libpaimon_c.a but expose ONLY the `paimon_*` C ABI in the
    # final binary's exported/dynamic symbol table. Otherwise the vendored
    # OpenSSL inside the archive leaks ~500 EVP_*/SSL_*/X509_* symbols, and a
    # later dlopen'd library expecting a different OpenSSL resolves back into
    # ours and crashes. The linkers express this very differently:
    if [[ "${IS_MACOS}" -eq 1 ]]; then
        # ld64 (macOS):
        #   -force_load <archive>    : pull in every object (no -l: / -Bstatic;
        #                              ld64 prefers .a here since we pass the
        #                              full path and there is no .dylib by that
        #                              path). Equivalent to --whole-archive.
        #   -exported_symbols_list   : whitelist `_paimon_*` and hide the rest
        #                              (counterpart to the GNU version script).
        #   -no_warn_inits, platform_version mismatch: the vendored crypto .o
        #     files inside the .a may be stamped with a newer macOS SDK than
        #     the CLT linker; -w silences those harmless "built for newer
        #     macOS version" warnings without hiding real link errors.
        "${CXX}" -std=c++11 ${OPT_FLAGS} -fno-omit-frame-pointer \
            -Wall -Wextra -Wno-unused-parameter \
            -I "${INCLUDE_DIR}" \
            "${SCRIPT_DIR}/test.cpp" \
            -Wl,-force_load,"${STATIC_LIB}" \
            -Wl,-exported_symbols_list,"${SCRIPT_DIR}/paimon-export.exp" \
            -Wl,-w \
            "${COMMON_LDFLAGS[@]}" "${STATIC_EXTRA_LDFLAGS[@]}" \
            -o "${OUT}"
    else
        # GNU ld (Linux):
        #   -l:libpaimon_c.a         : force the archive even when a .so sits
        #                              next to it (without ":" ld prefers .so).
        #   --version-script         : whitelist `paimon_*`.
        #   --exclude-libs,ALL       : hide all symbols pulled from archives.
        "${CXX}" -std=c++11 ${OPT_FLAGS} -fno-omit-frame-pointer \
            -Wall -Wextra -Wno-unused-parameter \
            -I "${INCLUDE_DIR}" \
            "${SCRIPT_DIR}/test.cpp" \
            -L "${LIB_DIR}" \
            -Wl,-Bstatic -l:libpaimon_c.a -Wl,-Bdynamic \
            -Wl,--version-script="${SCRIPT_DIR}/paimon-export.ver" \
            -Wl,--exclude-libs,ALL \
            "${COMMON_LDFLAGS[@]}" "${STATIC_EXTRA_LDFLAGS[@]}" \
            -o "${OUT}"
    fi
else
    # Shared link. Embed the lib dir as an rpath so the binary finds the
    # dylib/.so at runtime without DYLD_/LD_LIBRARY_PATH. The -Wl,-rpath syntax
    # is accepted by both ld64 and GNU ld.
    "${CXX}" -std=c++11 ${OPT_FLAGS} -fno-omit-frame-pointer \
        -Wall -Wextra -Wno-unused-parameter \
        -I "${INCLUDE_DIR}" \
        "${SCRIPT_DIR}/test.cpp" \
        -L "${LIB_DIR}" -lpaimon_c \
        -Wl,-rpath,"${LIB_DIR}" \
        "${COMMON_LDFLAGS[@]}" \
        -o "${OUT}"
fi

echo "Built: ${OUT}"
echo "  size: $(du -h "${OUT}" | cut -f1)"
if [[ "${IS_MACOS}" -eq 1 ]]; then
    # nm -gU = global, defined (no undefined) symbols; T/S/etc are exported.
    echo "  exported symbols: $(nm -gU "${OUT}" 2>/dev/null | wc -l | tr -d ' ')"
    echo "  otool -L snippet:"
    otool -L "${OUT}" | grep -E "paimon|python|ssl|crypto" || echo "    (no paimon/python/ssl in dynamic deps)"
else
    echo "  exported (.dynsym T/W) symbols: $(nm -D "${OUT}" 2>/dev/null | awk '$2=="T" || $2=="W"' | wc -l)"
    echo "  ldd snippet:"
    ldd "${OUT}" | grep -E "paimon|python|ssl|crypto" || echo "    (no paimon/python/ssl in dynamic deps — fully static of those)"
fi

if [[ "${ACTION}" == "run" ]]; then
    echo ""
    echo "Running ${OUT}..."
    "${OUT}"
fi
