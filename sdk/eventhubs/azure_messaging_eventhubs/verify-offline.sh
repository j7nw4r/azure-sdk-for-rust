#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
#
# Offline verification gate for the azure_messaging_eventhubs crate.
#
# Runs the checks that do NOT need an Azure Event Hubs namespace: formatting,
# build, clippy, the offline test subset (the ~160 unit/doc/integration tests
# that pass without credentials; the live tests auto-skip), documentation, and
# an optional semver-compatibility check against the last published release.
#
# This is the "always green" baseline. For the live end-to-end check, run the
# smoke-test harness instead:
#   cargo run --example eventhubs_smoke_test   (needs EVENTHUBS_HOST etc.)
#
# Usage:
#   ./verify-offline.sh            # run every step, report a tally
#   FAIL_FAST=1 ./verify-offline.sh  # stop at the first failing step
#
# Exit code is non-zero if any step failed.

set -u

PKG="azure_messaging_eventhubs"
# Resolve the crate directory so the script works from any cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PASSED=()
FAILED=()
SKIPPED=()

# run_step "Human name" cmd args...
# Captures the command's own exit status (via `if cmd`) rather than reading $?
# after a pipe, which would report the pipe's status instead.
run_step() {
  local name="$1"
  shift
  echo ""
  echo "=================================================="
  echo ">>> ${name}"
  echo "    \$ $*"
  echo "=================================================="
  if "$@"; then
    echo "--- PASS: ${name}"
    PASSED+=("${name}")
  else
    echo "--- FAIL: ${name}"
    FAILED+=("${name}")
    if [[ "${FAIL_FAST:-0}" == "1" ]]; then
      summary
      exit 1
    fi
  fi
}

summary() {
  echo ""
  echo "=================================================="
  echo "OFFLINE VERIFICATION SUMMARY"
  echo "=================================================="
  for s in "${PASSED[@]:-}"; do [[ -n "$s" ]] && echo "  PASS  $s"; done
  for s in "${SKIPPED[@]:-}"; do [[ -n "$s" ]] && echo "  SKIP  $s"; done
  for s in "${FAILED[@]:-}"; do [[ -n "$s" ]] && echo "  FAIL  $s"; done
  echo "--------------------------------------------------"
  echo "  ${#PASSED[@]} passed, ${#FAILED[@]} failed, ${#SKIPPED[@]} skipped"
}

# 1. Formatting.
run_step "rustfmt (check)" \
  cargo fmt --package "$PKG" --check

# 2. Build with every feature enabled.
run_step "build (--all-features)" \
  cargo build --package "$PKG" --all-features --all-targets

# 3. Clippy, warnings-as-errors, across all targets and features.
run_step "clippy (-D warnings)" \
  cargo clippy --package "$PKG" --all-targets --all-features -- -D warnings

# 4. Offline test subset. Live (recorded) tests self-skip without credentials.
run_step "test (offline subset)" \
  cargo test --package "$PKG" --all-features

# 5. Documentation builds cleanly, treating warnings (incl. missing_docs and
#    broken intra-doc links) as errors.
run_step "doc (--no-deps, -D warnings)" \
  env RUSTDOCFLAGS="-D warnings" cargo doc --package "$PKG" --no-deps --all-features

# 6. Semver compatibility vs the last crates.io release (optional tooling).
#    Answers "would this diff be a breaking change?" which matters for a 1.0.0
#    bump. Skipped with a note when cargo-semver-checks is not installed.
if command -v cargo-semver-checks >/dev/null 2>&1; then
  run_step "cargo-semver-checks (vs published)" \
    cargo semver-checks check-release --package "$PKG"
else
  echo ""
  echo ">>> cargo-semver-checks (vs published)  --  SKIPPED"
  echo "    Install with: cargo install cargo-semver-checks"
  SKIPPED+=("cargo-semver-checks (not installed)")
fi

summary

[[ ${#FAILED[@]} -eq 0 ]]
