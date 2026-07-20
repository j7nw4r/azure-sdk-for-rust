#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
#
# cspell:ignore toplevel worktrees RUSTDOCFLAGS
#
# Offline verification gate for the azure_messaging_eventhubs crate.
#
# Runs the checks that do NOT need an Azure Event Hubs namespace: formatting,
# spell check, build, clippy, the offline test subset (the live tests self-skip
# without credentials), documentation, packaging, and an optional
# semver-compatibility check against the last published release.
#
# This is the "always green" baseline for a 1.0.0 decision. For the live
# end-to-end check, run the smoke-test harness instead:
#   cargo run --example eventhubs_smoke_test   (needs EVENTHUBS_CONNECTION_STRING
#                                               or EVENTHUBS_HOST)
#
# Steps are one of two kinds:
#   required   a failure fails the gate
#   advisory   a failure is reported but does not fail the gate, because the
#              step can break for reasons unrelated to this crate
#
# Usage:
#   ./verify-offline.sh              # run every step, report a tally
#   FAIL_FAST=1 ./verify-offline.sh  # stop at the first failing required step
#
# Exit code is non-zero if any required step failed.

set -u

PKG="azure_messaging_eventhubs"
# Resolve the crate directory so the script works from any cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || echo "")"

PASSED=()
FAILED=()
ADVISORY=()
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

# run_advisory "Human name" cmd args...
# Same as run_step, but a failure does not fail the gate.
run_advisory() {
  local name="$1"
  shift
  echo ""
  echo "=================================================="
  echo ">>> ${name}  (advisory)"
  echo "    \$ $*"
  echo "=================================================="
  if "$@"; then
    echo "--- PASS: ${name}"
    PASSED+=("${name}")
  else
    echo "--- ADVISORY FAIL: ${name}"
    echo "    This step does not fail the gate. Read the output above and"
    echo "    decide whether it points at this crate or at the environment."
    ADVISORY+=("${name}")
  fi
}

skip_step() {
  local name="$1"
  local reason="$2"
  echo ""
  echo ">>> ${name}  --  SKIPPED"
  echo "    ${reason}"
  SKIPPED+=("${name}")
}

summary() {
  echo ""
  echo "=================================================="
  echo "OFFLINE VERIFICATION SUMMARY"
  echo "=================================================="
  for s in "${PASSED[@]:-}"; do [[ -n "$s" ]] && echo "  PASS      $s"; done
  for s in "${SKIPPED[@]:-}"; do [[ -n "$s" ]] && echo "  SKIP      $s"; done
  for s in "${ADVISORY[@]:-}"; do [[ -n "$s" ]] && echo "  ADVISORY  $s"; done
  for s in "${FAILED[@]:-}"; do [[ -n "$s" ]] && echo "  FAIL      $s"; done
  echo "--------------------------------------------------"
  echo "  ${#PASSED[@]} passed, ${#FAILED[@]} failed, ${#ADVISORY[@]} advisory, ${#SKIPPED[@]} skipped"
  if [[ ${#FAILED[@]} -eq 0 ]]; then
    echo "  GATE: PASS"
  else
    echo "  GATE: FAIL"
  fi
}

# Report whether this checkout is a linked git worktree. Tests that use
# #[recorded::test] resolve their recording paths from the compile-time
# CARGO_MANIFEST_DIR, so a test binary built under .claude/worktrees fails at
# test-proxy session start. The test step is skipped there.
#
# Both paths are resolved to absolute physical paths before they are compared.
# From a subdirectory, git reports --git-dir as an absolute path but
# --git-common-dir as a path relative to the current directory, so a raw string
# comparison reports every checkout as a worktree.
is_linked_worktree() {
  local git_dir common_dir
  git_dir="$(git rev-parse --git-dir 2>/dev/null || echo "")"
  common_dir="$(git rev-parse --git-common-dir 2>/dev/null || echo "")"
  [[ -n "$git_dir" && -n "$common_dir" ]] || return 1
  git_dir="$(cd "$git_dir" 2>/dev/null && pwd -P)" || return 1
  common_dir="$(cd "$common_dir" 2>/dev/null && pwd -P)" || return 1
  [[ "$git_dir" != "$common_dir" ]]
}

# 1. Formatting.
run_step "rustfmt (check)" \
  cargo fmt --package "$PKG" --check

# 2. Spell check, using the repo config that CI's Build Analyze job uses.
#    A plain `npx cspell` does not pick that config up and gives a different
#    answer, so the config path is explicit.
if [[ -n "$REPO_ROOT" && -f "$REPO_ROOT/.vscode/cspell.json" ]] && command -v npx >/dev/null 2>&1; then
  run_step "cspell (repo config)" \
    npx --yes cspell lint --config "$REPO_ROOT/.vscode/cspell.json" --no-must-find-files \
    "$SCRIPT_DIR/src/**" "$SCRIPT_DIR/examples/**" "$SCRIPT_DIR/tests/**" \
    "$SCRIPT_DIR"/*.md "$SCRIPT_DIR"/*.sh
else
  skip_step "cspell (repo config)" "Needs npx and <repo>/.vscode/cspell.json."
fi

# 3. Build with every feature enabled.
run_step "build (--all-features)" \
  cargo build --package "$PKG" --all-features --all-targets

# 4. Clippy, warnings-as-errors, across all targets and features.
run_step "clippy (-D warnings)" \
  cargo clippy --package "$PKG" --all-targets --all-features -- -D warnings

# 5. Offline test subset. Live (recorded) tests self-skip without credentials.
if is_linked_worktree; then
  skip_step "test (offline subset)" \
    "This is a linked git worktree. #[recorded::test] resolves recording paths from the compile-time CARGO_MANIFEST_DIR and fails with 'header not found x-recording-id'. Run this step from a normal checkout."
else
  run_step "test (offline subset)" \
    cargo test --package "$PKG" --all-features
fi

# 6. Documentation builds cleanly, treating warnings (incl. missing_docs and
#    broken intra-doc links) as errors.
run_step "doc (--no-deps, -D warnings)" \
  env RUSTDOCFLAGS="-D warnings" cargo doc --package "$PKG" --no-deps --all-features

# 7. Packaging. This is the machine check that the crate can actually be
#    published: it fails if any dependency is an unpublished path or a
#    prerelease that no registry version satisfies. That was the historical
#    1.0.0 blocker, so the step is here to keep answering the question.
run_advisory "publish (--dry-run)" \
  cargo publish --dry-run --package "$PKG" --allow-dirty

# 8. Semver compatibility vs the last crates.io release (optional tooling).
#    Answers "would this diff be a breaking change?" which matters for a 1.0.0
#    bump. Advisory: a fresh core release can break the rebuilt baseline for
#    reasons that have nothing to do with this crate's diff.
if command -v cargo-semver-checks >/dev/null 2>&1; then
  run_advisory "cargo-semver-checks (vs published)" \
    cargo semver-checks check-release --package "$PKG"
else
  skip_step "cargo-semver-checks (vs published)" \
    "Install with: cargo install cargo-semver-checks"
fi

summary

[[ ${#FAILED[@]} -eq 0 ]]
