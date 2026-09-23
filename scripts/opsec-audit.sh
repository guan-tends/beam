#!/usr/bin/env bash
# ─── OPSEC pre-publish gate ─────────────────────────────────────────────
# Audits the EXACT set of files that will ship to the registry
# (crates.io package staging set), not just the working tree.
#
# The sensitive pattern list lives OUTSIDE the repository, in a file that is
# never packaged. This is deliberate: a tracked pattern list is itself a leak
# (it enumerates the very identifiers the audit exists to protect).
#
# Usage:   scripts/opsec-audit.sh
# Env:     OPSEC_PATTERNS   path to pattern file (default below)
# Exit:    0 = clean, 1 = findings (fails a `set -e` pipeline), 2 = misconfig
set -euo pipefail

PATTERN_FILE="${OPSEC_PATTERNS:-$HOME/.config/beam/opsec-patterns.txt}"
if [[ ! -f "$PATTERN_FILE" ]]; then
  echo "OPSEC GATE: pattern file not found: $PATTERN_FILE" >&2
  echo "  Create it (one regex per line, '#' comments allowed) or set OPSEC_PATTERNS." >&2
  exit 2
fi

# Build the pattern alternation from the external file.
PATTERN="$(grep -vE '^\s*(#|$)' "$PATTERN_FILE" | paste -sd'|' -)"
[[ -n "$PATTERN" ]] || { echo "OPSEC GATE: empty pattern file" >&2; exit 2; }

# Resolve the exact publish set. Preferred: `cargo package --list` (the true
# .crate staging set, includes untracked-but-not-ignored files). Fallback:
# git's tracked + untracked-not-ignored set (same inclusion rule).
echo "=== OPSEC gate — auditing the publish set ==="
# Make cargo reachable even when invoked from a bare shell.
[[ -f "$HOME/.cargo/env" ]] && source "$HOME/.cargo/env" 2>/dev/null || true
if command -v cargo >/dev/null 2>&1 \
   && FILES="$(cargo package --list --allow-dirty 2>/dev/null)" \
   && [[ -n "$FILES" ]]; then
  SRC="cargo package --list"
else
  FILES="$(git ls-files --cached --others --exclude-standard)"
  SRC="git ls-files --cached --others (cargo package unavailable)"
fi
echo "Source: $SRC"
echo "Files in publish set: $(printf '%s\n' "$FILES" | wc -l)"
echo ""

HITS=0
while IFS= read -r f; do
  [[ -f "$f" ]] || continue
  # skip binaries
  if ! grep -Iq . "$f" 2>/dev/null; then continue; fi
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    echo "  HIT: $line" >&2
    HITS=$((HITS+1))
  done < <(grep -nHE "$PATTERN" "$f" 2>/dev/null || true)
done <<< "$FILES"

echo ""
if (( HITS > 0 )); then
  echo "=== OPSEC GATE: FAILED — $HITS finding(s) in the publish set ===" >&2
  echo "    Harden the tree, or move the offending content out of the package," >&2
  echo "    then re-run. DO NOT publish." >&2
  exit 1
fi
echo "=== OPSEC gate: CLEAN ✅ ==="
