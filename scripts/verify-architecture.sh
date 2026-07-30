#!/usr/bin/env bash
set -euo pipefail

# Run this script directly with ARCH_LINT_BIN pointing at an existing v1.16.0 binary,
# or use `make arch-verify`, which installs the pinned binary before invoking it.
root_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
checker=${ARCH_LINT_BIN:-"$root_dir/bin/go-arch-lint"}
output=$(mktemp)
fixture=''

cleanup() {
	if [[ -n "$fixture" ]]; then
		rm -f -- "$fixture"
	fi
	rm -f -- "$output"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

if [[ ! -x "$checker" ]]; then
	printf 'architecture checker is not executable: %s\n' "$checker" >&2
	exit 1
fi

fixture=$(mktemp "$root_dir/internal/application/architecture_boundary_fixture_XXXXXX.go")
cat >"$fixture" <<'EOF'
package application

import _ "github.com/notrodans/nebula-go/internal/infrastracture/logger/slog"
EOF

if "$checker" check --project-path "$root_dir" --arch-file "$root_dir/.go-arch-lint.yml" >"$output" 2>&1; then
	printf 'architecture checker accepted the forbidden application -> infrastracture import\n' >&2
	cat "$output" >&2
	exit 1
fi

output_text=$(<"$output")
if [[ "$output_text" != *application* || "$output_text" != *infrastracture* ]]; then
	printf 'architecture checker failed for an unexpected reason\n' >&2
	cat "$output" >&2
	exit 1
fi

rm -f -- "$fixture"

"$checker" check --project-path "$root_dir" --arch-file "$root_dir/.go-arch-lint.yml"
