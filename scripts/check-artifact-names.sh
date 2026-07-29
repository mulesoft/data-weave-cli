#!/usr/bin/env bash
# Validates that a GitHub Actions workflow uses the DataWeave CLI artifact
# naming convention: <base>-<version>-<os>-<arch>.<ext>, os lowercase via
# matrix.script_name, arch via `uname -m`. Also a regression guard against
# the OS-casing drift (${{ runner.os }}) that motivated the convention.
#
# Usage: scripts/check-artifact-names.sh <path-to-workflow.yml>
set -euo pipefail

file="${1:-}"
if [[ -z "$file" || ! -f "$file" ]]; then
  echo "usage: $0 <path-to-workflow.yml>" >&2
  exit 2
fi

fail() { echo "FAIL [$file]: $1" >&2; exit 1; }

# (a) YAML must be well-formed. Ruby stdlib is present on the runners and macOS.
ruby -ryaml -e 'YAML.load_file(ARGV[0])' "$file" >/dev/null 2>&1 \
  || fail "not well-formed YAML"

# present <token> <human-message>: require a literal substring to appear.
present() { grep -qF -- "$1" "$file" || fail "$2"; }
# absent <token> <human-message>: require a literal substring to NOT appear.
absent()  { grep -qF -- "$1" "$file" && fail "$2" || true; }
# no_runner_os_in_names: the casing invariant. `${{ runner.os }}` must not
# appear in artifact/asset NAMES, but legitimate `if: runner.os == '...'`
# job conditionals (release.yml gates .so vs .dll by OS) must survive. So:
# find runner.os lines, drop the `if:` conditional lines, fail if any remain.
no_runner_os_in_names() {
  if grep -n 'runner.os' "$file" | grep -v 'if:' | grep -q .; then
    fail "uses \${{ runner.os }} in a name (use matrix.script_name); see: $(grep -n 'runner.os' "$file" | grep -v 'if:' | head -1)"
  fi
}

check_ci_file() {
  no_runner_os_in_names
  present 'ARCH=$(uname -m)'  "missing arch derivation (ARCH=\$(uname -m))"
  present 'env.ARCH'          "artifact names do not reference env.ARCH"
  present 'dw-cli-'           "missing dw-cli- artifact base name"
  present 'dataweave-node-'   "missing dataweave-node- artifact base name"
  echo "OK [$file]: CI-artifact naming convention"
}

check_release_file() {
  no_runner_os_in_names
  present 'ARCH=$(uname -m)' "missing arch derivation (ARCH=\$(uname -m))"
  present 'env.ARCH'         "asset names do not reference env.ARCH"
  present 'dw-cli-'          "missing dw-cli- asset base name"
  present 'dataweave-node-'  "missing dataweave-node- asset base name"
  present 'file_glob: true'  "wheel upload missing file_glob: true (needed for the * glob)"
  absent  'dw-python-wheel'  "stale dw-python-wheel asset name still present"
  absent  'dw-node-package'  "stale dw-node-package asset name still present"
  absent  '-py3-none-any.whl' "wheel file path still hardcodes -any.whl (should match the platform tag)"
  echo "OK [$file]: release naming convention"
}

case "$(basename "$file")" in
  main.yml|ci.yml) check_ci_file ;;
  release.yml)     check_release_file ;;
  *) echo "usage: $0 must target main.yml, ci.yml, or release.yml" >&2; exit 2 ;;
esac
