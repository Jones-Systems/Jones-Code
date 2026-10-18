// Attachment must not choose the desktop's artifact version or take over the
// host service. Read only the launcher's active version and ready sentinel.
const RESOLVE_HOST_RUNTIME = `set -eu
JONES_BASE_DIR="\${T3CODE_HOME:-$HOME/.t3}"
JONES_STATE="$JONES_BASE_DIR/runtime/service-state.json"
jones_setup_required() {
  printf 'Set up Jones Code on this host with its managed service before connecting: %s.\\n' "$1" >&2
  exit 1
}
[ -r "$JONES_STATE" ] || jones_setup_required 'service state is missing'
# Native service state has no schema key. Handoff wrappers may contain a
# legacy activeVersion, which does not identify the current managed runtime.
if grep -Eq '"schema"[[:space:]]*:' "$JONES_STATE"; then
  jones_setup_required 'unsupported service state schema'
fi
# Accept a single path-safe exact version, never a desktop version or alias.
JONES_VERSION=$(grep -oE '"activeVersion"[[:space:]]*:[[:space:]]*"[^"]*"' "$JONES_STATE" | sed -E 's/^"activeVersion"[[:space:]]*:[[:space:]]*"([^"]*)"$/\\1/' || true)
printf '%s\\n' "$JONES_VERSION" | grep -Eq '^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\\.[0-9A-Za-z-]+)*)?(\\+[0-9A-Za-z-]+(\\.[0-9A-Za-z-]+)*)?$' || jones_setup_required 'active runtime version is invalid'
[ "$(printf '%s\\n' "$JONES_VERSION" | wc -l | tr -d ' ')" = 1 ] || jones_setup_required 'active runtime version is ambiguous'
JONES_RUNTIME_DIR="$JONES_BASE_DIR/runtime/versions/$JONES_VERSION"
JONES_RUNTIME="$JONES_RUNTIME_DIR/t3"
[ -x "$JONES_RUNTIME" ] || jones_setup_required 'active executable is missing'
[ "$(cat "$JONES_RUNTIME_DIR/.install-complete" 2>/dev/null)" = "$JONES_VERSION" ] || jones_setup_required 'active runtime is incomplete'
`;

export function buildHostRuntimeRunnerScript(): string {
  return `#!/bin/sh\n${RESOLVE_HOST_RUNTIME}\nexec "$JONES_RUNTIME" "$@"\n`;
}

export function buildHostRuntimeAttachScript(): string {
  return `${RESOLVE_HOST_RUNTIME}
JONES_INFO=$("$JONES_RUNTIME" __ssh-helper runtime-port "$JONES_BASE_DIR/userdata/server-runtime.json") || jones_setup_required 'managed server is not running'
JONES_PID="\${JONES_INFO%% *}"
JONES_PORT="\${JONES_INFO#* }"
case "$JONES_PID" in ''|*[!0-9]*) jones_setup_required 'managed server PID is invalid' ;; esac
case "$JONES_PORT" in ''|*[!0-9]*) jones_setup_required 'managed server port is invalid' ;; esac
[ "$JONES_PID" -gt 0 ] && [ "$JONES_PORT" -gt 0 ] && [ "$JONES_PORT" -le 65535 ] || jones_setup_required 'managed server endpoint is invalid'
"$JONES_RUNTIME" __ssh-helper wait-ready "$JONES_PORT" 2000 1000 || jones_setup_required 'managed server is not ready; inspect it on the host'
printf '{"remotePort":%s,"serverKind":"external"}\\n' "$JONES_PORT"
`;
}

export function buildHostRuntimePairingScript(): string {
  return `${RESOLVE_HOST_RUNTIME}\nexec "$JONES_RUNTIME" auth pairing create --base-dir "$JONES_BASE_DIR" --json\n`;
}
