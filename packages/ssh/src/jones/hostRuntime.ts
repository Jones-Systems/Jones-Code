// Attachment must not choose the desktop's artifact version or take over the
// host service. Read only the launcher's active version and ready sentinel.
// `T3CODE_HOME` wins; otherwise state in both `~/.t3` and `~/.jones-code` is refused.
const RESOLVE_HOST_RUNTIME = `set -eu
jones_setup_required() {
  printf 'Set up Jones Code on this host with its managed service before connecting: %s.\\n' "$1" >&2
  exit 1
}
if [ -n "\${T3CODE_HOME:-}" ]; then
  JONES_BASE_DIR="$T3CODE_HOME"
else
  JONES_BASE_DIR=""
  for JONES_CANDIDATE in "$HOME/.t3" "$HOME/.jones-code"; do
    if [ -r "$JONES_CANDIDATE/runtime/service-state.json" ]; then
      [ -z "$JONES_BASE_DIR" ] || jones_setup_required 'service state exists in both ~/.t3 and ~/.jones-code; set T3CODE_HOME to the one to use'
      JONES_BASE_DIR="$JONES_CANDIDATE"
    fi
  done
  [ -n "$JONES_BASE_DIR" ] || JONES_BASE_DIR="$HOME/.t3"
fi
JONES_STATE="$JONES_BASE_DIR/runtime/service-state.json"
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

// A running service can be briefly unreachable while it restarts, for example
// after an update. The caller's own launch timeout (90 s) bounds the total.
const DEFAULT_ATTACH_ATTEMPTS = 6;
const DEFAULT_ATTACH_RETRY_DELAY_SECONDS = 2;
// Each attempt waits this long for the server's health probe.
const ATTACH_READY_WAIT_MS = 3000;
const ATTACH_READY_PROBE_MS = 1000;

export function buildHostRuntimeRunnerScript(): string {
  return `#!/bin/sh\n${RESOLVE_HOST_RUNTIME}\nexec "$JONES_RUNTIME" "$@"\n`;
}

export interface HostRuntimeAttachOptions {
  /** Total attempts to find a running, ready server before reporting it unavailable. */
  readonly attempts?: number;
  /** Whole seconds to pause between attempts. */
  readonly retryDelaySeconds?: number;
}

function wholeNumber(value: number | undefined, fallback: number, minimum: number): number {
  return value !== undefined && Number.isInteger(value) && value >= minimum ? value : fallback;
}

export function buildHostRuntimeAttachScript(options: HostRuntimeAttachOptions = {}): string {
  const attempts = wholeNumber(options.attempts, DEFAULT_ATTACH_ATTEMPTS, 1);
  const delay = wholeNumber(options.retryDelaySeconds, DEFAULT_ATTACH_RETRY_DELAY_SECONDS, 0);
  // Missing or invalid service state is a setup problem. An absent or unready
  // server is retried first, then reported as not ready: the service may be
  // restarting after an update, so it must not tell the user to set it up again.
  return `${RESOLVE_HOST_RUNTIME}
jones_not_ready() {
  printf 'Jones Code on this host is not ready yet; it may still be restarting after an update. Try again shortly: %s.\\n' "$1" >&2
  exit 1
}
jones_probe_server() {
  JONES_INFO=$("$JONES_RUNTIME" __ssh-helper runtime-port "$JONES_BASE_DIR/userdata/server-runtime.json") || { JONES_REASON='managed server is not running'; return 1; }
  JONES_PID="\${JONES_INFO%% *}"
  JONES_PORT="\${JONES_INFO#* }"
  case "$JONES_PID" in ''|*[!0-9]*) jones_setup_required 'managed server PID is invalid' ;; esac
  case "$JONES_PORT" in ''|*[!0-9]*) jones_setup_required 'managed server port is invalid' ;; esac
  [ "$JONES_PID" -gt 0 ] && [ "$JONES_PORT" -gt 0 ] && [ "$JONES_PORT" -le 65535 ] || jones_setup_required 'managed server endpoint is invalid'
  "$JONES_RUNTIME" __ssh-helper wait-ready "$JONES_PORT" ${ATTACH_READY_WAIT_MS} ${ATTACH_READY_PROBE_MS} || { JONES_REASON='managed server is not ready; inspect it on the host if this persists'; return 1; }
}
JONES_ATTEMPT=0
JONES_REASON=''
until jones_probe_server; do
  JONES_ATTEMPT=$((JONES_ATTEMPT + 1))
  [ "$JONES_ATTEMPT" -lt ${attempts} ] || jones_not_ready "$JONES_REASON"
  sleep ${delay}
done
printf '{"remotePort":%s,"serverKind":"external"}\\n' "$JONES_PORT"
`;
}

export function buildHostRuntimePairingScript(): string {
  return `${RESOLVE_HOST_RUNTIME}\nexec "$JONES_RUNTIME" auth pairing create --base-dir "$JONES_BASE_DIR" --json\n`;
}
