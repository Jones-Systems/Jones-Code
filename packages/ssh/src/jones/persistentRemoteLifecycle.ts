// Requires resolved DEFAULT_RUNTIME_PID and DEFAULT_REMOTE_PORT plus REMOTE_PID,
// REMOTE_PORT, REMOTE_MANAGED, PID_FILE, PORT_FILE, MANAGED_FILE and wait_ready().
// The reuse timeout placeholder is replaced by the enclosing launch script.
export const PERSISTENT_REMOTE_REUSE_SCRIPT = `# A client attaches to the host owner; readiness failure never grants replacement.
if [ -n "$DEFAULT_REMOTE_PORT" ]; then
  REMOTE_PORT="$DEFAULT_REMOTE_PORT"
  if ! wait_ready "@@T3_REUSE_READY_TIMEOUT_MS@@"; then
    printf 'Remote T3 server is already running with PID %s but is not ready on 127.0.0.1:%s. Inspect it on the host before retrying.\\n' "$DEFAULT_RUNTIME_PID" "$REMOTE_PORT" >&2
    exit 1
  fi
  if [ "$REMOTE_MANAGED" != "managed" ] || [ "$REMOTE_PID" != "$DEFAULT_RUNTIME_PID" ]; then
    REMOTE_MANAGED="external"
  fi
  if [ "$REMOTE_PID" != "$DEFAULT_RUNTIME_PID" ]; then
    printf '%s\\n' "$DEFAULT_RUNTIME_PID" >"$PID_FILE"
  fi
  printf '%s\\n' "$REMOTE_PORT" >"$PORT_FILE"
  printf '%s\\n' "$REMOTE_MANAGED" >"$MANAGED_FILE"
elif [ -n "$REMOTE_PID" ] && kill -0 "$REMOTE_PID" 2>/dev/null; then
  if [ -z "$REMOTE_PORT" ] || ! wait_ready "@@T3_REUSE_READY_TIMEOUT_MS@@"; then
    printf 'Remote T3 server is already running with PID %s but is not ready. Inspect it on the host before retrying.\\n' "$REMOTE_PID" >&2
    exit 1
  fi
elif [ "$REMOTE_MANAGED" = "external" ] && [ -z "$REMOTE_PID" ]; then
  if [ -z "$REMOTE_PORT" ] || ! wait_ready "@@T3_REUSE_READY_TIMEOUT_MS@@"; then
    printf 'Previously discovered remote T3 server is unavailable. Verify its ownership on the host before retrying.\\n' >&2
    exit 1
  fi
else
  REMOTE_PID=""
  REMOTE_PORT=""
  REMOTE_MANAGED=""
fi`;

// Requires the captured REMOTE_PID, PID_FILE, PORT_FILE, MANAGED_FILE and
// wait_for_pid_exit(). Unknown exit retains ownership for the next attachment.
export const FAILED_NEW_REMOTE_SERVER_CLEANUP_SCRIPT = `    kill "$REMOTE_PID" 2>/dev/null || true
    wait_for_pid_exit "$REMOTE_PID"
    if kill -0 "$REMOTE_PID" 2>/dev/null; then
      printf 'Remote T3 server with PID %s did not stop. Its ownership files were kept.\\n' "$REMOTE_PID" >&2
    else
      rm -f "$PID_FILE" "$PORT_FILE" "$MANAGED_FILE"
    fi`;
