// Jones: SSH disconnect and client shutdown close only the forward this client
// owns. The remote server belongs to its host and outlives every client
// connection, so no client path stops it or asks for a password to do so.
export const CLIENT_CLOSE_STOPS_REMOTE_SERVER: boolean = false;

// Persist the adopted owner PID so later launches do not create a second server.
export const PERSISTENT_REMOTE_ADOPTION_SCRIPT = `      # Attaching to the home's owner never stops the server this client launched.
      REMOTE_PID=""
      REMOTE_PORT="$DEFAULT_REMOTE_PORT"
      REMOTE_MANAGED="external"
      printf '%s\\n' "$DEFAULT_RUNTIME_PID" >"$PID_FILE"
      printf '%s\\n' "$REMOTE_PORT" >"$PORT_FILE"
      printf 'external\\n' >"$MANAGED_FILE"`;

// Runs after adoption and launch-identity transfer. Requires REMOTE_PID,
// REMOTE_PORT, REMOTE_MANAGED, ADOPTED_PID, wait_ready() and
// managed_server_running(). The enclosing launch script replaces the reuse
// timeout. A cleared record continues to the update and home-owner waits.
export const PERSISTENT_REMOTE_REUSE_SCRIPT = `# A client attaches to the host owner; readiness failure never grants replacement.
# A changed runner takes effect at the next launch, never by stopping a server.
if [ "$REMOTE_MANAGED" = "external" ]; then
  # A server adopted above has just answered. Probing it again could catch a
  # momentary stall or exit and launch a second server beside it.
  if [ -n "$ADOPTED_PID" ]; then
    :
  elif [ -n "$REMOTE_PORT" ] && wait_ready "@@T3_REUSE_READY_TIMEOUT_MS@@"; then
    :
  elif managed_server_running "$REMOTE_PID"; then
    printf 'Remote T3 server is already running with PID %s but is not ready. Inspect it on the host before retrying.\\n' "$REMOTE_PID" >&2
    exit 1
  else
    REMOTE_PID=""
    REMOTE_PORT=""
    REMOTE_MANAGED=""
  fi
elif [ -n "$REMOTE_PORT" ] && managed_server_running "$REMOTE_PID"; then
  if ! wait_ready "@@T3_REUSE_READY_TIMEOUT_MS@@"; then
    printf 'Remote T3 server is already running with PID %s but is not ready on 127.0.0.1:%s. Inspect it on the host before retrying.\\n' "$REMOTE_PID" "$REMOTE_PORT" >&2
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
