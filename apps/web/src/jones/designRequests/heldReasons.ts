import type { DesignRequestHeldReason } from "@t3tools/contracts/jones/designRequests";

/** One sentence per held reason: what is wrong and how to fix it. */
export const DESIGN_REQUEST_HELD_FIXES: Record<DesignRequestHeldReason, string> = {
  "no-binding": "Pair this gallery project to an existing workstream.",
  "origin-not-allowed":
    "This gallery origin is not a loopback, tailnet or *.ts.net address, so it cannot be paired.",
  "not-authenticated": "Sign in to Jones in this browser, then refresh.",
  "runtime-unsupported": "Update this environment to use Workstreams.",
  "registry-stale": "Couldn't confirm the Workstream. Check the connection and try again.",
  "workstream-not-found": "The paired workstream no longer exists. Pair the project again.",
  "no-primary": "Move a thread into this Workstream before sending.",
  "multiple-primary": "Choose a Workstream with one thread before sending.",
  "primary-not-attested": "Couldn't confirm the thread. Check the connection and try again.",
  "placement-expired": "Couldn't confirm the thread. Check the connection and try again.",
  "thread-not-local": "The primary thread is not on this Jones server's primary environment.",
  "thread-archived": "The primary thread is archived. Unarchive it or choose another primary.",
  "thread-config-unknown":
    "The thread's runtime and interaction modes are unknown. Open it once, then retry.",
  "route-changed":
    "The destination changed after it was shown. Review the new destination and send again.",
  "packet-invalid":
    "The packet does not match design packet v1. Download it again from the gallery.",
  "digest-mismatch":
    "The packet digest does not match its content. Download it again from the gallery.",
  "attachment-mismatch":
    "An image is missing or does not match its SHA-256. Download the full packet.",
  "too-large":
    "The request exceeds the image or message limits. Remove images or shorten the note.",
  "already-sent-elsewhere": "These IDs were already used on another thread. Nothing new was sent.",
  "readback-unavailable":
    "Jones could not read command status. Check the connection, then check status.",
};

export const designRequestHeldFix = (reason: string | undefined): string =>
  reason !== undefined && Object.hasOwn(DESIGN_REQUEST_HELD_FIXES, reason)
    ? DESIGN_REQUEST_HELD_FIXES[reason as DesignRequestHeldReason]
    : "";
