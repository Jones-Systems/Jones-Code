import type { EnvironmentId, VoiceReviewSelfIdentity } from "@t3tools/contracts";
import { useId, useState } from "react";
import { Button } from "../ui/button";
import { useVoiceReview } from "./useVoiceReview";
import { voiceReviewEnrollmentEntry } from "./voiceReviewActions";

export function VoiceReviewDeviceSetup({ environmentId }: { environmentId: EnvironmentId }) {
  const { fetchSelfIdentity } = useVoiceReview(environmentId);
  const detailsId = useId();
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [identity, setIdentity] = useState<VoiceReviewSelfIdentity | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [copyStatus, setCopyStatus] = useState<string | null>(null);
  const entry = identity === null ? null : voiceReviewEnrollmentEntry(identity);
  const show = async () => {
    setOpen(true);
    setLoading(true);
    setMessage(null);
    setCopyStatus(null);
    try {
      const result = await fetchSelfIdentity();
      setIdentity(result);
      if (result === null)
        setMessage(
          "Device setup details are unavailable on this server. Update the server to use this option.",
        );
    } catch {
      setIdentity(null);
      setMessage(
        "Device setup details could not be loaded. Check your connection and access with the server owner.",
      );
    } finally {
      setLoading(false);
    }
  };
  const copy = async () => {
    if (entry === null) return;
    try {
      await navigator.clipboard.writeText(entry);
      setCopyStatus("Enrollment entry copied.");
    } catch {
      setCopyStatus("Could not copy. Select and copy the enrollment entry below.");
    }
  };
  return (
    <div className="space-y-2 text-sm">
      <Button
        variant="outline"
        size="compact"
        aria-expanded={open}
        aria-controls={detailsId}
        disabled={loading}
        onClick={() => {
          if (open) setOpen(false);
          else void show();
        }}
      >
        {open ? "Hide device setup" : "Set up this device"}
      </Button>
      {open ? (
        <div id={detailsId} className="space-y-2">
          <p className="text-muted-foreground">
            The server owner can use these details to enable prompt review for this device. Viewing
            or copying them does not change access.
          </p>
          {loading ? <p role="status">Loading device details…</p> : null}
          {message ? <p role="status">{message}</p> : null}
          {!loading && identity ? (
            <>
              <p>
                {identity.enrollment === "enrolled"
                  ? "This device is enrolled for prompt review."
                  : identity.enrollment === "not-configured"
                    ? "Prompt review is not configured on this server."
                    : "This device is not enrolled for prompt review."}
              </p>
              <dl className="space-y-1 break-all">
                <dt>Subject</dt>
                <dd>{identity.subject}</dd>
                <dt>Authentication method</dt>
                <dd>{identity.method}</dd>
              </dl>
              {entry !== null ? (
                <>
                  <p className="text-muted-foreground">
                    Ask the server owner to add this entry to the allowed device identities in the
                    prompt review configuration.
                  </p>
                  <pre
                    aria-label="Device enrollment entry"
                    className="overflow-x-auto whitespace-pre-wrap break-all rounded border p-3"
                  >
                    {entry}
                  </pre>
                  <Button variant="outline" size="compact" onClick={() => void copy()}>
                    Copy enrollment entry
                  </Button>
                  {copyStatus ? <p role="status">{copyStatus}</p> : null}
                </>
              ) : (
                <p className="text-muted-foreground">
                  This connection does not provide a verified device key. Connect with device
                  authentication to obtain an enrollment entry.
                </p>
              )}
            </>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
