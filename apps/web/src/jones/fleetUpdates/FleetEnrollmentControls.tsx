import { useEffect, useState } from "react";
import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId } from "@t3tools/contracts";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import { runtime } from "../../lib/runtime";
import { Button } from "../../components/ui/button";
import {
  fleetDesktopState,
  fleetHost,
  fleetStatusError,
  refreshFleetDesktopState,
} from "./runtime";

export function FleetEnrollmentControls({
  environmentId,
  label,
}: {
  readonly environmentId: EnvironmentId;
  readonly label: string;
}) {
  const state = useAtomValue(fleetDesktopState);
  const storageError = useAtomValue(fleetStatusError);
  const enrollment = state?.enrollments.find((entry) => entry.environmentId === environmentId);
  const [status, setStatus] = useState<FleetHostStatus | null>(null);
  const [message, setMessage] = useState<string>();
  const [busy, setBusy] = useState(false);
  const [continueThreads, setContinueThreads] = useState(false);
  const supported = window.desktopBridge?.fleetUpdates !== undefined;
  useEffect(() => {
    if (!supported) return;
    let cancelled = false;
    void Promise.all([refreshFleetDesktopState(), fleetHost(environmentId, { action: "status" })])
      .then(([, next]) => {
        if (!cancelled) setStatus(next);
      })
      .catch(() => {
        if (!cancelled)
          setMessage(
            "Fleet update support is unavailable. Older hosts need explicit launcher setup; reconnect to check again.",
          );
      });
    return () => {
      cancelled = true;
    };
  }, [environmentId, supported]);
  if (!supported) return null;
  const latest = state?.campaigns
    .flatMap((campaign) => campaign.members.map((member) => ({ campaign, member })))
    .filter((entry) => entry.member.enrollment.environmentId === environmentId)
    .at(-1);
  const member = latest?.member;
  const qualified =
    status?.operationProtocol === 1 &&
    status.update?.capability.install === true &&
    status.update.capability.download &&
    status.update.installedSource !== undefined;
  const toggle = async () => {
    const bridge = window.desktopBridge?.fleetUpdates;
    if (bridge === undefined) return;
    setBusy(true);
    try {
      if (enrollment?.enabled) {
        // Stop local automatic dispatch before contacting an offline host.
        await bridge({ action: "enroll", enrollment: { ...enrollment, enabled: false } });
        await refreshFleetDesktopState();
        const next = await fleetHost(environmentId, {
          action: "enroll",
          input: {
            enrollment: { ...enrollment, enabled: false },
            expectedEnrollmentId: enrollment.enrollmentId,
          },
        });
        setStatus(next);
        setMessage(
          "Automatic updates disabled. Already accepted host transactions still reconcile.",
        );
      } else {
        const fresh = await fleetHost(environmentId, { action: "status" });
        const selected = {
          enrollmentId: await runtime.runPromise(
            Crypto.Crypto.pipe(Effect.flatMap((crypto) => crypto.randomUUIDv4)),
          ),
          environmentId,
          enabled: true,
          continueRunningThreads: continueThreads,
        };
        const next = await fleetHost(environmentId, {
          action: "enroll",
          input: {
            enrollment: selected,
            expectedEnrollmentId: fresh.enrollment?.enrollmentId ?? null,
          },
        });
        if (next.enrollment?.enrollmentId !== selected.enrollmentId)
          throw new Error("Host enrollment was not confirmed.");
        await bridge({ action: "enroll", enrollment: selected });
        await refreshFleetDesktopState();
        setStatus(next);
        setMessage(
          "Enrolled. This host will follow the exact laptop source after its native update commits.",
        );
      }
    } catch {
      setMessage(
        enrollment?.enabled
          ? "Local automatic dispatch is stopped if its save succeeded. The host could not confirm the change; refresh its status before retrying."
          : "Enrollment could not be confirmed and saved. No automatic update was started; refresh before retrying.",
      );
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="mt-2 flex max-w-lg flex-col gap-2 text-xs">
      <p>
        Automatic updates for {label}: {enrollment?.enabled ? "enrolled" : "not enrolled"}
      </p>
      <p className="text-muted-foreground">
        Only this host’s qualified service is included. Installation follows a committed laptop
        update; offline hosts wait for reconnect.
      </p>
      {member ? (
        <p role="status">
          Laptop update: {latest?.campaign.phase} · Host: {member.phase}
          {member.reason ? ` · ${member.reason}` : ""}
        </p>
      ) : null}
      {storageError || message ? <p role="status">{storageError ?? message}</p> : null}
      {!enrollment?.enabled && qualified ? (
        <label className="flex items-center gap-2">
          <input
            type="checkbox"
            checked={continueThreads}
            disabled={busy}
            onChange={(event) => setContinueThreads(event.target.checked)}
          />
          Continue eligible active threads after this host updates
        </label>
      ) : null}
      {!qualified && !enrollment?.enabled ? (
        <p className="text-muted-foreground">
          Setup required: verify and adopt a source-qualified service on this selected host before
          enrollment.
        </p>
      ) : null}
      <div className="flex gap-2">
        <Button
          size="xs"
          variant="outline"
          disabled={busy || (!enrollment?.enabled && !qualified)}
          onClick={() => void toggle()}
        >
          {enrollment?.enabled ? "Disable automatic updates" : "Enroll this host"}
        </Button>
        <Button
          size="xs"
          variant="outline"
          disabled={busy}
          onClick={() => {
            setBusy(true);
            void Promise.all([
              refreshFleetDesktopState(),
              fleetHost(environmentId, { action: "status" }),
            ])
              .then(([, next]) => {
                setStatus(next);
                setMessage(undefined);
              })
              .catch(() => setMessage("Host unavailable or launcher setup is required."))
              .finally(() => setBusy(false));
          }}
        >
          Refresh
        </Button>
      </div>
    </div>
  );
}
