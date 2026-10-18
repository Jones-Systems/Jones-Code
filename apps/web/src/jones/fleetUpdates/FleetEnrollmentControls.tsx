import { useEffect, useState } from "react";
import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId } from "@t3tools/contracts";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import { runtime } from "../../lib/runtime";
import { FleetEnrollmentStatus, type FleetHostReadiness } from "./FleetEnrollmentStatus";
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
  const [statusUnavailable, setStatusUnavailable] = useState(false);
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
        if (!cancelled) setStatusUnavailable(true);
      });
    return () => {
      cancelled = true;
    };
  }, [environmentId, supported]);
  if (!supported) return null;
  const latest = state?.campaigns
    .flatMap((campaign) => campaign.members.map((member) => ({ campaign, member })))
    .findLast((entry) => entry.member.enrollment.environmentId === environmentId);
  const qualified =
    status?.operationProtocol === 1 &&
    status.update?.capability.install === true &&
    status.update.capability.download &&
    status.update.installedSource !== undefined;
  const toggle = async () => {
    const bridge = window.desktopBridge?.fleetUpdates;
    if (bridge === undefined) return;
    setBusy(true);
    await (async () => {
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
          "Automatic updates turned off. Host transactions already accepted still finish.",
        );
      } else {
        const fresh = await fleetHost(environmentId, { action: "status" });
        const selected = {
          enrollmentId: await runtime.runPromise(
            Crypto.Crypto.pipe(Effect.flatMap((crypto) => crypto.randomUUIDv4())),
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
          "Automatic updates on. This host follows this laptop’s exact source after each committed laptop update.",
        );
      }
    })()
      .catch(() => {
        setMessage(
          enrollment?.enabled
            ? "The host didn’t confirm. Automatic dispatch from this laptop is stopped if its local save succeeded; refresh before retrying."
            : "Enrollment wasn’t confirmed and saved, so no automatic update was started. Refresh before retrying.",
        );
      })
      .finally(() => {
        setBusy(false);
      });
  };
  const readiness: FleetHostReadiness = statusUnavailable
    ? "unavailable"
    : status === null
      ? "loading"
      : qualified
        ? "qualified"
        : "setup-required";
  return (
    <FleetEnrollmentStatus
      label={label}
      enrolled={enrollment?.enabled === true}
      readiness={readiness}
      campaign={
        latest === undefined
          ? undefined
          : {
              laptopPhase: latest.campaign.phase,
              hostPhase: latest.member.phase,
              ...(latest.member.reason ? { reason: latest.member.reason } : {}),
            }
      }
      notice={storageError ?? message}
      continueThreads={continueThreads}
      busy={busy}
      onContinueThreadsChange={setContinueThreads}
      onToggle={() => void toggle()}
      onRefresh={() => {
        setBusy(true);
        void Promise.all([
          refreshFleetDesktopState(),
          fleetHost(environmentId, { action: "status" }),
        ])
          .then(([, next]) => {
            setStatus(next);
            setStatusUnavailable(false);
            setMessage(undefined);
          })
          .catch(() => setStatusUnavailable(true))
          .finally(() => setBusy(false));
      }}
    />
  );
}
