import { useEffect, useRef, useState } from "react";
import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId } from "@t3tools/contracts";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import { runtime } from "../../lib/runtime";
import { FleetEnrollmentStatus, type FleetHostReadiness } from "./FleetEnrollmentStatus";
import { describeFollower } from "./followerStatus";
import { fleetInstalledSource, fleetPublication } from "./publicationState";
import {
  describePublication,
  describePublicationReceipt,
  selectPublicationReceipt,
} from "./publicationStatus";
import {
  fleetDesktopState,
  fleetHost,
  fleetStatusError,
  refreshFleetDesktopState,
} from "./runtime";

interface FleetEnrollmentControlsProps {
  readonly environmentId: EnvironmentId;
  readonly label: string;
}

export function FleetEnrollmentControls(props: FleetEnrollmentControlsProps) {
  return <FleetEnrollmentControlsForHost key={props.environmentId} {...props} />;
}

function FleetEnrollmentControlsForHost({ environmentId, label }: FleetEnrollmentControlsProps) {
  const state = useAtomValue(fleetDesktopState);
  const storageError = useAtomValue(fleetStatusError);
  const publication = useAtomValue(fleetPublication);
  const installedSource = useAtomValue(fleetInstalledSource);
  const enrollment = state?.enrollments.find((entry) => entry.environmentId === environmentId);
  const [status, setStatus] = useState<FleetHostStatus | null>(null);
  const [statusObservedAt, setStatusObservedAt] = useState<string>();
  const statusRequest = useRef(0);
  const [message, setMessage] = useState<string>();
  const [statusUnavailable, setStatusUnavailable] = useState(false);
  const [busy, setBusy] = useState(false);
  const [continueThreads, setContinueThreads] = useState(false);
  const supported = window.desktopBridge?.fleetUpdates !== undefined;
  // oxlint-disable-next-line t3code/react-effect-budget -- Read fleet enrollment status through the desktop bridge and ignore responses after the controls retire.
  useEffect(() => {
    if (!supported) return;
    let cancelled = false;
    const request = ++statusRequest.current;
    void Promise.all([refreshFleetDesktopState(), fleetHost(environmentId, { action: "status" })])
      .then(([, next]) => {
        if (!cancelled && request === statusRequest.current) {
          setStatus(next);
          setStatusObservedAt(new Date().toISOString());
          setStatusUnavailable(false);
        }
      })
      .catch(() => {
        if (!cancelled && request === statusRequest.current) {
          setStatusUnavailable(true);
          setStatusObservedAt(new Date().toISOString());
        }
      });
    return () => {
      cancelled = true;
      statusRequest.current++;
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
    const request = ++statusRequest.current;
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
        if (request !== statusRequest.current) return;
        setStatus(next);
        setStatusObservedAt(new Date().toISOString());
        setStatusUnavailable(false);
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
        if (request !== statusRequest.current) return;
        setStatus(next);
        setStatusObservedAt(new Date().toISOString());
        setStatusUnavailable(false);
        setMessage(
          "Automatic updates on. This host follows this laptop’s exact source after each committed laptop update.",
        );
      }
    })()
      .catch(() => {
        if (request !== statusRequest.current) return;
        setMessage(
          enrollment?.enabled
            ? "The host didn’t confirm. Automatic dispatch from this laptop is stopped if its local save succeeded; refresh before retrying."
            : "Enrollment wasn’t confirmed and saved, so no automatic update was started. Refresh before retrying.",
        );
      })
      .finally(() => {
        if (request === statusRequest.current) setBusy(false);
      });
  };
  const readiness: FleetHostReadiness = statusUnavailable
    ? "unavailable"
    : status === null
      ? "loading"
      : qualified
        ? "qualified"
        : "setup-required";
  const latestReceipt = selectPublicationReceipt(enrollment, publication?.hosts, {
    status,
    observedAt: statusObservedAt,
    unavailable: statusUnavailable,
  });
  const follower = latestReceipt?.outcome === "unknown" ? undefined : latestReceipt?.follower;
  const publicationNotice = enrollment?.enabled
    ? [
        describePublication(publication?.publication, installedSource),
        describePublicationReceipt(label, latestReceipt),
      ].join(" ")
    : undefined;
  return (
    <FleetEnrollmentStatus
      label={label}
      enrolled={enrollment?.enabled === true}
      readiness={readiness}
      follower={
        enrollment?.enabled === true && follower !== undefined
          ? describeFollower(follower)
          : undefined
      }
      campaign={
        latest === undefined
          ? undefined
          : {
              laptopPhase: latest.campaign.phase,
              hostPhase: latest.member.phase,
              ...(latest.member.reason ? { reason: latest.member.reason } : {}),
            }
      }
      notice={[storageError ?? message, publicationNotice].filter(Boolean).join(" ")}
      continueThreads={continueThreads}
      busy={busy}
      onContinueThreadsChange={setContinueThreads}
      onToggle={() => void toggle()}
      onRefresh={() => {
        const request = ++statusRequest.current;
        setBusy(true);
        void Promise.all([
          refreshFleetDesktopState(),
          fleetHost(environmentId, { action: "status" }),
        ])
          .then(([, next]) => {
            if (request !== statusRequest.current) return;
            setStatus(next);
            setStatusObservedAt(new Date().toISOString());
            setStatusUnavailable(false);
            setMessage(undefined);
          })
          .catch(() => {
            if (request === statusRequest.current) {
              setStatusUnavailable(true);
              setStatusObservedAt(new Date().toISOString());
            }
          })
          .finally(() => {
            if (request === statusRequest.current) setBusy(false);
          });
      }}
    />
  );
}
