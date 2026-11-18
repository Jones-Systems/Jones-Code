import { useRef, useState } from "react";
import { operationOutstanding } from "@t3tools/client-runtime/jones/install-plan";
import type {
  JonesInstallPlan,
  JonesInstallPlanRequest,
} from "@t3tools/contracts/jones/installPlan";
import { Button } from "../../components/ui/button";
import { refreshDeviceEvidence } from "./actions";
import type { InstallStatusStore } from "./panelUi";
import { installPlanServices, requestInstallPlan } from "./services";
import { useInstallPlan } from "./useInstallPlan";

type ManualCloseRequest = Extract<JonesInstallPlanRequest, { type: "close-after-manual-install" }>;
type DeviceObservation = ManualCloseRequest["devices"][number];
const REVIEW_LIFETIME_MS = 2 * 60_000;
const STATUS_BATCH_SIZE = 4;

function selectedDevices(plan: JonesInstallPlan) {
  return plan.devices.filter((device) => device.membership === "selected");
}

function buildDescription(observation: DeviceObservation) {
  const build =
    observation.kind === "unverified"
      ? "Couldn’t verify running build"
      : observation.build.source === undefined
        ? observation.build.version
        : `${observation.build.version} (${observation.build.source.sha.slice(0, 7)})`;
  return observation.directive === undefined
    ? build
    : `${build} · follower review for ${observation.directive.targetSource.slice(0, 7)}`;
}

async function reviewAndClose(planId: string) {
  const binding = installPlanServices();
  const readStatus = binding?.services.readStatus;
  if (binding === undefined || readStatus === undefined)
    throw new Error("Manual install status is unavailable.");
  const { services, store } = binding;
  const readCurrent = async () => {
    if (installPlanServices() !== binding) throw new Error("Install status connection changed.");
    const { state, refused } = await requestInstallPlan({ type: "read" }, binding);
    if (refused !== undefined) throw new Error(refused.message);
    if (state.readOnly !== undefined) throw new Error(state.readOnly.message);
    if (state.current?.planId !== planId || state.view.manualClose === undefined)
      throw new Error("This plan or the running Laptop build changed. Check Install status again.");
    return { plan: state.current, proof: state.view.manualClose };
  };

  const initial = await readCurrent();
  const selected = selectedDevices(initial.plan);
  const startedAt = services.now();
  const readDevice = async (
    device: JonesInstallPlan["devices"][number],
  ): Promise<DeviceObservation> => {
    const { environmentId } = device;
    const needsReconciliation = device.operations.some(operationOutstanding);
    const observed =
      needsReconciliation || device.route === "follower-operations"
        ? await services.observe(environmentId).catch(() => undefined)
        : undefined;
    if (needsReconciliation && observed !== undefined) {
      await refreshDeviceEvidence(binding, environmentId, initial.plan, observed, {
        refreshInstallOutcome: true,
      }).catch(() => undefined);
    }
    const observedAt = services.now();
    const status = await readStatus(environmentId).catch(() => null);
    const source = status?.installedSource;
    const version = status?.currentVersion?.trim();
    const fleetStatus = observed?.fleetStatus;
    const directive =
      fleetStatus?.environmentId === environmentId
        ? fleetStatus.follower?.planDirective
        : undefined;
    const sample = {
      environmentId,
      observedAt,
      ...(directive === undefined
        ? {}
        : {
            directive: { directiveId: directive.directiveId, targetSource: directive.targetSource },
          }),
    };
    if (status?.environmentId !== environmentId || !version || version.length > 128)
      return { ...sample, kind: "unverified" };
    return {
      ...sample,
      kind: "running",
      build: {
        version,
        ...(source === undefined ? {} : { source: { sha: source.sha, tree: source.tree } }),
      },
    };
  };
  const devices: DeviceObservation[] = [];
  for (let i = 0; i < selected.length; i += STATUS_BATCH_SIZE) {
    devices.push(...(await Promise.all(selected.slice(i, i + STATUS_BATCH_SIZE).map(readDevice))));
  }

  const review = await readCurrent();
  if (
    review.proof.activeGeneration !== initial.proof.activeGeneration ||
    selectedDevices(review.plan)
      .map((device) => device.environmentId)
      .join() !== selected.map((device) => device.environmentId).join()
  )
    throw new Error("Laptop or the selected devices changed. Check the devices again.");
  if (
    review.plan.laptop.operations.some(
      (operation) =>
        operation.result === undefined ||
        ["running", "stop-requested", "held"].includes(operation.result.status),
    ) ||
    review.plan.devices.some((device) => device.operations.some(operationOutstanding))
  )
    throw new Error(
      "This plan still has outstanding work. Check the affected device before closing.",
    );

  const { laptop, activeGeneration } = review.proof;
  const versions = devices.map(
    (observation, index) => `${selected[index]!.label}: ${buildDescription(observation)}`,
  );
  if (
    !(await services.confirm(
      [
        "Close this update plan after your manual install?",
        "",
        `Plan target: ${review.plan.target.version} (${review.plan.target.sourceSha.slice(0, 7)})`,
        `Laptop: ${laptop.version} (${laptop.sourceSha.slice(0, 7)})`,
        ...versions,
        "",
        "These observations are saved in Last update. Unstarted work in this plan ends here. Follower settings and installed builds stay as they are.",
      ].join("\n"),
    ))
  )
    return;

  if (installPlanServices() !== binding || services.now() - startedAt > REVIEW_LIFETIME_MS)
    throw new Error("The review expired. Check the devices again.");
  const closed = (state: Awaited<ReturnType<typeof requestInstallPlan>>["state"]) =>
    state.current === null &&
    state.last?.planId === planId &&
    state.last.manualClosure !== undefined;
  const result = await requestInstallPlan(
    {
      type: "close-after-manual-install",
      planId,
      expectedRevision: review.plan.revision,
      laptop,
      activeGeneration,
      devices,
    },
    binding,
  ).catch(async () => {
    if (installPlanServices() !== binding) throw new Error("Install status connection changed.");
    return requestInstallPlan({ type: "read" }, binding);
  });
  if (!closed(result.state) && result.refused !== undefined)
    throw new Error(result.refused.message);
  if (installPlanServices() !== binding || !closed(result.state))
    throw new Error("Closing this plan was not confirmed. Check Install status again.");
  store.update({ open: false, showLast: true, previewSelection: [] });
}

export function ManualInstallClosure({
  store,
  plan,
  readOnly,
}: {
  readonly store: InstallStatusStore;
  readonly plan: JonesInstallPlan;
  readonly readOnly: boolean;
}) {
  const snapshot = useInstallPlan(store);
  const running = useRef(false);
  const [checking, setChecking] = useState(false);
  if (readOnly) {
    const recorded = plan.manualClosure;
    if (recorded === undefined) return null;
    return (
      <div className="space-y-1 px-2 py-2 text-2xs text-muted-foreground">
        <p className="font-medium text-sidebar-foreground">Closed after manual install</p>
        <p>
          Laptop: {recorded.laptop.version} ({recorded.laptop.sourceSha.slice(0, 7)})
        </p>
        {recorded.devices.map((observation) => (
          <p key={observation.environmentId}>
            {
              plan.devices.find((device) => device.environmentId === observation.environmentId)
                ?.label
            }
            : {buildDescription(observation)}
          </p>
        ))}
      </div>
    );
  }
  if (
    snapshot.native?.current?.planId !== plan.planId ||
    snapshot.native.view.manualClose === undefined
  )
    return null;
  return (
    <div className="flex min-h-7 items-center px-2 pb-1">
      <Button
        size="micro"
        variant="ghost-muted"
        disabled={checking}
        onClick={() => {
          if (running.current) return;
          running.current = true;
          setChecking(true);
          store.update({ error: null });
          void reviewAndClose(plan.planId)
            .catch((error: unknown) =>
              store.update({
                error:
                  error instanceof Error
                    ? error.message
                    : "Manual install status could not be verified.",
              }),
            )
            .finally(() => {
              running.current = false;
              setChecking(false);
            });
        }}
      >
        {checking ? "Checking installed versions…" : "I installed manually"}
      </Button>
    </div>
  );
}
