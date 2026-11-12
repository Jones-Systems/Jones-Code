import { randomUUID } from "../../../lib/utils";
import { EnvironmentId, type DesktopUpdateState } from "@t3tools/contracts";
import type {
  JonesInstallPlanGrant,
  JonesInstallPlanRequest,
  JonesInstallPlanResult,
  JonesInstallPlanState,
} from "@t3tools/contracts/jones/installPlan";
import type {
  JonesOperationReceipt,
  JonesOperationStartInput,
} from "@t3tools/contracts/jones/jonesOperations";
import type { JonesUpdateProgress, JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import type { FleetHostStatus } from "@t3tools/contracts/jones/fleet-updates";
import {
  applyEdit,
  isPlanComplete,
  startPlan,
  validateReceipt,
  deviceState,
} from "@t3tools/client-runtime/jones/install-plan";
import { JonesInstallPlanState as PlanStateSchema } from "@t3tools/contracts/jones/installPlan";
import * as Schema from "effect/Schema";
import { createInstallStatusStore, type InstallStatusStore } from "../../installStatus/panelUi";
import type { InstallPlanServices } from "../../installStatus/services";

const FIXTURE_LAPTOP = EnvironmentId.make("install-fixture-laptop");
const decodePlanState = Schema.decodeUnknownSync(PlanStateSchema);
export const FIXTURE_HOSTS = ["VPS", "Mini", "Test"].map((label) => ({
  label,
  environmentId: EnvironmentId.make(`install-fixture-${label.toLowerCase()}`),
}));
/** Joins the plan only in the step progress showcase, where it cannot be reached. */
const FIXTURE_SHOWCASE_HOST = {
  label: "Studio",
  environmentId: EnvironmentId.make("install-fixture-studio"),
};

const SHOWCASE_INSTALL_STEPS = [
  ["mark-threads", "Mark running threads"],
  ["verify-runtime", "Verify runtime"],
  ["startup-readiness", "Check startup readiness"],
  ["capture-continuity", "Capture continuity"],
  ["launcher-handoff", "Hand off to launcher"],
  ["restart", "Restart"],
  ["confirm-restart", "Confirm restart"],
] as const;
const SHOWCASE_DOWNLOAD_STEPS = [
  ["download", "Download build"],
  ["verify-stage", "Verify download"],
] as const;

/**
 * A synthetic progress record at one step. Finished steps take 20 seconds each and end when the
 * current step starts. `at` turns a millisecond offset before now into an ISO time.
 */
function showcaseProgress(input: {
  readonly kind: "install" | "download";
  readonly operationId: string;
  readonly stepIndex: number;
  readonly phase: JonesUpdateProgress["phase"];
  readonly stepAgoMs: number;
  readonly updatedAgoMs: number;
  /** When the host served this record, which is when the client heard it. */
  readonly reportedAgoMs: number;
  readonly at: (agoMs: number) => string;
  readonly error?: JonesUpdateProgress["error"];
}): JonesUpdateProgress {
  const catalog = input.kind === "install" ? SHOWCASE_INSTALL_STEPS : SHOWCASE_DOWNLOAD_STEPS;
  const finished = input.stepIndex - 1;
  const stepStarted = input.stepAgoMs;
  const [stepKey, stepLabel] = catalog[input.stepIndex - 1]!;
  return {
    schema: 1,
    kind: input.kind,
    operationId: input.operationId,
    phase: input.phase,
    stepIndex: input.stepIndex,
    stepCount: catalog.length,
    stepKey,
    stepLabel,
    stepStartedAt: input.at(stepStarted),
    startedAt: input.at(stepStarted + finished * 20_000),
    updatedAt: input.at(input.updatedAgoMs),
    reportedAt: input.at(input.reportedAgoMs),
    steps: catalog.map(([key, label], offset) => {
      const index = offset + 1;
      if (index > input.stepIndex) return { key, label };
      if (index === input.stepIndex)
        return {
          key,
          label,
          startedAt: input.at(stepStarted),
          ...(input.phase === "failed" ? { endedAt: input.at(input.updatedAgoMs) } : {}),
        };
      const ended = stepStarted + (finished - index) * 20_000;
      return { key, label, startedAt: input.at(ended + 20_000), endedAt: input.at(ended) };
    }),
    ...(input.error === undefined ? {} : { error: input.error }),
  };
}

/** Scripted receipts exercise renderer boundaries only; none proves a native or host effect. */
export function createScriptedInstallPlan(input: {
  readonly readDesktop: () => DesktopUpdateState;
  readonly publishDesktop: (state: DesktopUpdateState) => void;
  readonly record: (action: string, payload?: unknown) => void;
  readonly storageKey?: string;
  readonly now?: () => number;
  readonly store?: InstallStatusStore;
  readonly nextLaptopOutcome?: (action: "download" | "install") => "accept" | "reject" | "refuse";
}) {
  const now = input.now ?? Date.now;
  const iso = () => new Date(now()).toISOString();
  const uuid = () => randomUUID();
  const store = input.store ?? createInstallStatusStore();
  const listeners = new Set<(state: JonesInstallPlanState) => void>();
  const receipts = new Map<string, JonesOperationReceipt>();
  const hosts = new Map<EnvironmentId, JonesUpdateState>(
    FIXTURE_HOSTS.map((host) => [
      host.environmentId,
      {
        ...input.readDesktop().jones!,
        environmentId: host.environmentId,
        phase: "available" as const,
        capability: {
          check: true,
          download: true,
          install: true,
          exactSource: true,
          operations: 1 as const,
        },
      },
    ]),
  );
  const fleets = new Map<EnvironmentId, FleetHostStatus>(
    FIXTURE_HOSTS.map((host) => [
      host.environmentId,
      {
        environmentId: host.environmentId,
        operationProtocol: 1,
        planDirectiveProtocol: 1,
        enrollment: null,
        operation: null,
        update: hosts.get(host.environmentId)!,
      },
    ]),
  );
  /** Showcase-only reachability: when each host was last heard, and whether it is fresh. */
  const reachability = new Map<
    EnvironmentId,
    { readonly heardAt: number; readonly freshness: "fresh" | "stale" }
  >();
  const showcaseHosts: (typeof FIXTURE_SHOWCASE_HOST)[] = [];
  let native: JonesInstallPlanState = {
    schema: 1,
    current: null,
    last: null,
    laptop: { environmentId: FIXTURE_LAPTOP, observedAt: iso(), activationBlocked: false },
    view: { complete: false },
  };
  if (input.storageKey !== undefined) {
    const saved = sessionStorage.getItem(input.storageKey);
    if (saved !== null) native = decodePlanState(JSON.parse(saved));
    for (const device of native.current?.devices ?? []) {
      for (const operation of device.operations)
        if (operation.receipt !== undefined) receipts.set(operation.operationId, operation.receipt);
      const latest = device.operations.at(-1)?.receipt;
      if (latest !== undefined)
        hosts.set(device.environmentId, {
          ...hosts.get(device.environmentId)!,
          operation: latest,
          phase:
            latest.phase === "completed" && latest.action.kind !== "install"
              ? "staged"
              : latest.phase === "stop-requested"
                ? "downloading"
                : latest.phase === "held"
                  ? "blocked"
                  : "available",
          ...(latest.staged === undefined ? {} : { stagedHandle: latest.staged.stagedHandle }),
        });
    }
  }
  const notify = () => {
    native = {
      ...native,
      view: { complete: native.current !== null && isPlanComplete(native.current) },
    };
    if (input.storageKey !== undefined)
      sessionStorage.setItem(input.storageKey, JSON.stringify(native));
    store.update({ native, desktop: input.readDesktop(), devices: devices() });
    for (const listener of listeners) listener(native);
  };
  const devices = () =>
    [...FIXTURE_HOSTS, ...showcaseHosts].map((host) => {
      const reach = reachability.get(host.environmentId);
      if (reach === undefined)
        return {
          ...host,
          observation: { state: hosts.get(host.environmentId)!, freshness: "fresh" as const },
          canOperate: true,
          fleetStatus: fleets.get(host.environmentId)!,
          lastObservedAt: iso(),
        };
      const { heardAt } = reach;
      return {
        ...host,
        observation: {
          state: hosts.get(host.environmentId)!,
          freshness: reach.freshness,
          heardAt,
          ...(reach.freshness === "stale"
            ? { message: "Update status is temporarily unavailable; retrying." }
            : {}),
        },
        canOperate: true,
        fleetStatus: fleets.get(host.environmentId)!,
        lastObservedAt: new Date(heardAt).toISOString(),
      };
    });
  const result = (
    extras: Omit<Partial<JonesInstallPlanResult>, "state"> = {},
  ): JonesInstallPlanResult => ({ state: native, ...extras });
  const refuse = (code: NonNullable<JonesInstallPlanResult["refused"]>["code"], message: string) =>
    result({ refused: { code, message } });
  const revise = (plan: NonNullable<JonesInstallPlanState["current"]>) => {
    native = { ...native, current: { ...plan, revision: plan.revision + 1 } };
    notify();
  };
  function laptopChanged() {
    const desktop = input.readDesktop();
    const jones = desktop.jones;
    if (jones === undefined) return;
    native = {
      ...native,
      laptop: {
        environmentId: FIXTURE_LAPTOP,
        observedAt: iso(),
        activationBlocked: jones.phase === "preparing" || jones.phase === "installing",
        activeVersion: desktop.currentVersion,
        ...(jones.installedSource === undefined
          ? {}
          : { activeSource: jones.installedSource.sha, activeTree: jones.installedSource.tree }),
        ...(jones.phase === "committed"
          ? {
              committedTransactionId: "fixture-native-commit",
              activeGeneration: "fixture-generation-2",
            }
          : {}),
      },
    };
    const plan = native.current;
    if (plan !== null) {
      const previous = plan.laptop.operations.at(-1);
      const status =
        jones.phase === "staged"
          ? "completed"
          : jones.phase === "committed"
            ? "committed"
            : jones.phase === "rolled-back"
              ? "rolled-back"
              : undefined;
      const gate =
        jones.phase === "committed" && jones.installedSource?.sha === plan.target.sourceSha
          ? {
              openedAt: iso(),
              basis: "plan-install" as const,
              transactionId: "fixture-native-commit",
              activeGeneration: "fixture-generation-2",
              activeSource: plan.target.sourceSha,
              activeTree: plan.target.sourceTree,
            }
          : plan.laptop.gate;
      native = {
        ...native,
        current: {
          ...plan,
          revision: plan.revision + 1,
          laptop: {
            ...plan.laptop,
            ...(gate === undefined ? {} : { gate }),
            operations:
              previous !== undefined && status !== undefined
                ? [
                    ...plan.laptop.operations.slice(0, -1),
                    { ...previous, result: { status, at: iso() } },
                  ]
                : plan.laptop.operations,
          },
        },
      };
    }
    notify();
  }
  async function laptop(
    action: "download" | "install" | "stop",
    review?: import("@t3tools/contracts/jones/installPlan").JonesInstallPlanReview,
  ) {
    const plan = native.current;
    if (plan === null) return refuse("plan-mismatch", "No current fixture plan.");
    const desktop = input.readDesktop();
    if (
      action === "install" &&
      (review === undefined ||
        review.stagedHandle !== desktop.jones?.stagedHandle ||
        review.targetSource !== plan.target.sourceSha)
    )
      return refuse("invalid-evidence", "Fixture install requires the reviewed stage.");
    const outcome = action === "stop" ? "accept" : (input.nextLaptopOutcome?.(action) ?? "accept");
    const download = plan.laptop.operations.filter((o) => o.step === "download").at(-1);
    if (action === "stop" && download === undefined)
      return refuse("outstanding-operation", "No Laptop download is recorded.");
    const op = {
      operationId: uuid(),
      step: action,
      requestedAt: iso(),
      ...(review === undefined ? {} : { review }),
      ...(action === "stop" ? { of: download!.operationId } : {}),
      result: {
        status: action === "stop" ? ("stop-requested" as const) : ("running" as const),
        at: iso(),
      },
    };
    revise({ ...plan, laptop: { ...plan.laptop, operations: [...plan.laptop.operations, op] } });
    input.record(`fixture-laptop-${action}`, {
      planId: plan.planId,
      operationId: op.operationId,
      fixtureOnly: true,
    });
    if (outcome !== "accept") {
      const message = `Synthetic ${action} ${outcome === "reject" ? "transport failure; native outcome not confirmed" : "refused"}.`;
      revise({
        ...native.current!,
        laptop: {
          ...native.current!.laptop,
          operations: native.current!.laptop.operations.map((entry) =>
            entry.operationId === op.operationId
              ? {
                  ...entry,
                  result: { status: outcome === "reject" ? "held" : "refused", at: iso(), message },
                }
              : entry,
          ),
        },
      });
      input.publishDesktop({
        ...desktop,
        status: "error",
        message,
        errorContext: action === "stop" ? "download" : action,
        jones: { ...desktop.jones!, phase: "blocked", message },
      });
      return result();
    }
    input.publishDesktop({
      ...desktop,
      status: action === "download" ? "downloading" : desktop.status,
      downloadPercent: null,
      jones: {
        ...desktop.jones!,
        phase:
          action === "install"
            ? "preparing"
            : action === "download"
              ? "downloading"
              : desktop.jones!.phase,
      },
    });
    return result();
  }
  async function request(request: JonesInstallPlanRequest): Promise<JonesInstallPlanResult> {
    input.record("fixture-installPlan", request);
    if (request.type === "read") return result();
    if (request.type === "recovery-preflight")
      return refuse("unsupported", "Fixture recovery capability is not advertised.");
    if (request.type === "publication")
      return result({
        publication: {
          kind: "withheld",
          code: "plan-managed",
          message: "Fixture never publishes a plan target.",
        },
      });
    if (request.type === "start") {
      if (native.current !== null && native.current.laptop.operations.length > 0)
        return refuse("plan-active", "Finish the current plan first.");
      if (request.laptop === undefined && native.laptop.activeSource !== request.target.sourceSha)
        return refuse(
          "laptop-action-required",
          "Start with Download on Laptop; review staged installation from Install status.",
        );
      native = {
        ...native,
        last: native.current ?? native.last,
        current: startPlan({
          planId: uuid(),
          origin: request.origin,
          target: request.target,
          from: { version: input.readDesktop().currentVersion },
          laptopEnvironmentId: FIXTURE_LAPTOP,
          select: request.select,
          now: iso(),
        }),
      };
      notify();
      return request.laptop === undefined
        ? result()
        : laptop(request.laptop.action, request.laptop.review);
    }
    const plan = native.current;
    if (plan === null || plan.planId !== request.planId)
      return refuse("plan-mismatch", "The fixture plan changed.");
    if (request.type === "edit") {
      const edit = applyEdit(plan, request, iso());
      if (edit.refused !== undefined)
        return refuse(edit.refused, "The plan changed during Edit. Review the current rows.");
      native = { ...native, current: edit.plan };
      notify();
      return result({ applied: edit.applied });
    }
    if (request.type === "dismiss") {
      if (request.expectedRevision !== plan.revision)
        return refuse("stale-revision", "The plan changed before Done.");
      if (!isPlanComplete(plan))
        return refuse("not-complete", "The whole chosen plan has not finished.");
      native = { ...native, current: null, last: { ...plan, dismissedAt: iso() } };
      notify();
      return result();
    }
    if (request.type === "laptop")
      return request.action === "preflight"
        ? refuse("unsupported", "Fixture recovery capability is not advertised.")
        : laptop(request.action, request.review);
    const device = plan.devices.find((d) => d.environmentId === request.environmentId);
    if (device === undefined || device.membership !== "selected")
      return refuse("not-selectable", "The fixture device is not selected.");
    if (request.type === "end") {
      if (request.expectedRevision !== plan.revision)
        return refuse("stale-revision", "The plan changed.");
      if (deviceState(plan, device, now()) !== "staged")
        return refuse("outstanding-operation", "Only a retained stage can end.");
      revise({
        ...plan,
        devices: plan.devices.map((d) => (d === device ? { ...d, ended: { at: iso() } } : d)),
      });
      return result();
    }
    if (request.type === "report") {
      if (request.evidence.kind !== "receipt")
        return refuse("invalid-evidence", "This fixture scripts manual hosts only.");
      const receipt = request.evidence.receipt;
      const op = device.operations.find((o) => o.operationId === request.operationId);
      if (
        op?.request.kind !== "operation-start" ||
        !validateReceipt(op.request.input, receipt) ||
        (op.receipt !== undefined && !validateReceipt(op.receipt, receipt))
      )
        return refuse("invalid-evidence", "Receipt binding failed.");
      if (op.receipt !== undefined && JSON.stringify(op.receipt) === JSON.stringify(receipt))
        return result();
      revise({
        ...plan,
        devices: plan.devices.map((d) =>
          d === device
            ? { ...d, operations: d.operations.map((o) => (o === op ? { ...o, receipt } : o)) }
            : d,
        ),
      });
      return result();
    }
    if (plan.laptop.gate === undefined || plan.laptop.gateClosed !== undefined)
      return refuse("gate-closed", "Laptop is not verified.");
    const lease = {
      claimant: request.claimant,
      reservedAt: iso(),
      leaseUntil: new Date(now() + 120_000).toISOString(),
    };
    if (request.step === "stop" || request.step === "fence") {
      const op = device.operations.find((o) => o.operationId === request.operationId);
      if (op?.request.kind !== "operation-start")
        return refuse("outstanding-operation", "No bound operation.");
      if (
        request.step === "stop" &&
        (op.receipt === undefined || !["accepted", "running"].includes(op.receipt.phase))
      )
        return refuse("outstanding-operation", "Download cannot be stopped in this phase.");
      const grant: JonesInstallPlanGrant = {
        planId: plan.planId,
        environmentId: device.environmentId,
        operationId: op.operationId,
        step: request.step,
        leaseUntil: lease.leaseUntil,
        request:
          request.step === "stop"
            ? {
                kind: "operation-stop",
                input: {
                  operationId: op.operationId,
                  stopId: op.stop?.stopId ?? uuid(),
                  environmentId: device.environmentId,
                },
              }
            : { kind: "operation-fence", input: op.request.input },
      };
      return result({ grant });
    }
    if (request.step !== "download" && request.step !== "install")
      return refuse("unsupported", "Fixture directive is not advertised.");
    const latest = device.operations.at(-1);
    if (
      latest !== undefined &&
      latest.receipt === undefined &&
      latest.lease.claimant === request.claimant &&
      latest.request.kind === "operation-start"
    )
      return result({
        grant: {
          planId: plan.planId,
          environmentId: device.environmentId,
          operationId: latest.operationId,
          step: request.step,
          leaseUntil: latest.lease.leaseUntil,
          request: latest.request,
        },
      });
    if (
      latest !== undefined &&
      ["accepted", "running", "stop-requested", "held", "handed-off", "adopting"].includes(
        latest.receipt?.phase ?? "held",
      )
    )
      return refuse("outstanding-operation", "An operation is outstanding.");
    if (request.action === undefined || request.expectedInstalledSource === undefined)
      return refuse("invalid-evidence", "A bound action is required.");
    if (request.step === "install" && (!request.explicit || request.review === undefined))
      return refuse("invalid-evidence", "Every install needs review.");
    const operationId = uuid();
    const start: JonesOperationStartInput = {
      operationId,
      environmentId: device.environmentId,
      expectedInstalledSource: request.expectedInstalledSource,
      action: request.action,
      correlation: { planId: plan.planId, deviceKey: device.environmentId },
    };
    const op = {
      operationId,
      step: request.step,
      request: { kind: "operation-start" as const, input: start },
      lease,
      ...(request.review === undefined ? {} : { review: request.review }),
    };
    revise({
      ...plan,
      devices: plan.devices.map((d) =>
        d === device ? { ...d, operations: [...d.operations, op] } : d,
      ),
    });
    return result({
      grant: {
        planId: plan.planId,
        environmentId: device.environmentId,
        operationId,
        step: request.step,
        leaseUntil: lease.leaseUntil,
        request: op.request,
      },
    });
  }
  const publishReceipt = (receipt: JonesOperationReceipt) => {
    receipts.set(receipt.operationId, receipt);
    const old = hosts.get(receipt.environmentId)!;
    const committed = receipt.phase === "completed" && receipt.action.kind === "install";
    hosts.set(receipt.environmentId, {
      ...old,
      operation: receipt,
      phase:
        receipt.phase === "stop-requested" || receipt.phase === "running"
          ? "downloading"
          : receipt.phase === "held"
            ? "blocked"
            : receipt.phase === "completed" && !committed
              ? "staged"
              : committed
                ? "committed"
                : "available",
      ...(receipt.staged === undefined ? {} : { stagedHandle: receipt.staged.stagedHandle }),
      ...(native.current === null || old.provenance === undefined
        ? {}
        : {
            provenance: {
              ...old.provenance,
              sourceSha: native.current.target.sourceSha,
              sourceTree: native.current.target.sourceTree,
              version: native.current.target.version,
            },
          }),
      ...(committed
        ? {
            currentVersion: native.current!.target.version,
            installedSource: {
              repository: "Jones-Systems/Jones-Code",
              sha: native.current!.target.sourceSha,
              tree: native.current!.target.sourceTree,
            },
          }
        : {}),
    });
    store.update({ devices: devices() });
    input.record("fixture-target-receipt", { ...receipt, fixtureOnly: true });
  };
  const services = (bridge: InstallPlanServices["bridge"]): InstallPlanServices => ({
    bridge,
    devices,
    observe: async (id) => {
      const d = devices().find((d) => d.environmentId === id);
      if (d === undefined) throw new Error("Unknown fixture host.");
      return d;
    },
    legacy: async () => {
      throw new Error("Fixture host effects require operations.");
    },
    operation: async (id, command) => {
      input.record(`fixture-host-${command.action}`, { environmentId: id, input: command.input });
      if (command.action === "operationRead")
        return {
          receipts: command.input.operationIds.map((operationId) => ({
            operationId,
            receipt: receipts.get(operationId) ?? null,
          })),
        };
      if (command.action === "recoverCleanup") return { recoveredCleanup: null };
      if (command.action === "recoveryPreflight")
        throw new Error("Fixture recovery is unsupported.");
      const previous = receipts.get(command.input.operationId);
      if (previous !== undefined) return previous;
      const receipt: JonesOperationReceipt = {
        protocol: 1,
        ...command.input,
        phase: command.action === "operationFence" ? "fenced" : "running",
        updatedAt: iso(),
        ...(command.action === "operationFence" ? {} : { acceptedAt: iso(), jobId: uuid() }),
      };
      publishReceipt(receipt);
      return receipt;
    },
    stop: async (id, command) => {
      input.record("fixture-host-operationStop", { environmentId: id, input: command.input });
      const receipt = receipts.get(command.input.operationId);
      if (receipt === undefined) throw new Error("No fixture operation was accepted.");
      const next: JonesOperationReceipt = {
        ...receipt,
        phase: "stop-requested",
        updatedAt: iso(),
        stop: { stopId: command.input.stopId, requestedAt: iso() },
      };
      publishReceipt(next);
      return next;
    },
    host: async (id) => fleets.get(id)!,
    confirm: async (message) => {
      input.record("fixture-review", { message });
      return true;
    },
    chooseRecovery: async () => null,
    now,
  });
  const settleHost = async (
    label: string,
    phase: "staged" | "committed" | "stopped" | "held",
    heldCode: "unreaped" | "cleanup-unconfirmed" = "unreaped",
  ) => {
    const device = native.current?.devices.find((d) => d.label === label);
    const op = device?.operations.at(-1);
    if (device === undefined || op === undefined || op.request.kind !== "operation-start")
      throw new Error("No matching fixture host operation.");
    const previous = receipts.get(op.operationId);
    if (previous === undefined) throw new Error("No accepted fixture operation.");
    const receipt: JonesOperationReceipt = {
      ...previous,
      phase: phase === "staged" || phase === "committed" ? "completed" : phase,
      updatedAt: iso(),
      ...(phase === "staged"
        ? {
            staged: {
              stagedHandle: `fixture-stage-${label}`,
              sourceSha: native.current!.target.sourceSha,
              sourceTree: native.current!.target.sourceTree,
              artifactId: 9001,
              version: native.current!.target.version,
            },
          }
        : {}),
      ...(phase === "committed"
        ? {
            updateId: op.operationId,
            outcome: {
              status: "committed",
              fromVersion:
                hosts.get(device.environmentId)!.currentVersion ??
                input.readDesktop().currentVersion,
              targetVersion: native.current!.target.version,
            },
          }
        : {}),
      ...(phase === "held"
        ? { code: heldCode, message: "Synthetic cleanup is unproved. No new work starts." }
        : {}),
      ...(phase === "stopped" && previous.stop !== undefined
        ? { stop: { ...previous.stop, result: "stopped" } }
        : {}),
    };
    publishReceipt(receipt);
    await request({
      type: "report",
      planId: native.current!.planId,
      environmentId: device.environmentId,
      operationId: op.operationId,
      evidence: { kind: "receipt", receipt },
    });
  };
  const settleLaptopStop = (held = false) => {
    const plan = native.current;
    if (plan === null) throw new Error("No fixture plan.");
    revise({
      ...plan,
      laptop: {
        ...plan.laptop,
        operations: plan.laptop.operations.map((op) =>
          op.result?.status === "running" || op.result?.status === "stop-requested"
            ? { ...op, result: { status: held ? "held" : "stopped", at: iso() } }
            : op,
        ),
      },
    });
    const desktop = input.readDesktop();
    input.publishDesktop({
      ...desktop,
      status: held ? "error" : "available",
      downloadPercent: null,
      jones: { ...desktop.jones!, phase: held ? "blocked" : "available" },
    });
  };
  const setSupported = (label: string, supported: boolean) => {
    const id = FIXTURE_HOSTS.find((h) => h.label === label)?.environmentId;
    if (id === undefined) throw new Error("Unknown fixture host.");
    const host = hosts.get(id)!;
    hosts.set(id, {
      ...host,
      capability: {
        check: true,
        download: true,
        install: true,
        exactSource: true,
        ...(supported ? { operations: 1 as const } : {}),
      },
    });
    notify();
  };
  /**
   * Replace the plan with a verified Laptop and four hosts mid-flight: VPS preparing an install,
   * Mini failed at a step, Test stalled mid-download, and Studio unreachable. Synthetic only.
   */
  const showcaseStepProgress = () => {
    const desktop = input.readDesktop();
    const provenance = desktop.jones?.provenance;
    if (provenance === undefined) throw new Error("The showcase needs a Laptop target build.");
    const at = (agoMs: number) => new Date(now() - agoMs).toISOString();
    if (!showcaseHosts.includes(FIXTURE_SHOWCASE_HOST)) {
      showcaseHosts.push(FIXTURE_SHOWCASE_HOST);
      const template = hosts.get(FIXTURE_HOSTS[0]!.environmentId)!;
      hosts.set(FIXTURE_SHOWCASE_HOST.environmentId, {
        ...template,
        environmentId: FIXTURE_SHOWCASE_HOST.environmentId,
      });
      fleets.set(FIXTURE_SHOWCASE_HOST.environmentId, {
        ...fleets.get(FIXTURE_HOSTS[0]!.environmentId)!,
        environmentId: FIXTURE_SHOWCASE_HOST.environmentId,
        update: hosts.get(FIXTURE_SHOWCASE_HOST.environmentId)!,
      });
    }
    const target = {
      repository: "Jones-Systems/Jones-Code" as const,
      sourceSha: provenance.sourceSha,
      sourceTree: provenance.sourceTree,
      version: provenance.version ?? desktop.availableVersion ?? "fixture",
    };
    const all = [...FIXTURE_HOSTS, FIXTURE_SHOWCASE_HOST];
    const plan = startPlan({
      planId: uuid(),
      origin: "update-button",
      target,
      from: { version: desktop.currentVersion },
      laptopEnvironmentId: FIXTURE_LAPTOP,
      select: all.map(({ environmentId, label }) => ({
        environmentId,
        label,
        route: "operations" as const,
      })),
      now: at(30 * 60_000),
    });
    const byLabel = (label: string) => all.find((host) => host.label === label)!;
    const scenarios = [
      {
        host: byLabel("VPS"),
        step: "install" as const,
        phase: "running" as const,
        progress: { kind: "install" as const, stepIndex: 3, phase: "prep" as const },
        stepAgoMs: 102_000,
        updatedAgoMs: 8_000,
        reach: { heardAgoMs: 5_000, freshness: "fresh" as const },
      },
      {
        host: byLabel("Mini"),
        step: "install" as const,
        phase: "failed" as const,
        progress: { kind: "install" as const, stepIndex: 3, phase: "failed" as const },
        stepAgoMs: 9 * 60_000,
        updatedAgoMs: 8 * 60_000,
        reach: { heardAgoMs: 20_000, freshness: "fresh" as const },
        error: {
          code: "startup-not-ready",
          message: "The new build did not report ready within 60 seconds. Nothing was installed.",
        },
      },
      {
        host: byLabel("Test"),
        step: "download" as const,
        phase: "running" as const,
        progress: { kind: "download" as const, stepIndex: 1, phase: "download" as const },
        stepAgoMs: 7 * 60_000,
        updatedAgoMs: 4 * 60_000,
        reach: { heardAgoMs: 12_000, freshness: "fresh" as const },
      },
      {
        host: byLabel("Studio"),
        step: "install" as const,
        phase: "running" as const,
        progress: { kind: "install" as const, stepIndex: 4, phase: "prep" as const },
        stepAgoMs: 19 * 60_000,
        updatedAgoMs: 18 * 60_000,
        reach: { heardAgoMs: 18 * 60_000, freshness: "stale" as const },
      },
    ];
    const devicesWithWork = plan.devices.map((device) => {
      const scenario = scenarios.find((s) => s.host.environmentId === device.environmentId)!;
      const operationId = uuid();
      const start: JonesOperationStartInput = {
        operationId,
        environmentId: device.environmentId,
        expectedInstalledSource: desktop.jones!.installedSource?.sha ?? "b".repeat(40),
        action:
          scenario.step === "install"
            ? {
                kind: "install",
                stagedHandle: `fixture-stage-${device.label}`,
                currentVersion: desktop.currentVersion,
                targetSource: target.sourceSha,
              }
            : { kind: "download-exact", targetSource: target.sourceSha },
        correlation: { planId: plan.planId, deviceKey: device.environmentId },
      };
      const receipt: JonesOperationReceipt = {
        protocol: 1,
        ...start,
        phase: scenario.phase,
        acceptedAt: at(scenario.stepAgoMs + 60_000),
        jobId: uuid(),
        updatedAt: at(scenario.updatedAgoMs),
        ...(scenario.error === undefined
          ? {}
          : { code: scenario.error.code, message: scenario.error.message }),
      };
      receipts.set(operationId, receipt);
      hosts.set(device.environmentId, {
        ...hosts.get(device.environmentId)!,
        operation: receipt,
        phase:
          scenario.phase === "failed"
            ? "error"
            : scenario.step === "install"
              ? "preparing"
              : "downloading",
        progress: showcaseProgress({
          ...scenario.progress,
          operationId,
          stepAgoMs: scenario.stepAgoMs,
          updatedAgoMs: scenario.updatedAgoMs,
          reportedAgoMs: scenario.reach.heardAgoMs,
          at,
          ...(scenario.error === undefined ? {} : { error: scenario.error }),
        }),
      });
      reachability.set(device.environmentId, {
        heardAt: now() - scenario.reach.heardAgoMs,
        freshness: scenario.reach.freshness,
      });
      return {
        ...device,
        operations: [
          {
            operationId,
            step: scenario.step,
            request: { kind: "operation-start" as const, input: start },
            lease: { claimant: "fixture-showcase", reservedAt: at(0), leaseUntil: at(-120_000) },
            receipt,
          },
        ],
      };
    });
    native = {
      ...native,
      current: {
        ...plan,
        laptop: {
          ...plan.laptop,
          gate: {
            openedAt: at(25 * 60_000),
            basis: "plan-install",
            transactionId: "fixture-native-commit",
            activeGeneration: "fixture-generation-2",
            activeSource: target.sourceSha,
            activeTree: target.sourceTree,
          },
        },
        devices: devicesWithWork,
      },
    };
    notify();
    store.update({
      pollHealth: {
        ...store.get().pollHealth,
        [FIXTURE_SHOWCASE_HOST.environmentId]: { failures: 3, lastSuccessAt: now() - 18 * 60_000 },
      },
    });
  };
  /** Current source evidence alongside interrupted, staged, cached and held attempts. */
  const showcaseSourceProof = () => {
    showcaseStepProgress();
    const plan = native.current!;
    const at = (agoMs: number) => new Date(now() - agoMs).toISOString();
    const phases = ["interrupted", "completed", "interrupted", "held"] as const;
    const devicesWithProof = plan.devices.map((device, index) => {
      const operationId = uuid();
      const phase = phases[index]!;
      const action = { kind: "download-exact" as const, targetSource: plan.target.sourceSha };
      const staged =
        phase === "completed"
          ? {
              stagedHandle: `fixture-proof-${index}`,
              sourceSha: plan.target.sourceSha,
              sourceTree: plan.target.sourceTree,
              artifactId: 9001,
            }
          : undefined;
      const receipt: JonesOperationReceipt = {
        protocol: 1,
        operationId,
        environmentId: device.environmentId,
        action,
        expectedInstalledSource: "b".repeat(40),
        phase,
        acceptedAt: at(600_000),
        updatedAt: at(300_000),
        jobId: operationId,
        ...(staged === undefined ? {} : { staged }),
        ...(phase === "held"
          ? { code: "cleanup-unconfirmed" }
          : phase === "interrupted"
            ? { code: "cleanup-recovered" }
            : {}),
      };
      receipts.set(operationId, receipt);
      const host = hosts.get(device.environmentId)!;
      hosts.set(device.environmentId, {
        ...host,
        phase: staged === undefined ? "available" : "staged",
        operation: receipt,
        installedSource: {
          repository: plan.target.repository,
          sha: plan.target.sourceSha,
          tree: plan.target.sourceTree,
        },
        ...(staged === undefined ? {} : { stagedHandle: staged.stagedHandle }),
        progress: showcaseProgress({
          kind: "download",
          operationId,
          stepIndex: 2,
          phase: phase === "completed" ? "done" : "failed",
          stepAgoMs: 360_000,
          updatedAgoMs: 300_000,
          reportedAgoMs: index === 2 ? 300_000 : 0,
          at,
          ...(phase === "completed"
            ? {}
            : { error: { code: receipt.code!, message: "Synthetic earlier attempt." } }),
        }),
      });
      reachability.set(device.environmentId, {
        freshness: index === 2 ? "stale" : "fresh",
        heardAt: now() - (index === 2 ? 300_000 : 0),
      });
      return {
        ...device,
        operations: [
          {
            operationId,
            step: "download" as const,
            lease: {
              claimant: "fixture-source-proof",
              reservedAt: at(600_000),
              leaseUntil: at(480_000),
            },
            request: { kind: "operation-start" as const, input: receipt },
            receipt,
          },
        ],
      };
    });
    native = { ...native, current: { ...plan, devices: devicesWithProof } };
    notify();
  };
  notify();
  return {
    request,
    showcaseStepProgress,
    showcaseSourceProof,
    onInstallPlan: (listener: (state: JonesInstallPlanState) => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    services,
    store,
    laptopChanged,
    publishHostReceipt: publishReceipt,
    settleHost,
    settleLaptopStop,
    setSupported,
    snapshot: () => native,
  };
}
