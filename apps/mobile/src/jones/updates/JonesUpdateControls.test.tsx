// @effect-diagnostics globalDate:off - Synthetic receipts use fixed timestamps; no host or filesystem is accessed.
import type { JonesUpdateObservation } from "@t3tools/client-runtime/jones/updates";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import type {
  JonesOperationReceipt,
  JonesOperationStartInput,
  JonesRecoveryPreflight,
} from "@t3tools/contracts/jones/jonesOperations";
import { EnvironmentId } from "@t3tools/contracts";
import type { AlertButton } from "react-native";
import type { ReactNode } from "react";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const harness = vi.hoisted(() => ({
  observation: { state: null, freshness: "loading" } as JonesUpdateObservation,
  action: vi.fn(),
  operation: vi.fn(),
  stop: vi.fn(),
  alert: vi.fn(),
  nextId: 1,
}));
vi.mock("@effect/atom-react", async () => {
  const { createContext } = await import("react");
  return {
    useAtomValue: () => harness.observation,
    RegistryContext: createContext({ get: () => harness.observation }),
  };
});
vi.mock("@t3tools/client-runtime/jones/updates", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@t3tools/client-runtime/jones/updates")>()),
  createJonesUpdateAtoms: () => ({
    observation: () => "observation",
    action: "action",
    operation: "operation",
    stop: "stop",
  }),
}));
vi.mock("../../connection/runtime", () => ({ connectionAtomRuntime: {} }));
vi.mock("./actionJournal", async () => {
  const { createJonesActionJournal } =
    await import("@t3tools/client-runtime/jones/updates/action-journal");
  const disk = new Map<string, string>();
  return {
    mobileJonesJournal: createJonesActionJournal({
      getItem: (key) => disk.get(key) ?? null,
      setItem: (key, value) => {
        disk.set(key, value);
      },
    }),
  };
});
vi.mock("../../lib/uuid", () => ({
  uuidv4: () => `11111111-1111-4111-8111-${String(harness.nextId++).padStart(12, "0")}`,
}));
vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: (command: "action" | "operation" | "stop") => harness[command],
}));
vi.mock("react-native", () => ({
  View: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  Alert: { alert: harness.alert },
}));
vi.mock("../../components/ui/AppText", () => ({
  AppText: ({ children }: { children: ReactNode }) => <span>{children}</span>,
}));
vi.mock("../../features/settings/components/SettingsSection", () => ({
  SettingsSection: ({ children }: { children: ReactNode }) => <section>{children}</section>,
}));
vi.mock("../../features/settings/components/SettingsActionRow", () => ({
  SettingsActionRow: ({
    label,
    disabled,
    onPress,
  }: {
    label: string;
    disabled?: boolean;
    onPress: () => void;
  }) => (
    <button type="button" disabled={disabled} onClick={onPress}>
      {label}
    </button>
  ),
}));

import { JonesUpdateControls } from "./JonesUpdateControls";

const source = "a".repeat(40),
  installed = "b".repeat(40),
  tree = "c".repeat(40);
const stamp = "2026-10-27T05:00:00.000Z";
const planId = "22222222-2222-4222-8222-222222222222";
let root: Root;
let container: HTMLDivElement;
let environmentId: EnvironmentId;
let environmentSequence = 0;
let allowed: boolean;

const success = (value: unknown) => ({ _tag: "Success", value });
function state(staged = false): JonesUpdateState {
  return {
    source: "jones-actions",
    channel: "jones-main",
    phase: staged ? "staged" : "available",
    environmentId,
    currentVersion: "old",
    installedSource: { repository: "Jones-Systems/Jones-Code", sha: installed, tree },
    capability: { check: true, download: true, install: true, exactSource: true, operations: 1 },
    ...(staged ? { stagedHandle: "stage" } : {}),
    provenance: {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: source,
      sourceTree: tree,
      workflow: "linux",
      runId: 1,
      runAttempt: 1,
      artifactId: 2,
      artifactDigest: "d".repeat(64),
      platform: "linux",
      architecture: "x64",
      version: "new",
    },
  };
}
function receipt(
  request: JonesOperationStartInput,
  phase: JonesOperationReceipt["phase"] = "accepted",
): JonesOperationReceipt {
  return { ...request, protocol: 1, phase, acceptedAt: stamp, updatedAt: stamp };
}
function running(): JonesOperationReceipt {
  return receipt(
    {
      environmentId,
      operationId: planId,
      expectedInstalledSource: installed,
      action: { kind: "download-exact", targetSource: source },
    },
    "running",
  );
}
async function render(
  next?: JonesUpdateState,
  freshness: JonesUpdateObservation["freshness"] = "fresh",
) {
  if (next !== undefined) harness.observation = { state: next, freshness };
  await act(async () =>
    root.render(<JonesUpdateControls environmentId={environmentId} allowed={allowed} />),
  );
}
function button(label: string) {
  const found = [...container.querySelectorAll("button")].find(
    (entry) => entry.textContent === label,
  );
  if (found === undefined) throw new Error(`Missing button: ${label}`);
  return found;
}
async function click(label: string) {
  await act(async () => button(label).click());
}
async function answer(label: string) {
  const buttons = harness.alert.mock.calls.at(-1)?.[2] as AlertButton[] | undefined;
  const choice = buttons?.find((entry) => entry.text === label);
  if (choice === undefined) throw new Error(`Missing Alert choice: ${label}`);
  await act(async () => choice.onPress?.());
}
function latestState() {
  const current = harness.observation.state;
  if (current === null) throw new Error("Missing synthetic state");
  return current;
}

beforeEach(() => {
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  container = document.createElement("div");
  root = createRoot(container);
  environmentId = EnvironmentId.make(`mobile-install-test-${++environmentSequence}`);
  allowed = true;
  harness.observation = { state: state(), freshness: "fresh" };
  harness.action
    .mockReset()
    .mockImplementation(async () => success({ ...latestState(), phase: "downloading" }));
  harness.operation.mockReset().mockImplementation(async ({ input }) => {
    if (input.action === "operationStart") return success(receipt(input.input));
    if (input.action === "operationRead") return success({ receipts: [] });
    throw new Error("Unexpected synthetic request");
  });
  harness.stop.mockReset();
  harness.alert.mockReset();
});
afterEach(() => {
  act(() => root.unmount());
  container.remove();
});

describe("mobile host update controls", () => {
  const alternativeState = () => {
    const retained = state(true);
    return {
      ...retained,
      capability: { ...retained.capability, replaceStaged: true },
      migrationPlan: { pendingUpstream: [42], pendingJones: [7] },
      alternativeCandidate: {
        provenance: { ...retained.provenance!, sourceSha: "f".repeat(40), artifactId: 99 },
        checkedAt: stamp,
        expiresAt: "2099-01-01T00:00:00Z",
      },
    };
  };

  it("shows separate retained and alternative builds and confirms exact replacement without installing", async () => {
    const retained = alternativeState();
    await render(retained);
    expect(container.textContent).toContain("Downloaded");
    expect(container.textContent).toContain("Alternative");
    expect(container.textContent).toContain("Downloaded migrations: 1 upstream, 1 Jones");
    harness.action.mockImplementation(async () =>
      success({ ...retained, provenance: retained.alternativeCandidate.provenance }),
    );
    await click("Replace download");
    expect(harness.action).not.toHaveBeenCalled();
    expect(harness.alert.mock.calls.at(-1)?.[1]).toContain("aaaaaaa");
    await answer("Replace download");
    expect(harness.action).toHaveBeenCalledTimes(1);
    expect(harness.action.mock.calls[0]?.[0]).toMatchObject({
      environmentId,
      input: {
        action: "downloadExact",
        input: {
          targetSource: "f".repeat(40),
          replace: {
            stagedHandle: "stage",
            sourceSha: source,
            currentVersion: "old",
            expectedInstalledSource: installed,
            environmentId,
            alternative: retained.alternativeCandidate,
            operationId: expect.any(String),
          },
        },
      },
    });
    expect(harness.operation).not.toHaveBeenCalled();
    expect(container.textContent).toContain("Choose Install separately");
  });

  it("cancels or refuses a changed offer after confirmation and retains unknown work across remount", async () => {
    const retained = alternativeState();
    await render(retained);
    await click("Replace download");
    await answer("Cancel");
    expect(harness.action).not.toHaveBeenCalled();
    await click("Replace download");
    harness.observation = {
      state: {
        ...retained,
        alternativeCandidate: {
          ...retained.alternativeCandidate,
          provenance: { ...retained.alternativeCandidate.provenance, artifactId: 100 },
        },
      },
      freshness: "fresh",
    };
    await answer("Replace download");
    expect(harness.action).not.toHaveBeenCalled();
    expect(container.textContent).toContain("Nothing was sent");
    await render(retained);
    harness.action.mockRejectedValue(new Error("Synthetic lost reply"));
    await click("Replace download");
    await answer("Replace download");
    const sent = harness.action.mock.calls[0]?.[0];
    expect(sent.input.input.replace.operationId).toBeDefined();
    await act(async () => root.unmount());
    root = createRoot(container);
    await render(retained);
    expect(button("Replace download").disabled).toBe(true);
    expect(container.textContent).toContain("nothing will be resent");
    await click("Replace download");
    expect(harness.action).toHaveBeenCalledTimes(1);
  });

  it("Download starts only the exact remote download and displays its receipt", async () => {
    await render();
    await click("Download");
    const sent = harness.operation.mock.calls[0]?.[0];
    expect(sent).toMatchObject({
      environmentId,
      input: {
        action: "operationStart",
        input: {
          environmentId,
          expectedInstalledSource: installed,
          action: { kind: "download-exact", targetSource: source },
        },
      },
    });
    expect(harness.operation).toHaveBeenCalledTimes(1);
    expect(harness.action).not.toHaveBeenCalled();
    expect(harness.alert).not.toHaveBeenCalled();
    expect(container.textContent).toContain(sent.input.input.operationId);
    expect(button("Download").disabled).toBe(true);
  });

  it("reviews an install through Alert before sending the exact stage and host binding", async () => {
    await render(state(true));
    await click("Install downloaded build");
    expect(harness.operation).not.toHaveBeenCalled();
    expect(harness.alert.mock.calls[0]?.[1]).toContain("on this host");
    await answer("Install");
    expect(harness.operation).toHaveBeenCalledTimes(1);
    expect(harness.operation.mock.calls[0]?.[0]).toMatchObject({
      environmentId,
      input: {
        action: "operationStart",
        input: {
          environmentId,
          expectedInstalledSource: installed,
          action: {
            kind: "install",
            stagedHandle: "stage",
            currentVersion: "old",
            targetSource: source,
          },
        },
      },
    });
    expect(harness.action).not.toHaveBeenCalled();
  });

  it("cancels install review without sending a mutation", async () => {
    await render(state(true));
    await click("Install downloaded build");
    await answer("Cancel");
    expect(harness.operation).not.toHaveBeenCalled();
    expect(harness.action).not.toHaveBeenCalled();
    expect(container.textContent).toContain("Nothing was sent");
  });

  it.each(["stage", "source", "version", "environment", "stale", "capability", "permission"])(
    "revokes install authority when %s changes during Alert review",
    async (change) => {
      await render(state(true));
      await click("Install downloaded build");
      let changed = latestState();
      if (change === "stage") changed = { ...changed, stagedHandle: "different" };
      if (change === "source")
        changed = { ...changed, provenance: { ...changed.provenance!, sourceSha: tree } };
      if (change === "version") changed = { ...changed, currentVersion: "different" };
      if (change === "environment")
        changed = { ...changed, environmentId: EnvironmentId.make("other-host") };
      if (change === "capability")
        changed = { ...changed, capability: { check: true, download: false, install: false } };
      if (change === "permission") allowed = false;
      await render(changed, change === "stale" ? "stale" : "fresh");
      await answer("Install");
      expect(harness.operation).not.toHaveBeenCalled();
      expect(harness.action).not.toHaveBeenCalled();
      expect(container.textContent).toMatch(/changed|Fresh/);
    },
  );

  it("revokes an open install review when the selected environment changes before its observation catches up", async () => {
    await render(state(true));
    await click("Install downloaded build");
    const previousHost = latestState();
    environmentId = EnvironmentId.make(`${environmentId}-switched`);
    await render(previousHost);
    await answer("Install");
    expect(harness.operation).not.toHaveBeenCalled();
    expect(harness.action).not.toHaveBeenCalled();
    expect(harness.alert).toHaveBeenCalledTimes(1);
  });

  it("revokes an open install review synchronously when the controls unmount", async () => {
    await render(state(true));
    await click("Install downloaded build");
    act(() => root.unmount());
    root = createRoot(container);
    await answer("Install");
    expect(harness.operation).not.toHaveBeenCalled();
    expect(harness.action).not.toHaveBeenCalled();
    expect(harness.alert).toHaveBeenCalledTimes(1);
  });

  it("retains a lost mutation through source equality, revision reset and screen remount; only its exact receipt resolves it", async () => {
    let request: JonesOperationStartInput | undefined;
    let answerReceipt: JonesOperationReceipt | null = null;
    harness.operation.mockImplementation(async ({ input }) => {
      if (input.action === "operationStart") {
        request = input.input;
        throw new Error("lost reply");
      }
      return success({ receipts: [{ operationId: request!.operationId, receipt: answerReceipt }] });
    });
    await render();
    await click("Download");
    const changed = {
      ...latestState(),
      revision: 0,
      installedSource: { ...latestState().installedSource!, sha: source },
    };
    act(() => root.unmount());
    root = createRoot(container);
    await render(changed);
    expect(container.textContent).toContain("Not confirmed");
    expect(container.textContent).toContain(request!.operationId);
    await click("Download");
    await click("Read operation status");
    expect(container.textContent).toContain("Not confirmed");
    answerReceipt = { ...receipt(request!, "stopped"), operationId: planId };
    await click("Read operation status");
    expect(container.textContent).toContain("Not confirmed");
    answerReceipt = receipt(request!, "stopped");
    await click("Read operation status");
    expect(container.textContent).not.toContain("Not confirmed");
    expect(container.textContent).toContain("stopped");
    expect(
      harness.operation.mock.calls.filter(([call]) => call.input.action === "operationStart"),
    ).toHaveLength(1);
    expect(harness.action).not.toHaveBeenCalled();
  });

  it("sends Stop on its own lane while start waits, and never turns requested into stopped or regresses on the late start reply", async () => {
    let finishStart: ((value: unknown) => void) | undefined;
    let request: JonesOperationStartInput | undefined;
    let current: JonesOperationReceipt | undefined;
    harness.operation.mockImplementation(({ input }) => {
      if (input.action === "operationStart") {
        request = input.input;
        return new Promise((resolve) => {
          finishStart = resolve;
        });
      }
      return Promise.resolve(
        success({ receipts: [{ operationId: request!.operationId, receipt: current }] }),
      );
    });
    await render();
    await click("Download");
    current = receipt(request!, "running");
    await render({ ...latestState(), phase: "downloading", operation: current });
    harness.stop.mockImplementation(async ({ input }) => {
      current = {
        ...current!,
        phase: "stop-requested",
        stop: { stopId: input.input.stopId, requestedAt: stamp },
      };
      return success(current);
    });
    await click("Stop download");
    expect(harness.stop.mock.calls[0]?.[0]).toMatchObject({
      environmentId,
      input: {
        action: "operationStop",
        input: {
          environmentId,
          operationId: request!.operationId,
        },
      },
    });
    expect(container.textContent).toContain("Stop requested");
    expect(container.textContent).not.toContain("· stopped");
    current = { ...current!, phase: "stopped", stop: { ...current!.stop!, result: "stopped" } };
    await click("Read operation status");
    await act(async () => finishStart!(success(receipt(request!))));
    expect(container.textContent).toContain("· stopped");
    expect(container.textContent).not.toContain("· accepted");
    expect(harness.stop).toHaveBeenCalledTimes(1);
  });

  it("keeps an unconfirmed Stop held until the exact stop ID is read, without retrying", async () => {
    const current = running();
    await render({ ...state(), operation: current, phase: "downloading" });
    harness.stop.mockRejectedValue(new Error("lost stop reply"));
    await click("Stop download");
    const stopId = harness.stop.mock.calls[0]?.[0].input.input.stopId;
    harness.operation.mockResolvedValue(
      success({
        receipts: [{ operationId: current.operationId, receipt: { ...current, phase: "stopped" } }],
      }),
    );
    await click("Read operation status");
    expect(container.textContent).toContain("Not confirmed");
    harness.operation.mockResolvedValue(
      success({
        receipts: [
          {
            operationId: current.operationId,
            receipt: {
              ...current,
              phase: "stopped",
              stop: { stopId, requestedAt: stamp, result: "stopped" },
            },
          },
        ],
      }),
    );
    await click("Read operation status");
    expect(container.textContent).not.toContain("Not confirmed");
    expect(container.textContent).toContain("· stopped");
    expect(harness.stop).toHaveBeenCalledTimes(1);
  });

  it("shows held and launcher-unknown operations separately, with plan correlation and no membership controls", async () => {
    const current = {
      ...running(),
      phase: "held" as const,
      code: "unreaped",
      correlation: { planId, deviceKey: "host" },
    };
    await render({ ...state(), operation: current });
    expect(container.textContent).toContain("Stop not confirmed · unreaped");
    expect(container.textContent).toContain(current.operationId);
    expect(container.textContent).toContain("Part of the Laptop’s update plan");
    expect(button("Download").disabled).toBe(true);
    expect(button("Stop download").disabled).toBe(true);
    await render({ ...state(), revision: 0 });
    expect(container.textContent).toContain("Stop not confirmed · unreaped");
    await render({
      ...state(),
      operation: { ...current, phase: "handed-off", code: "launcher-unconfirmed" },
    });
    expect(container.textContent).toContain("Not confirmed");
    expect(
      [...container.querySelectorAll("button")].map((entry) => entry.textContent),
    ).not.toContain("Edit");
    expect(harness.operation).not.toHaveBeenCalled();
  });

  it("respects follower policy and only reviews a manual directive target after the server reports it", async () => {
    const follower = {
      ...state(true),
      policy: { role: "follower" as const, leaderSource: installed, leaderTree: tree },
    };
    await render(follower);
    expect(button("Install downloaded build").disabled).toBe(true);
    await click("Install downloaded build");
    expect(harness.alert).not.toHaveBeenCalled();
    expect(harness.operation).not.toHaveBeenCalled();
    await render({
      ...follower,
      policy: { ...follower.policy, reviewTarget: { source, tree, directiveId: planId } },
    });
    expect(button("Install downloaded build").disabled).toBe(false);
    expect(harness.operation).not.toHaveBeenCalled();
    await click("Install downloaded build");
    expect(harness.operation).not.toHaveBeenCalled();
    await answer("Install");
    expect(harness.operation).toHaveBeenCalledTimes(1);
    expect(harness.operation.mock.calls[0]?.[0].input.input.action.kind).toBe("install");
  });

  it("preserves matching legacy offered downloads and separately confirmed staged installs, with no unsupported endpoints", async () => {
    const legacy = { ...state(), capability: { check: true, download: true, install: true } };
    await render(legacy);
    await click("Download");
    expect(harness.action.mock.calls[0]?.[0]).toEqual({
      environmentId,
      input: { action: "download", input: { artifactId: 2, sourceSha: source } },
    });
    expect(harness.operation).not.toHaveBeenCalled();
    expect(container.textContent).not.toContain("Stop download");
    await render({ ...legacy, phase: "staged", stagedHandle: "legacy-stage" });
    harness.action.mockResolvedValue(success({ ...latestState(), phase: "installing" }));
    await click("Install downloaded build");
    expect(harness.action).toHaveBeenCalledTimes(1);
    await answer("Install");
    expect(harness.action.mock.calls[1]?.[0]).toEqual({
      environmentId,
      input: {
        action: "install",
        input: {
          environmentId,
          stagedHandle: "legacy-stage",
          currentVersion: "old",
        },
      },
    });
    expect(harness.operation).not.toHaveBeenCalled();
  });

  it("does not replay a lost legacy request after a fresh revision reset", async () => {
    const legacy = { ...state(), capability: { check: true, download: true, install: true } };
    harness.action.mockRejectedValue(new Error("lost legacy reply"));
    await render(legacy);
    await click("Download");
    await render({ ...legacy, revision: 0 });
    await click("Download");
    expect(container.textContent).toContain("Not confirmed");
    expect(button("Download").disabled).toBe(true);
    expect(harness.action).toHaveBeenCalledTimes(1);
    expect(harness.operation).not.toHaveBeenCalled();
  });

  it.each([
    "qualification-pending",
    "qualification-failed",
    "no-eligible-package-in-window",
    "incomplete-discovery",
    "failure",
  ] as const)("shows additive %s classification while retaining the legacy phase", async (kind) => {
    await render({ ...state(), phase: "no-new", checkClassification: { kind, checkedAt: stamp } });
    const text: Record<typeof kind, string> = {
      "qualification-pending": "qualification is still pending",
      "qualification-failed": "qualification failed",
      "no-eligible-package-in-window": "in this search window",
      "incomplete-discovery": "Discovery was incomplete",
      failure: "update check failed",
    };
    expect(container.textContent).toContain(text[kind]);
    expect(latestState().phase).toBe("no-new");
    expect(harness.operation).not.toHaveBeenCalled();
  });

  it("revokes controls on freshness and capability downgrade without using the prior receipt as authority", async () => {
    await render();
    expect(button("Download").disabled).toBe(false);
    await render(latestState(), "stale");
    expect(button("Download").disabled).toBe(true);
    await render({
      ...latestState(),
      capability: { check: false, download: false, install: false },
    });
    expect(button("Download").disabled).toBe(true);
    expect(container.textContent).not.toContain("Stop download");
    await click("Download");
    expect(harness.operation).not.toHaveBeenCalled();
    expect(harness.action).not.toHaveBeenCalled();
  });

  it("does not infer recovery support from new client types, and first-install preflight claims no existing backup", async () => {
    await render(state(true));
    await click("Install downloaded build");
    await answer("Cancel");
    expect(harness.operation).not.toHaveBeenCalled();
    await render({ ...state(true), capability: { ...state().capability, recoveryPolicy: 1 } });
    harness.operation.mockResolvedValue(
      success({
        protocol: 1,
        preflightId: planId,
        measuredAt: stamp,
        expiresAt: "2099-10-27T05:00:00.000Z",
        binding: {
          environmentId,
          stagedHandle: "stage",
          currentVersion: "old",
          expectedInstalledSource: installed,
        },
        firstInstall: true,
        stateBytes: 0,
        volumes: [],
        clone: { status: "unknown" },
        policies: { cloneOrCopy: { admissible: true }, cloneRequired: { admissible: false } },
      } satisfies JonesRecoveryPreflight),
    );
    await click("Install downloaded build");
    await answer("Install");
    expect(harness.alert.mock.calls.at(-1)?.[1]).toContain("no existing state backup is claimed");
    await answer("Stop");
    expect(
      harness.operation.mock.calls.filter(([call]) => call.input.action === "operationStart"),
    ).toHaveLength(0);
  });

  it.each([true, false])(
    "offers Clone or Stop only from negotiated host preflight proof (clone proved: %s)",
    async (proved) => {
      await render({ ...state(true), capability: { ...state().capability, recoveryPolicy: 1 } });
      const preflight: JonesRecoveryPreflight = {
        protocol: 1,
        preflightId: planId,
        measuredAt: stamp,
        expiresAt: "2099-10-27T05:00:00.000Z",
        binding: {
          environmentId,
          stagedHandle: "stage",
          currentVersion: "old",
          expectedInstalledSource: installed,
        },
        firstInstall: false,
        stateBytes: 100,
        volumes: [],
        clone: { status: proved ? "proved" : "unknown" },
        policies: {
          cloneOrCopy: { admissible: false, reason: "Not enough host space for a copy." },
          cloneRequired: { admissible: proved },
        },
      };
      harness.operation.mockImplementation(async ({ input }) =>
        input.action === "recoveryPreflight" ? success(preflight) : success(receipt(input.input)),
      );
      await click("Install downloaded build");
      await answer("Install");
      expect(harness.operation.mock.calls[0]?.[0]).toMatchObject({
        environmentId,
        input: { action: "recoveryPreflight", input: preflight.binding },
      });
      const choices = harness.alert.mock.calls.at(-1)?.[2] as AlertButton[];
      expect(choices.map((entry) => entry.text)).toEqual(proved ? ["Stop", "Clone"] : ["Stop"]);
      await answer(proved ? "Clone" : "Stop");
      const starts = harness.operation.mock.calls.filter(
        ([call]) => call.input.action === "operationStart",
      );
      expect(starts).toHaveLength(proved ? 1 : 0);
      if (proved)
        expect(starts[0]?.[0].input.input.action.recovery).toEqual({
          policy: "clone-required",
          preflightId: planId,
        });
      expect(harness.action).not.toHaveBeenCalled();
    },
  );

  it("sends the reviewed clone-or-copy policy only after the host recovery confirmation", async () => {
    await render({ ...state(true), capability: { ...state().capability, recoveryPolicy: 1 } });
    const preflight: JonesRecoveryPreflight = {
      protocol: 1,
      preflightId: planId,
      measuredAt: stamp,
      expiresAt: "2099-10-27T05:00:00.000Z",
      binding: {
        environmentId,
        stagedHandle: "stage",
        currentVersion: "old",
        expectedInstalledSource: installed,
      },
      firstInstall: false,
      stateBytes: 100,
      volumes: [],
      clone: { status: "unknown" },
      policies: { cloneOrCopy: { admissible: true }, cloneRequired: { admissible: false } },
    };
    harness.operation.mockImplementation(async ({ input }) =>
      input.action === "recoveryPreflight" ? success(preflight) : success(receipt(input.input)),
    );
    await click("Install downloaded build");
    await answer("Install");
    expect(harness.operation).toHaveBeenCalledTimes(1);
    expect(harness.operation.mock.calls[0]?.[0]).toMatchObject({
      environmentId,
      input: { action: "recoveryPreflight", input: preflight.binding },
    });
    const choices = harness.alert.mock.calls.at(-1)?.[2] as AlertButton[];
    expect(choices.map((entry) => entry.text)).toEqual(["Stop", "Continue"]);
    await answer("Continue");
    expect(harness.operation).toHaveBeenCalledTimes(2);
    expect(harness.operation.mock.calls[1]?.[0]).toMatchObject({
      environmentId,
      input: {
        action: "operationStart",
        input: {
          environmentId,
          expectedInstalledSource: installed,
          action: {
            kind: "install",
            stagedHandle: "stage",
            currentVersion: "old",
            targetSource: source,
            recovery: { policy: "clone-or-copy", preflightId: planId },
          },
        },
      },
    });
    expect(harness.action).not.toHaveBeenCalled();
  });
});
