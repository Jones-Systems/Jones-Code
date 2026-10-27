import { act, render, stubGlobal, type Screen } from "../../../test/render";
import { describe, expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesUpdateObservation,
  JonesOperationCommand,
} from "@t3tools/client-runtime/jones/updates";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import type {
  JonesOperationReceipt,
  JonesOperationStartInput,
  JonesRecoveryPolicy,
  JonesRecoveryPreflight,
} from "@t3tools/contracts/jones/jonesOperations";
import "../../index.css";

const h = vi.hoisted(() => ({
  observations: new Map<string, JonesUpdateObservation>(),
  permissions: new Map<string, boolean>(),
  confirmations: [] as Array<(value: boolean) => void>,
  recoveries: [] as Array<(policy: JonesRecoveryPolicy | null) => void>,
  disk: new Map<string, string>(),
  sequence: 1,
  lock: Promise.resolve(),
  legacy: vi.fn(),
  operation: vi.fn(),
  stop: vi.fn(),
  native: vi.fn(),
}));
vi.mock("@t3tools/client-runtime/jones/updates/action-journal", async (original) => {
  const actual =
    await original<typeof import("@t3tools/client-runtime/jones/updates/action-journal")>();
  return {
    ...actual,
    createJonesActionJournal: () =>
      actual.createJonesActionJournal({
        getItem: (key) => h.disk.get(key) ?? null,
        setItem: (key, value) => {
          h.disk.set(key, value);
        },
        withLock: (work) => {
          const next = h.lock.then(work);
          h.lock = next.then(
            () => undefined,
            () => undefined,
          );
          return next;
        },
      }),
  };
});
vi.mock("./jonesUpdates", () => ({
  jonesUpdates: {
    observation: (env: string) => ({ kind: "observation", env }),
    action: "action",
    operation: "operation",
    stop: "stop",
  },
}));
vi.mock("../../state/session", () => ({
  environmentSession: { sessionStateAtom: (env: string) => ({ kind: "session", env }) },
}));
vi.mock("@effect/atom-react", async (original) => {
  const actual = await original<typeof import("@effect/atom-react")>();
  const { AsyncResult } = await import("effect/reactivity");
  return {
    ...actual,
    useAtomRefresh: () => () => {},
    useAtomValue: (atom: { kind?: string; env?: string }) =>
      atom.kind === "observation"
        ? h.observations.get(atom.env!)
        : atom.kind === "session"
          ? AsyncResult.success({
              authenticated: true,
              permissions: h.permissions.get(atom.env!) ? ["orchestration:operate"] : [],
            })
          : AsyncResult.success(undefined),
  };
});
vi.mock("../../rpc/atomRegistry", () => ({
  appAtomRegistry: {
    get: (atom: { kind: string; env: string }) =>
      atom.kind === "observation"
        ? h.observations.get(atom.env)
        : {
            _tag: "Success",
            value: {
              authenticated: true,
              permissions: h.permissions.get(atom.env) ? ["orchestration:operate"] : [],
            },
          },
  },
}));
vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: (command: string) => (command === "stop" ? h.stop : h.legacy),
}));
vi.mock("@t3tools/client-runtime/state/runtime", async (original) => ({
  ...(await original<typeof import("@t3tools/client-runtime/state/runtime")>()),
  runAtomCommand: (_registry: unknown, _atom: unknown, input: unknown) => h.operation(input),
}));
vi.mock("../../confirmDialog", () => ({
  requestConfirmDialog: () =>
    new Promise<boolean>((resolve) => {
      h.confirmations.push(resolve);
    }),
}));
vi.mock("../installStatus/recoveryReview", () => ({
  chooseRecoveryPolicy: () =>
    new Promise<JonesRecoveryPolicy | null>((resolve) => {
      h.recoveries.push(resolve);
    }),
}));
// These controls do not request a laptop comparison. Avoid activating the real catalog
// behind that unused fixture input; controls, executor and native gates remain real.
vi.mock("../../connection/runtime", async () => {
  const { Atom } = await import("effect/reactivity");
  const Layer = await import("effect/Layer");
  return { connectionAtomRuntime: Atom.runtime(Layer.empty) };
});
vi.mock("../../state/primaryEnvironment", async () => {
  const { Atom } = await import("effect/reactivity");
  return { primaryEnvironmentIdAtom: Atom.make(null) };
});
vi.mock("../../env", async (original) => ({
  ...(await original<typeof import("../../env")>()),
  isElectron: false,
}));
vi.mock("../../lib/utils", async (original) => ({
  ...(await original<typeof import("../../lib/utils")>()),
  randomUUID: () => `${String(h.sequence++).padStart(8, "0")}-1111-4111-8111-111111111111`,
}));
import { JonesUpdateControls } from "./JonesUpdateControls";
import { ProductionExactSourceUpdateAction } from "./ProductionExactSourceUpdateAction";

import { webJonesJournal } from "../installStatus/executionJournal";
import { bindInstallPlanServices, type InstallPlanServices } from "../installStatus/services";
import { createInstallStatusStore } from "../installStatus/panelUi";

let root: Screen, container: HTMLElement;
let count = 0;
const source = "a".repeat(40),
  old = "b".repeat(40);
const success = (value: unknown) => ({ _tag: "Success", value });
function state(environmentId: EnvironmentId, modern: boolean, recovery = false): JonesUpdateState {
  return {
    source: "jones-actions",
    channel: "jones-main",
    environmentId,
    phase: "staged",
    currentVersion: "old",
    stagedHandle: "stage",
    capability: {
      check: true,
      download: true,
      install: true,
      ...(modern ? { operations: 1 } : {}),
      ...(recovery ? { recoveryPolicy: 1 } : {}),
    },
    ...(modern
      ? { installedSource: { repository: "Jones-Systems/Jones-Code", sha: old, tree: old } }
      : {}),
    provenance: {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: source,
      sourceTree: source,
      workflow: "synthetic",
      runId: 1,
      runAttempt: 1,
      artifactId: 1,
      artifactDigest: "c".repeat(64),
      platform: "linux",
      architecture: "x64",
    },
  };
}
const draw = (control: "connections" | "exact", env: EnvironmentId, targetSource = source) =>
  root.rerender(
    control === "connections" ? (
      <JonesUpdateControls environmentId={env} />
    ) : (
      <ProductionExactSourceUpdateAction
        environmentId={env}
        label="host"
        targetSource={targetSource}
      />
    ),
  );
async function click(label: string) {
  const button = [...container.querySelectorAll("button")].find(
    (node) => node.textContent === label,
  );
  expect(button, container.textContent ?? "").toBeDefined();
  await act(async () => button!.click());
}
function hosts(modern = true, recovery = false) {
  const a = EnvironmentId.make(`production-review-a-${++count}`),
    b = EnvironmentId.make(`production-review-b-${count}`);
  for (const env of [a, b]) {
    h.observations.set(env, { state: state(env, modern, recovery), freshness: "fresh" });
    h.permissions.set(env, true);
  }
  return { a, b };
}
async function fixture() {
  const stack = new AsyncDisposableStack();
  stack.use(stubGlobal("desktopBridge", undefined));
  root = stack.use(await render(null, { router: false }));
  container = root.container;
  await act(() => webJonesJournal.load());
  h.confirmations.length = 0;
  h.recoveries.length = 0;
  h.native.mockReset();
  h.stop.mockReset();
  h.legacy.mockReset().mockImplementation(async ({ environmentId }) => {
    const next = { ...h.observations.get(environmentId)!.state!, phase: "installing" as const };
    h.observations.set(environmentId, { state: next, freshness: "fresh" });
    return success(next);
  });
  h.operation
    .mockReset()
    .mockImplementation(async ({ input }: { input: JonesOperationCommand }) => {
      if (input.action === "recoveryPreflight") {
        const at = Date.now();
        return success({
          protocol: 1,
          preflightId: "99999999-1111-4111-8111-111111111111",
          binding: input.input,
          measuredAt: new Date(at).toISOString(),
          expiresAt: new Date(at + 300_000).toISOString(),
          firstInstall: false,
          stateBytes: 5,
          volumes: [
            { role: "state", availableBytes: 4, copyRequiredBytes: 9, cloneRequiredBytes: 4 },
          ],
          clone: { status: "proved" },
          policies: { cloneOrCopy: { admissible: false }, cloneRequired: { admissible: true } },
        } satisfies JonesRecoveryPreflight);
      }
      if (input.action === "operationStart")
        return success({
          protocol: 1,
          ...input.input,
          phase: "running",
          acceptedAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        });
      return success({ receipts: [] });
    });
  stack.defer(async () => {
    await h.lock;
  });
  return stack;
}

function nativeFixture(environmentId: EnvironmentId): Disposable {
  h.native.mockResolvedValue({
    state: {
      schema: 1,
      current: null,
      last: null,
      clientCommands: 1,
      laptop: { environmentId, observedAt: new Date().toISOString(), activationBlocked: false },
      view: { complete: false },
    },
  });
  const unavailable = async (): Promise<never> => {
    throw new Error("Unexpected fixture port call");
  };
  const services: InstallPlanServices = {
    bridge: {
      installPlan: h.native,
      onInstallPlan: () => () => {},
      getUpdateState: unavailable,
      onUpdateState: () => () => {},
    },
    devices: () => [],
    observe: unavailable,
    legacy: unavailable,
    operation: unavailable,
    stop: unavailable,
    host: unavailable,
    confirm: async () => true,
    chooseRecovery: async () => null,
    now: Date.now,
  };
  const release = bindInstallPlanServices(services, createInstallStatusStore());
  const bridge = stubGlobal("desktopBridge", services.bridge);
  return {
    [Symbol.dispose]() {
      release();
      bridge[Symbol.dispose]();
    },
  };
}

describe("production web review authority", () => {
  it("revokes an exact-source control when its target changes within the same fresh environment", async () => {
    await using _fixture = await fixture();
    const { a } = hosts();
    await draw("exact", a);
    await click("Install aaaaaaa");
    await draw("exact", a, old);
    await act(async () => h.confirmations[0]!(true));
    expect(h.operation).not.toHaveBeenCalled();
    expect(h.legacy).not.toHaveBeenCalled();
    expect(h.native).not.toHaveBeenCalled();
  });
  it.each(
    (["connections", "exact"] as const).flatMap((control) =>
      [true, false].flatMap((modern) =>
        (["retarget", "unmount", "permission"] as const).map((transition) => ({
          control,
          modern,
          transition,
        })),
      ),
    ),
  )(
    "$control modern=$modern: revokes $transition while A remains fresh",
    async ({ control, modern, transition }) => {
      await using _fixture = await fixture();
      const { a, b } = hosts(modern);
      using _native = nativeFixture(a);
      await draw(control, a);
      await click(control === "connections" ? "Install" : "Install aaaaaaa");
      expect(h.confirmations).toHaveLength(1);
      if (transition === "retarget") await draw(control, b);
      else if (transition === "unmount") await root.rerender(null);
      else {
        h.permissions.set(a, false);
        await draw(control, a);
      }
      expect(h.observations.get(a)?.freshness).toBe("fresh");
      await act(async () => h.confirmations[0]!(true));
      expect(h.legacy).not.toHaveBeenCalled();
      expect(h.operation).not.toHaveBeenCalled();
      expect(h.native).not.toHaveBeenCalled();
    },
  );
  it.each(
    (["connections", "exact"] as const).flatMap((control) =>
      [true, false].map((modern) => ({ control, modern })),
    ),
  )("$control modern=$modern: unchanged review sends once", async ({ control, modern }) => {
    await using _fixture = await fixture();
    const { a } = hosts(modern);
    await draw(control, a);
    await click(control === "connections" ? "Install" : "Install aaaaaaa");
    await draw(control, a);
    await act(async () => h.confirmations[0]!(true));
    if (modern) expect(h.operation).toHaveBeenCalledTimes(1);
    else expect(h.legacy).toHaveBeenCalledTimes(1);
    const button = [...container.querySelectorAll("button")].find((node) =>
      node.textContent?.startsWith("Install"),
    );
    if (button !== undefined)
      await act(async () => {
        button.click();
        button.focus();
      });
    await userEvent.keyboard("{Enter} ");
    expect(h.confirmations).toHaveLength(1);
  });
  it.each(
    (["connections", "exact"] as const).flatMap((control) =>
      (["retarget", "unmount", "permission"] as const).map((transition) => ({
        control,
        transition,
      })),
    ),
  )("$control: revokes pending recovery on $transition", async ({ control, transition }) => {
    await using _fixture = await fixture();
    const { a, b } = hosts(true, true);
    await draw(control, a);
    await click(control === "connections" ? "Install" : "Install aaaaaaa");
    await act(async () => h.confirmations[0]!(true));
    expect(h.recoveries).toHaveLength(1);
    if (transition === "retarget") await draw(control, b);
    else if (transition === "unmount") await root.rerender(null);
    else {
      h.permissions.set(a, false);
      await draw(control, a);
    }
    await act(async () => h.recoveries[0]!("clone-required"));
    expect(
      h.operation.mock.calls.filter(([request]) => request.input.action === "operationStart"),
    ).toEqual([]);
    expect(h.legacy).not.toHaveBeenCalled();
    expect(h.native).not.toHaveBeenCalled();
  });

  it("keeps one Stop through pointer/keyboard, remount, wrong proofs and a late start response", async () => {
    await using _fixture = await fixture();
    const { a } = hosts();
    const { stagedHandle: _stage, ...unstaged } = state(a, true);
    const initial = { ...unstaged, phase: "available" as const };
    h.observations.set(a, { state: initial, freshness: "fresh" });
    let start!: JonesOperationStartInput, finish!: (value: unknown) => void;
    h.operation.mockImplementation(async ({ input }: { input: JonesOperationCommand }) => {
      if (input.action === "operationStart") {
        start = input.input;
        return new Promise((resolve) => {
          finish = resolve;
        });
      }
      return success({ receipts: [] });
    });
    await draw("connections", a);
    await click("Download");
    const running: JonesOperationReceipt = {
      protocol: 1,
      ...start,
      phase: "running",
      acceptedAt: "2026-10-27T04:00:00Z",
      updatedAt: "2026-10-27T04:00:00Z",
    };
    h.observations.set(a, { state: { ...initial, operation: running }, freshness: "fresh" });
    await draw("connections", a);
    h.stop.mockResolvedValue({ _tag: "Failure" });
    await click("Stop download");
    await click("Stop download");
    const stopId = h.stop.mock.calls[0]![0].input.input.stopId as string;
    await root.rerender(null);
    await draw("connections", a);
    await click("Stop download");
    await userEvent.keyboard("{Enter} ");
    expect(h.stop).toHaveBeenCalledTimes(1);
    let answer: JonesOperationReceipt = { ...running, phase: "stopped" };
    h.operation.mockImplementation(async () =>
      success({ receipts: [{ operationId: running.operationId, receipt: answer }] }),
    );
    await click("Read operation status");
    expect(container.textContent).toContain("Read operation status");
    answer = {
      ...answer,
      stop: {
        stopId: "88888888-1111-4111-8111-111111111111",
        requestedAt: running.updatedAt,
        result: "stopped",
      },
    };
    await click("Read operation status");
    expect(container.textContent).toContain("Read operation status");
    answer = { ...answer, stop: { ...answer.stop!, stopId } };
    await click("Read operation status");
    expect(webJonesJournal.getSnapshot().duties.filter((d) => d.environmentId === a)).toEqual([]);
    const stopped = () =>
      webJonesJournal.getSnapshot().history.find((d) => d.environmentId === a && d.kind === "stop")
        ?.receipt;
    expect(stopped()).toMatchObject({ phase: "stopped", stop: { stopId, result: "stopped" } });
    await act(async () => finish(success(running)));
    expect(stopped()).toMatchObject({ phase: "stopped", stop: { stopId, result: "stopped" } });
    expect(container.textContent).not.toContain("Read operation status");
    expect(container.textContent).toContain("stopped");
    expect(h.stop).toHaveBeenCalledTimes(1);
  });
});
