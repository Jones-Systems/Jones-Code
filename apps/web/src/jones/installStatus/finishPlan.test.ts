import { afterEach, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesInstallPlanState,
  JonesInstallPlanRequest,
  JonesInstallPlanResult,
} from "@t3tools/contracts/jones/installPlan";
import type { JonesOperationReceipt } from "@t3tools/contracts/jones/jonesOperations";
import { isPlanComplete, startPlan } from "@t3tools/client-runtime/jones/install-plan";
import { createInstallStatusStore, type InstallStatusDevice } from "./panelUi";
import { bindInstallPlanServices, type InstallPlanServices } from "./services";
import { finishPlan } from "./finishPlan";

const at = "2026-11-01T12:00:00Z";
const sha = "a".repeat(40),
  tree = "b".repeat(40);
const ids = [EnvironmentId.make("mini"), EnvironmentId.make("vps")];
let release: (() => void) | undefined;
afterEach(() => release?.());

function setup() {
  const base = startPlan({
    planId: "11111111-1111-4111-8111-111111111111",
    origin: "update-button",
    now: at,
    target: {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: sha,
      sourceTree: tree,
      version: "new",
    },
    from: { version: "old" },
    laptopEnvironmentId: EnvironmentId.make("laptop"),
    select: ids.map((environmentId) => ({
      environmentId,
      label: environmentId,
      route: "operations",
    })),
  });
  let native: JonesInstallPlanState = {
    schema: 1,
    last: null,
    laptop: { observedAt: at, activationBlocked: false },
    view: { complete: false },
    current: {
      ...base,
      revision: 7,
      laptop: {
        ...base.laptop,
        gate: {
          openedAt: at,
          transactionId: "tx",
          activeGeneration: "gen",
          basis: "plan-install",
          activeSource: sha,
          activeTree: tree,
        },
      },
      devices: base.devices.map((d, i) => {
        const receipt: JonesOperationReceipt = {
          protocol: 1,
          operationId: `${i + 2}1111111-1111-4111-8111-111111111111`,
          environmentId: d.environmentId,
          action: { kind: "download-exact", targetSource: sha },
          expectedInstalledSource: "c".repeat(40),
          phase: "completed",
          acceptedAt: at,
          updatedAt: at,
          staged: { stagedHandle: `stage-${i}`, sourceSha: sha, sourceTree: tree, artifactId: 1 },
        };
        return {
          ...d,
          operations: [
            {
              operationId: receipt.operationId,
              step: "download",
              lease: { claimant: "test", reservedAt: at, leaseUntil: at },
              request: { kind: "operation-start", input: receipt },
              receipt,
            },
          ],
        };
      }),
    },
  };
  let devices: readonly InstallStatusDevice[] = ids.map((environmentId) => ({
    environmentId,
    label: environmentId,
    canOperate: false,
    observation: {
      freshness: "fresh",
      state: {
        source: "jones-actions",
        channel: "jones-main",
        phase: "staged",
        environmentId,
        capability: { check: true, download: false, install: false },
        installedSource: { repository: "Jones-Systems/Jones-Code", sha, tree },
      },
    },
  }));
  const store = createInstallStatusStore();
  store.update({ native, devices, open: true });
  const calls: JonesInstallPlanRequest[] = [];
  let intercept:
    | ((request: JonesInstallPlanRequest) => JonesInstallPlanResult | undefined)
    | undefined;
  const installPlan = vi.fn(
    async (request: JonesInstallPlanRequest): Promise<JonesInstallPlanResult> => {
      calls.push(request);
      const response = intercept?.(request);
      if (response !== undefined) return response;
      const plan = native.current!;
      if (request.type === "end" || request.type === "dismiss") {
        if (request.expectedRevision !== plan.revision)
          return {
            state: native,
            refused: { code: "stale-revision", message: "Competing claim changed revision." },
          };
        if (request.type === "end")
          native = {
            ...native,
            current: {
              ...plan,
              revision: plan.revision + 1,
              devices: plan.devices.map((d) =>
                d.environmentId === request.environmentId ? { ...d, ended: { at } } : d,
              ),
            },
          };
        else {
          if (!isPlanComplete(plan)) throw new Error("Unsettled dismissal");
          native = {
            ...native,
            current: null,
            last: { ...plan, revision: plan.revision + 1, dismissedAt: at },
          };
        }
      }
      return { state: native };
    },
  );
  const services: InstallPlanServices = {
    bridge: { installPlan, getUpdateState: vi.fn(), onUpdateState: vi.fn() },
    devices: () => devices,
    observe: vi.fn(),
    operation: vi.fn(),
    legacy: vi.fn(),
    stop: vi.fn(),
    host: vi.fn(),
    confirm: vi.fn(),
    chooseRecovery: vi.fn(),
    now: () => 0,
  };
  release = bindInstallPlanServices(services, store);
  return {
    planId: base.planId,
    store,
    calls,
    services,
    get native() {
      return native;
    },
    set native(value) {
      native = value;
      store.update({ native });
    },
    intercept: (fn: NonNullable<typeof intercept>) => {
      intercept = fn;
    },
    stale: (index = 0) => {
      devices = devices.map((d, i) =>
        i === index
          ? { ...d, observation: { ...d.observation, freshness: "stale", heardAt: 0 } }
          : d,
      );
    },
  };
}

it("Done ends multiple proven stages using each successful response revision, then dismisses", async () => {
  const world = setup();
  await finishPlan(world.planId);
  expect(
    world.calls.map((r) => [r.type, "expectedRevision" in r ? r.expectedRevision : null]),
  ).toEqual([
    ["read", null],
    ["end", 7],
    ["end", 8],
    ["dismiss", 9],
  ]);
  expect(world.native.current).toBeNull();
  expect(world.store.get()).toMatchObject({ open: false, showLast: true });
});

it("ordinary ledger-complete Done needs no fresh proof", async () => {
  const world = setup();
  world.native = {
    ...world.native,
    current: {
      ...world.native.current!,
      devices: world.native.current!.devices.map((d) => ({ ...d, ended: { at } })),
    },
  };
  world.stale();
  await finishPlan(world.planId);
  expect(world.calls.map((r) => r.type)).toEqual(["read", "dismiss"]);
});

it.each(["stale-revision", "outstanding-operation"] as const)(
  "stops on %s after a successful end, preserving partial success",
  async (code) => {
    const world = setup();
    let ends = 0;
    world.intercept((r) =>
      r.type === "end" && ++ends === 2
        ? { state: world.native, refused: { code, message: "Refused next end" } }
        : undefined,
    );
    await expect(finishPlan(world.planId)).rejects.toThrow("Refused next end");
    expect(world.native.current?.devices.map((d) => d.ended !== undefined)).toEqual([true, false]);
    expect(world.calls.map((r) => r.type)).toEqual(["read", "end", "end"]);
    expect(world.store.get().open).toBe(true);
  },
);

it("a competing claim refuses the clicked revision without a retry", async () => {
  const world = setup();
  world.intercept((r) => {
    if (r.type === "end")
      world.native = { ...world.native, current: { ...world.native.current!, revision: 8 } };
    return undefined;
  });
  await expect(finishPlan(world.planId)).rejects.toThrow("Competing claim");
  expect(world.calls.map((r) => r.type)).toEqual(["read", "end"]);
});

it("a lost response stops; the next explicit click reads and keeps the accepted end", async () => {
  const world = setup();
  let lost = false;
  world.intercept((r) => {
    if (r.type === "end" && !lost) {
      lost = true;
      world.native = {
        ...world.native,
        current: {
          ...world.native.current!,
          revision: 8,
          devices: world.native.current!.devices.map((d) =>
            d.environmentId === ids[0] ? { ...d, ended: { at } } : d,
          ),
        },
      };
      throw new Error("Response lost");
    }
    return undefined;
  });
  await expect(finishPlan(world.planId)).rejects.toThrow("Response lost");
  expect(world.calls.map((r) => r.type)).toEqual(["read", "end"]);
  expect(world.store.get().open).toBe(true);
  await finishPlan(world.planId);
  expect(world.calls.map((r) => r.type)).toEqual(["read", "end", "read", "end", "dismiss"]);
  expect(world.calls.filter((r) => r.type === "end" && r.environmentId === ids[0])).toHaveLength(1);
});

it("stale proof at click time cannot end or dismiss a stage", async () => {
  const world = setup();
  world.stale();
  await expect(finishPlan(world.planId)).rejects.toThrow("Fresh proof");
  expect(world.calls.map((r) => r.type)).toEqual(["read"]);
});

it("rechecks the next candidate's proof after the previous response", async () => {
  const world = setup();
  world.intercept((r) => {
    if (r.type === "end") world.stale(1);
    return undefined;
  });
  await expect(finishPlan(world.planId)).rejects.toThrow("proof changed");
  expect(world.calls.map((r) => r.type)).toEqual(["read", "end"]);
  expect(world.native.current?.devices[0]?.ended).toBeDefined();
});

it.each(["identity", "read-only", "connection"])(
  "stops when the initial read changes %s",
  async (change) => {
    const world = setup();
    world.intercept((r) => {
      if (r.type === "read") {
        if (change === "identity")
          world.native = {
            ...world.native,
            current: { ...world.native.current!, planId: "other" },
          };
        if (change === "read-only")
          world.native = {
            ...world.native,
            readOnly: { code: "newer-schema", message: "Read only" },
          };
        if (change === "connection") {
          release?.();
          release = bindInstallPlanServices(world.services, world.store);
        }
      }
      return undefined;
    });
    await expect(finishPlan(world.planId)).rejects.toThrow();
    expect(world.calls.map((r) => r.type)).toEqual(["read"]);
  },
);

it("does not close on an unconfirmed dismissal", async () => {
  const world = setup();
  world.intercept((r) => (r.type === "dismiss" ? { state: world.native } : undefined));
  await expect(finishPlan(world.planId)).rejects.toThrow("dismissal was not confirmed");
  expect(world.store.get().open).toBe(true);
});
