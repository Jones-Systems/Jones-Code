import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import {
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";
import { collectProviderUpdateCandidates } from "@t3tools/client-runtime/jones/provider-updates";
import { getProviderUpdateRunOutcome } from "~/components/ProviderUpdateLaunchNotification.logic";
import {
  createProviderUpdateController,
  providerUpdatePresentationAttempt,
  type ProviderUpdateMachine,
} from "./providerUpdateController";

const environmentId = EnvironmentId.make("provider-controller");
function provider(
  id = "personal",
  status?: "succeeded" | "unchanged" | "failed" | "running",
): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(id),
    driver: ProviderDriverKind.make("codex"),
    enabled: true,
    installed: true,
    version: status === "succeeded" ? "2.0.0" : "1.0.0",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-10-26T01:00:00.000Z",
    models: [],
    skills: [],
    slashCommands: [],
    versionAdvisory: {
      status: "behind_latest",
      currentVersion: "1.0.0",
      latestVersion: "2.0.0",
      updateCommand: "npm install -g @openai/codex@latest",
      canUpdate: true,
      checkedAt: "2026-10-26T01:00:00.000Z",
      message: "Update available.",
    },
    ...(status
      ? {
          updateState: {
            status,
            startedAt: "2026-10-26T01:00:00.000Z",
            finishedAt: status === "running" ? null : "2026-10-26T01:01:00.000Z",
            message: status === "unchanged" ? "Installed version could not be verified." : status,
            output: null,
          },
        }
      : {}),
  };
}
const machine = (providers = [provider()]): ProviderUpdateMachine => ({
  environmentId,
  label: "WSL",
  candidates: collectProviderUpdateCandidates(providers),
});
const success = (providers = [provider("personal", "succeeded")]) =>
  AsyncResult.success({ providers });
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
const flush = async () => {
  for (let i = 0; i < 8; i++) await Promise.resolve();
};
afterEach(() => vi.useRealTimers());

describe("provider update controller", () => {
  it("keeps captured active targets and only supersedes terminal feedback for a new semantic basis", async () => {
    let providers = [provider()];
    const controller = createProviderUpdateController({
      readCurrent: () => ({ connected: true, permitted: true, providers }),
    });
    const remaining = deferred<ReturnType<typeof success>>();
    const pending = controller.start(machine(providers), () => remaining.promise);
    const active = controller.getSnapshot().get(environmentId)!;
    providers = [provider("work")];
    expect(providerUpdatePresentationAttempt(active, machine(providers).candidates)).toBe(active);
    remaining.resolve(success());
    await pending;
    const terminal = controller.getSnapshot().get(environmentId)!;
    expect(providerUpdatePresentationAttempt(terminal, [])).toBe(terminal);
    expect(
      providerUpdatePresentationAttempt(
        terminal,
        machine([{ ...provider(), checkedAt: "2026-10-26T02:00:00.000Z" }]).candidates,
      ),
    ).toBe(terminal);
    expect(
      providerUpdatePresentationAttempt(terminal, machine(providers).candidates),
    ).toBeUndefined();
    const newer = {
      ...provider(),
      versionAdvisory: { ...provider().versionAdvisory!, latestVersion: "3.0.0" },
    };
    expect(
      providerUpdatePresentationAttempt(terminal, machine([newer]).candidates),
    ).toBeUndefined();
    const dispatch = vi.fn(async () => success([provider("work", "succeeded")]));
    await controller.start(machine(providers), dispatch);
    expect(dispatch).toHaveBeenCalledWith({
      environmentId,
      input: { provider: "codex", instanceId: "work" },
    });
    expect(controller.getSnapshot().get(environmentId)?.generation).toBe(2);
  });

  it.each(["connection", "permission", "eligibility", "running"])(
    "rechecks %s after offering, including the primary environment",
    async (lost) => {
      let current = { connected: true, permitted: true, providers: [provider()] };
      const controller = createProviderUpdateController({ readCurrent: () => current });
      const offered = machine();
      current = {
        connected: lost !== "connection",
        permitted: lost !== "permission",
        providers:
          lost === "eligibility"
            ? [{ ...provider(), enabled: false }]
            : lost === "running"
              ? [provider("personal", "running")]
              : [provider()],
      };
      const update = vi.fn(async () => success());
      const runs = await controller.start(offered, update);
      expect(update).not.toHaveBeenCalled();
      expect(runs).toHaveLength(1);
      expect(getProviderUpdateRunOutcome(runs[0]!)).toMatchObject({ status: "failed" });
      expect(controller.getSnapshot().get(environmentId)?.phase).toBe("failed");
    },
  );

  it("selects the freshest duplicate before eligibility without suppressing an eligible sibling", async () => {
    const old = provider();
    const newest = { ...old, checkedAt: "2026-10-26T01:02:00.000Z", enabled: false };
    const sibling = provider("work");
    const controller = createProviderUpdateController({
      readCurrent: () => ({ connected: true, permitted: true, providers: [newest, sibling, old] }),
    });
    const update = vi.fn(async (_input: { input: { instanceId: string } }) =>
      success([provider("work", "succeeded")]),
    );
    const runs = await controller.start(machine([old, sibling]), update);
    expect(update.mock.calls.map(([input]) => input.input.instanceId)).toEqual(["work"]);
    expect(runs.map(getProviderUpdateRunOutcome).map((outcome) => outcome.status)).toEqual([
      "failed",
      "updated",
    ]);
    expect(controller.getSnapshot().get(environmentId)?.phase).toBe("failed");
  });

  it("coalesces duplicate clicks and reports partial progress without declaring success", async () => {
    const providers = [provider(), provider("work")];
    const controller = createProviderUpdateController({
      readCurrent: () => ({ connected: true, permitted: true, providers }),
    });
    const remaining = deferred<ReturnType<typeof success>>();
    const update = vi.fn(async ({ input }: { input: { instanceId: string } }) =>
      input.instanceId === "work" ? remaining.promise : success(),
    );
    const first = controller.start(machine(providers), update);
    expect(controller.start(machine(providers), update)).toBe(first);
    await flush();
    expect(update).toHaveBeenCalledTimes(2);
    expect(controller.getSnapshot().get(environmentId)).toMatchObject({
      phase: "running",
      runs: [expect.objectContaining({ instanceId: "personal" })],
    });
    remaining.resolve(success([provider("work", "succeeded")]));
    const runs = await first;
    expect(runs).toHaveLength(2);
    expect(controller.getSnapshot().get(environmentId)?.phase).toBe("succeeded");
  });

  it.each(["missing", "running"])("never treats a %s result as success", async (kind) => {
    const controller = createProviderUpdateController({
      readCurrent: () => ({ connected: true, permitted: true, providers: [provider()] }),
    });
    const runs = await controller.start(machine(), async () =>
      success(kind === "missing" ? [] : [provider("personal", "running")]),
    );
    expect(getProviderUpdateRunOutcome(runs[0]!)).toMatchObject({ status: "failed" });
    expect(controller.getSnapshot().get(environmentId)?.phase).toBe("failed");
  });

  it("retains an unverified warning and every mixed failure", async () => {
    const providers = [provider(), provider("work")];
    const controller = createProviderUpdateController({
      readCurrent: () => ({ connected: true, permitted: true, providers }),
    });
    const runs = await controller.start(machine(providers), async ({ input }) =>
      success([provider(input.instanceId, input.instanceId === "work" ? "failed" : "unchanged")]),
    );
    expect(runs.map(getProviderUpdateRunOutcome).map((outcome) => outcome.status)).toEqual([
      "unchanged",
      "failed",
    ]);
    expect(controller.getSnapshot().get(environmentId)).toMatchObject({
      phase: "failed",
      message: expect.stringContaining("Installed version could not be verified."),
    });
    const retried = await controller.start(machine(), async () =>
      success([provider("personal", "unchanged")]),
    );
    expect(retried).toHaveLength(1);
    expect(controller.getSnapshot().get(environmentId)).toMatchObject({
      phase: "unchanged",
      message: expect.stringContaining("could not be verified"),
    });
  });

  it("exposes interruption and permits a per-environment retry", async () => {
    const controller = createProviderUpdateController({
      readCurrent: () => ({ connected: true, permitted: true, providers: [provider()] }),
    });
    await controller.start(machine(), async () => AsyncResult.failure(Cause.interrupt()));
    expect(controller.getSnapshot().get(environmentId)?.phase).toBe("interrupted");
    await controller.start(machine(), async () => success());
    expect(controller.getSnapshot().get(environmentId)).toMatchObject({
      generation: 2,
      phase: "succeeded",
    });
  });

  it("bounds pending recovery and ignores a stale completion after retry", async () => {
    vi.useFakeTimers();
    const controller = createProviderUpdateController({
      readCurrent: () => ({ connected: true, permitted: true, providers: [provider()] }),
      timeoutMs: 100,
    });
    const stale = deferred<ReturnType<typeof success>>();
    const first = controller.start(machine(), () => stale.promise);
    await vi.advanceTimersByTimeAsync(100);
    const timedOut = await first;
    expect(timedOut).toHaveLength(1);
    expect(controller.getSnapshot().get(environmentId)).toMatchObject({
      phase: "failed",
      message: expect.stringContaining("may still be running"),
    });
    await controller.start(machine(), async () => success());
    stale.resolve(success([provider("personal", "failed")]));
    await flush();
    expect(controller.getSnapshot().get(environmentId)).toMatchObject({
      generation: 2,
      phase: "succeeded",
    });
  });

  it("turns read and dispatch exceptions into target failures", async () => {
    const read = vi.fn(() => {
      throw new Error("Snapshot unavailable");
    });
    const controller = createProviderUpdateController({ readCurrent: read });
    const runs = await controller.start(machine(), async () => success());
    expect(getProviderUpdateRunOutcome(runs[0]!)).toMatchObject({
      status: "failed",
      line: expect.stringContaining("Snapshot unavailable"),
    });
    const healthy = createProviderUpdateController({
      readCurrent: () => ({ connected: true, permitted: true, providers: [provider()] }),
    });
    const rejected = await healthy.start(machine(), async () => {
      throw new Error("Connection lost");
    });
    expect(getProviderUpdateRunOutcome(rejected[0]!)).toMatchObject({
      status: "failed",
      line: expect.stringContaining("Connection lost"),
    });
  });
});
