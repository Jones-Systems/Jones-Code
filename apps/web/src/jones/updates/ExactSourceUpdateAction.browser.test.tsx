import { act, render as renderScreen, type Screen } from "../../../test/render";
import type { JonesAction } from "@t3tools/client-runtime/jones/updates";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesUpdateProvenance,
  JonesUpdateState,
} from "@t3tools/contracts/jones/jonesUpdates";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  ExactSourceUpdateAction,
  performExactSourceStep,
  type ExactSourceHost,
} from "./ExactSourceUpdateAction";

const environmentId = EnvironmentId.make("exact-source-host");
const target = "a".repeat(40);
const other = "b".repeat(40);
const currentVersion = "0.1.0-preview.20261019.39341000929.1";

const build = (sourceSha: string, artifactId = 7): JonesUpdateProvenance => ({
  repository: "Jones-Systems/Jones-Code",
  sourceSha,
  sourceTree: sourceSha,
  workflow: ".github/workflows/artifact-cli-linux.yml",
  runId: 1,
  runAttempt: 1,
  artifactId,
  artifactDigest: "d".repeat(64),
  platform: "linux",
  architecture: "x64",
  version: "0.1.0-preview.20261019.39341000901.1",
});
const hostState = (overrides: Partial<JonesUpdateState> = {}): JonesUpdateState => ({
  source: "jones-actions",
  channel: "jones-main",
  phase: "available",
  revision: 1,
  capability: { check: true, download: true, install: true, exactSource: true },
  currentVersion,
  installedSource: {
    repository: "Jones-Systems/Jones-Code",
    sha: "e".repeat(40),
    tree: "f".repeat(40),
  },
  provenance: build(other),
  ...overrides,
});
const stagedTarget = (revision = 2) =>
  hostState({ phase: "staged", revision, stagedHandle: "handle-1", provenance: build(target) });

/** A host whose state only changes through the requests the action sends or the test applies. */
function createHost(initial: JonesUpdateState | null) {
  let state = initial;
  let freshness: "fresh" | "stale" = "fresh";
  const requests: JonesAction[] = [];
  const dialogs: string[] = [];
  const handlers = {
    run: (_request: JonesAction): Promise<AtomCommandResult<JonesUpdateState | null, unknown>> =>
      Promise.reject(new Error("No handler for this request.")),
    dialog: async () => true,
  };
  let render = async () => {};
  const snapshot = () => ({
    observation: { state, freshness },
    canOperate: true,
  });
  const host: ExactSourceHost = {
    get snapshot() {
      return snapshot();
    },
    readLatest: snapshot,
    run: (request) => {
      requests.push(request);
      return handlers.run(request);
    },
    confirm: async (message) => {
      dialogs.push(message);
      return handlers.dialog();
    },
  };
  return {
    host,
    requests,
    dialogs,
    handlers,
    set: (next: JonesUpdateState | null, nextFreshness: "fresh" | "stale" = "fresh") => {
      state = next;
      freshness = nextFreshness;
      return render();
    },
    onChange: (callback: () => Promise<void>) => {
      render = callback;
    },
  };
}

let container: HTMLDivElement;
let root: Screen;
let renderDisposables: AsyncDisposableStack;
async function setupFixture1() {
  const stack = new AsyncDisposableStack();
  renderDisposables = stack;
  stack.defer(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();

  });
  try {
  container = document.createElement("div");
  document.body.append(container);
  root = renderDisposables.use(await renderScreen(null, { container, router: false }));

    return stack;
  } catch (error) {
    await stack.disposeAsync();
    throw error;
  }
}


async function mount(world: ReturnType<typeof createHost>) {
  const draw = () =>
    root.rerender(
      <ExactSourceUpdateAction
        environmentId={environmentId}
        label="Mac Mini"
        targetSource={target}
        host={world.host}
      />,
    );
  world.onChange(draw);
  await draw();
}
const buttons = () => [...container.querySelectorAll("button")].map((node) => node.textContent);
const click = async (name: string) => {
  const button = [...container.querySelectorAll("button")].find(
    (node) => node.textContent === name,
  );
  expect(button, `button ${name} in ${container.textContent}`).toBeDefined();
  await act(async () => button!.click());
};
const status = () => container.querySelector('[role="status"]')?.textContent ?? null;

describe("exact-source action", () => {
  it("downloads by source, then installs only after a confirmation bound to the staged handle", async () => {
    await using _fixture = await setupFixture1();
    const world = createHost(hostState());
    world.handlers.run = async () => {
      await world.set(stagedTarget());
      return AsyncResult.success(stagedTarget());
    };
    await mount(world);
    // The host's own offer (another source) never changes the label or the target.
    expect(buttons()).toEqual(["Download aaaaaaa"]);
    await click("Download aaaaaaa");
    expect(world.requests).toEqual([{ action: "downloadExact", input: { targetSource: target } }]);
    // Download never installs by itself.
    expect(buttons()).toEqual(["Install aaaaaaa"]);
    expect(world.dialogs).toEqual([]);

    world.handlers.dialog = async () => false;
    await click("Install aaaaaaa");
    expect(world.dialogs).toHaveLength(1);
    expect(world.dialogs[0]).toContain("aaaaaaa");
    expect(world.dialogs[0]).toContain("Mac Mini");
    expect(world.requests).toHaveLength(1);
    expect(buttons()).toEqual(["Install aaaaaaa"]);

    world.handlers.dialog = async () => true;
    world.handlers.run = async () =>
      AsyncResult.success(hostState({ phase: "installing", revision: 3 }));
    await click("Install aaaaaaa");
    expect(world.requests[1]).toEqual({
      action: "install",
      input: { stagedHandle: "handle-1", environmentId, currentVersion },
    });
  });

  it("refuses an install when the host changed while the dialog was open", async () => {
    await using _fixture = await setupFixture1();
    const world = createHost(stagedTarget());
    world.handlers.dialog = async () => {
      // Another client staged a different build behind this dialog.
      await world.set(
        hostState({
          phase: "staged",
          revision: 3,
          stagedHandle: "handle-2",
          provenance: build(other),
        }),
      );
      return true;
    };
    await mount(world);
    await click("Install aaaaaaa");
    expect(world.requests).toEqual([]);
    expect(buttons()).toEqual([]);
    expect(status()).toContain("bbbbbbb");
  });

  it("does not resend an install whose response was lost, and resumes once the host moves", async () => {
    await using _fixture = await setupFixture1();
    const world = createHost(stagedTarget());
    world.handlers.run = async () => AsyncResult.failure(Cause.fail(new Error("socket closed")));
    await mount(world);
    await click("Install aaaaaaa");
    expect(world.requests).toHaveLength(1);
    expect(buttons()).toEqual([]);
    expect(status()).toContain("will not be sent again");
    // The same staged state is not evidence that the install failed.
    await world.set(stagedTarget(2));
    expect(buttons()).toEqual([]);
    // Once the host reports movement the notice reconciles from the host's own state.
    await world.set(
      hostState({
        phase: "committed",
        revision: 5,
        installedSource: { repository: "Jones-Systems/Jones-Code", sha: target, tree: target },
      }),
    );
    expect(buttons()).toEqual([]);
    expect(status()).toContain("already runs source aaaaaaa");
    expect(world.requests).toHaveLength(1);
  });

  it("stays in the notice with the host's progress while a download transitions", async () => {
    await using _fixture = await setupFixture1();
    const world = createHost(hostState());
    let finish: (result: AtomCommandResult<JonesUpdateState | null, unknown>) => void = () => {};
    world.handlers.run = () => new Promise((resolve) => (finish = resolve));
    await mount(world);
    await click("Download aaaaaaa");
    // A second click while the request is outstanding must not send another.
    expect(buttons()).toEqual(["Downloading…"]);
    await click("Downloading…");
    expect(world.requests).toHaveLength(1);
    await world.set(hostState({ phase: "downloading", revision: 2, message: "Downloading build" }));
    expect(buttons()).toEqual(["Downloading…"]);
    await act(async () => {
      await world.set(stagedTarget(3));
      finish(AsyncResult.success(stagedTarget(3)));
    });
    expect(buttons()).toEqual(["Install aaaaaaa"]);
    await world.set(
      hostState({
        phase: "checking",
        revision: 4,
        stagedHandle: "handle-1",
        provenance: build(target),
      }),
    );
    expect(buttons()).toEqual([]);
    expect(status()).toBe("checking");
  });

  it("shows the refusal when a download stages another source than the target", async () => {
    await using _fixture = await setupFixture1();
    const world = createHost(hostState());
    world.handlers.run = async () => {
      const wrong = hostState({
        phase: "staged",
        revision: 2,
        stagedHandle: "handle-9",
        provenance: build(other),
      });
      await world.set(wrong);
      return AsyncResult.success(wrong);
    };
    await mount(world);
    await click("Download aaaaaaa");
    expect(status()).toContain("bbbbbbb");
    expect(status()).toContain("will not be installed from this notice");
    expect(buttons()).toEqual([]);
  });

  it("reports a host refusal and a failed download without installing anything", async () => {
    await using _fixture = await setupFixture1();
    const world = createHost(hostState());
    world.handlers.run = async () =>
      AsyncResult.success(
        hostState({ phase: "blocked", message: "Candidate is not newer than installed." }),
      );
    await mount(world);
    await click("Download aaaaaaa");
    expect(status()).toBe("Candidate is not newer than installed.");
    world.handlers.run = async () => AsyncResult.failure(Cause.fail(new Error("timeout")));
    await click("Download aaaaaaa");
    expect(status()).toContain("download request did not finish");
    expect(world.requests.every((request) => request.action === "downloadExact")).toBe(true);
  });

  it("lets an older host download only an offer that already is the target", async () => {
    await using _fixture = await setupFixture1();
    const legacy = { check: true, download: true, install: true };
    const mismatch = createHost(hostState({ capability: legacy }));
    await mount(mismatch);
    expect(buttons()).toEqual([]);
    expect(status()).toContain("bbbbbbb");
    expect(mismatch.requests).toEqual([]);
    await act(async () => root.unmount());
    root = renderDisposables.use(await renderScreen(null, { container, router: false }));

    const match = createHost(hostState({ capability: legacy, provenance: build(target, 42) }));
    match.handlers.run = async () => AsyncResult.success(hostState({ phase: "downloading" }));
    await mount(match);
    await click("Download aaaaaaa");
    expect(match.requests).toEqual([
      { action: "download", input: { artifactId: 42, sourceSha: target } },
    ]);
  });

  it("offers nothing while the host observation is stale or the host has no updater", async () => {
    await using _fixture = await setupFixture1();
    const world = createHost(hostState());
    await mount(world);
    await world.set(hostState(), "stale");
    expect(buttons()).toEqual([]);
    expect(status()).toContain("Waiting");
    await world.set(null);
    expect(status()).toContain("does not support Jones updates");
    expect(world.requests).toEqual([]);
  });
});

describe("performExactSourceStep", () => {
  it("treats an interrupted request as cancelled, not as an unknown install", async () => {
    await using _fixture = await setupFixture1();
    const world = createHost(stagedTarget());
    world.handlers.run = async () => AsyncResult.failure(Cause.interrupt());
    const outcome = await performExactSourceStep({
      environmentId,
      label: "Mac Mini",
      targetSource: target,
      step: {
        kind: "install",
        input: { stagedHandle: "handle-1", environmentId, currentVersion },
      },
      host: world.host,
    });
    expect(outcome).toEqual({ kind: "cancelled" });
  });

  it("records the revision an unknown install was sent against", async () => {
    await using _fixture = await setupFixture1();
    const world = createHost(stagedTarget(6));
    world.handlers.run = () => Promise.reject(new Error("connection reset"));
    const outcome = await performExactSourceStep({
      environmentId,
      label: "Mac Mini",
      targetSource: target,
      step: {
        kind: "install",
        input: { stagedHandle: "handle-1", environmentId, currentVersion },
      },
      host: world.host,
    });
    expect(outcome).toMatchObject({
      kind: "unknown",
      unresolvedInstall: { stagedHandle: "handle-1", revision: 6 },
    });
  });
});
