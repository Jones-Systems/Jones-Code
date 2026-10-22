import type { ReactElement } from "react";
import { RegistryContext } from "@effect/atom-react";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";
import { act, render as renderScreen } from "../../../test/render";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { DestinationEnvironment, ShellDestination } from "./model";
import { ShellRunController } from "./controller";
import { randomUUID } from "../../lib/utils";
import {
  SHELL_RUN_STORAGE_KEY,
  type ShellRun,
  ShellRunStore,
  shellRunKey,
  shellRunStore,
} from "./runStore";

type TestTerminal = { threadId: string; terminalId: string; status: string };
const state = vi.hoisted(() => ({
  environments: [] as DestinationEnvironment[],
  sessions: [] as TestTerminal[],
  metadata: null as Atom.Writable<AsyncResult.AsyncResult<readonly TestTerminal[], never>> | null,
  open: vi.fn(),
  write: vi.fn(),
  close: vi.fn(),
  queryTime: 0,
  queryWaiting: true,
  queryHasSnapshot: true,
}));
vi.mock(import("./useShellDestination"), async () => {
  const { resolveShellDestination } = await import("./model");
  const resolve = (fence: Parameters<typeof resolveShellDestination>[0], source: string) =>
    resolveShellDestination(fence, source, state.environments);
  return {
    readShellDestination: resolve,
    useShellDestination: (fence, source) => {
      const destination = resolve(fence, source);
      const environment = state.environments.find(
        (entry) => entry.environmentId === destination.environmentId,
      );
      return {
        destination,
        target: environment
          ? ({
              environmentId: EnvironmentId.make(environment.environmentId),
              serverConfig: { cwd: environment.cwd, keybindings: [] },
            } as never)
          : null,
      };
    },
  };
});
vi.mock(import("../../state/query"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    // Exercise the actual adapter: a live Success snapshot can still be waiting.
    useEnvironmentQuery: ((query: unknown) =>
      actual.useEnvironmentQuery(
        query ? state.metadata : null,
      )) as typeof actual.useEnvironmentQuery,
  };
});

vi.mock(import("../../state/use-atom-command"), async () => {
  const { terminalEnvironment } = await import("../../state/terminal");
  return {
    useAtomCommand: ((command: unknown) =>
      command === terminalEnvironment.open
        ? state.open
        : command === terminalEnvironment.write
          ? state.write
          : command === terminalEnvironment.close
            ? state.close
            : vi.fn(async () => ({ _tag: "Success" }))) as never,
  };
});
vi.mock(import("../../state/environments"), async (original) => ({
  ...(await original()),
  useEnvironment: () => ({ label: "Source computer" }) as never,
}));
vi.mock(import("../../state/server"), async (original) => {
  const actual = await original();
  const { Atom } = await import("effect/reactivity");
  const noConfig = Atom.make(null);
  return {
    ...actual,
    serverEnvironment: { ...actual.serverEnvironment, configValueAtom: (() => noConfig) as never },
  };
});
vi.mock(import("../../state/session"), async (original) => ({
  ...(await original()),
  useEnvironmentScope: (() => true) as never,
  readEnvironmentScope: (() => true) as never,
}));
vi.mock(import("../../remoteOpen"), async (original) => ({
  ...(await original()),
  useRemoteOpenResolution: () => ({ state: { mode: "local-exec" }, isResolved: true }) as never,
}));
vi.mock(
  import("../../components/ThreadTerminalDrawer"),
  () =>
    ({
      TerminalViewport: (props: {
        cwd: string;
        focusRequestId: number;
        attachExistingOnly?: boolean;
      }) => (
        <textarea
          aria-label="Native terminal test surface"
          data-cwd={props.cwd}
          data-focus-request={props.focusRequestId}
          data-attach-existing={String(props.attachExistingOnly)}
        />
      ),
    }) as never,
);

import ChatMarkdown from "../../components/ChatMarkdown";
import { getSyntaxHighlighterPromise } from "../../lib/syntaxHighlighting";

let fixture = 0;

function setupFixture() {
  using resources = new DisposableStack();
  const previousStorage = localStorage.getItem(SHELL_RUN_STORAGE_KEY);
  resources.defer(() => {
    if (previousStorage === null) localStorage.removeItem(SHELL_RUN_STORAGE_KEY);
    else localStorage.setItem(SHELL_RUN_STORAGE_KEY, previousStorage);
    shellRunStore().observe();
  });
  localStorage.removeItem(SHELL_RUN_STORAGE_KEY);
  shellRunStore().observe();
  const registry = AtomRegistry.make();
  resources.defer(() => {
    registry.dispose();
    state.metadata = null;
  });
  state.metadata = Atom.make<AsyncResult.AsyncResult<readonly TestTerminal[], never>>(
    AsyncResult.initial(),
  );
  function publishMetadata() {
    registry.set(
      state.metadata!,
      state.queryHasSnapshot
        ? AsyncResult.success(state.sessions, {
            waiting: state.queryWaiting,
            timestamp: state.queryTime,
          })
        : AsyncResult.initial(true),
    );
  }
  state.environments = [
    {
      environmentId: "mini",
      label: "Mini",
      serverLabel: "Mini",
      cwd: "/destination-only",
      phase: "connected",
      canOperate: true,
    },
  ];
  state.sessions = [];
  state.queryTime = 0;
  state.queryWaiting = true;
  state.queryHasSnapshot = true;
  state.open.mockReset().mockImplementation(async ({ input }) => {
    state.sessions = [
      ...state.sessions,
      {
        threadId: input.threadId,
        terminalId: input.terminalId,
        status: "running",
      },
    ];
    publishMetadata();
    return { _tag: "Success" };
  });
  state.write.mockReset().mockResolvedValue({ _tag: "Success" });
  state.close.mockReset().mockResolvedValue({ _tag: "Success" });
  const owned = resources.move();
  return {
    [Symbol.dispose]: () => owned.dispose(),
    async render(element: ReactElement) {
      await getSyntaxHighlighterPromise("bash");
      publishMetadata();
      const screen = await renderScreen(
        <RegistryContext.Provider value={registry}>{element}</RegistryContext.Provider>,
        { router: false },
      );
      return {
        ...screen,
        rerender: (element: ReactElement) =>
          act(async () => {
            publishMetadata();
            await screen.rerender(
              <RegistryContext.Provider value={registry}>{element}</RegistryContext.Provider>,
            );
          }),
      };
    },
  };
}
function message(
  text: string,
  options: {
    source?: string;
    streaming?: boolean;
    callback?: (command: string) => void;
    backed?: boolean;
  } = {},
) {
  const threadRef = scopeThreadRef(
    EnvironmentId.make(options.source ?? "source"),
    ThreadId.make(`shell-fixture-${++fixture}`),
  );
  return (
    <ChatMarkdown
      cwd="/source/worktree"
      text={text}
      threadRef={options.backed === false ? undefined : threadRef}
      onRunShellCommand={options.backed === false ? undefined : (options.callback ?? vi.fn())}
      isStreaming={options.streaming ?? false}
    />
  );
}
const fence = (meta = "environment=mini label=Mini", code = "pwd", close = true) =>
  `\`\`\`jones-shell ${meta}\n${code}\n${close ? "```" : ""}`;
const play = (container: HTMLElement) =>
  container.querySelector<HTMLButtonElement>('button[aria-label="Run on Mini"]')!;

async function waitForRun(phase: ShellRun["phase"], writes = 1) {
  await act(async () => {
    await expect
      .poll(() => {
        if (state.write.mock.calls.length !== writes) return null;
        const id = state.open.mock.lastCall?.[0].input.terminalId;
        const runs: ShellRun[] = JSON.parse(localStorage.getItem(SHELL_RUN_STORAGE_KEY) ?? "[]");
        return runs.find((run) => run.terminalId === id)?.phase;
      })
      .toBe(phase);
  });
}

describe("destination shell Markdown UI", () => {
  it("uses a successful waiting metadata stream to mount, focus, and preserve the native terminal", async () => {
    using harness = setupFixture();
    const { render } = harness;
    const element = message(fence());
    await using view = await render(element);
    expect(view.container.textContent).toContain("Mini · ready");
    expect(
      view.container.querySelector('[aria-label="Destination computer: mini"]'),
    ).not.toBeNull();
    await act(async () => {
      play(view.container).click();
      play(view.container).click();
    });
    await waitForRun("running");
    expect(state.open).toHaveBeenCalledTimes(1);
    expect(state.write).toHaveBeenCalledTimes(1);
    expect(state.open.mock.calls[0]?.[0]).toMatchObject({
      environmentId: "mini",
      input: { threadId: "jones-shell-run", cwd: "/destination-only" },
    });
    const terminal = view.container.querySelector("textarea")!;
    expect(terminal.getAttribute("data-attach-existing")).toBe("true");
    const focus = terminal.getAttribute("data-focus-request");
    await act(async () =>
      view.container
        .querySelector<HTMLButtonElement>('button[aria-label="Focus terminal on Mini"]')!
        .click(),
    );
    expect(terminal.getAttribute("data-focus-request")).not.toBe(focus);
    expect(state.write).toHaveBeenCalledTimes(1);
    await view.unmount();
    expect(state.close).not.toHaveBeenCalled();
    await using remount = await render(element);
    expect(remount.container.querySelector("textarea")).not.toBeNull();
    expect(state.write).toHaveBeenCalledTimes(1);
    await act(async () =>
      [...remount.container.querySelectorAll("button")]
        .find((button) => button.textContent === "Close")!
        .click(),
    );
    await waitForRun("closed");
    expect(state.close.mock.calls[0]?.[0].input.deleteHistory).toBe(true);
    expect(remount.container.querySelector("textarea")).toBeNull();
  });
  it("uses the existing callback for the source computer without opening a second terminal", async () => {
    using harness = setupFixture();
    const { render } = harness;
    const callback = vi.fn();
    await using view = await render(message(fence(), { source: "mini", callback }));
    const { container } = view;
    expect(container.textContent).toContain("Mini · self");
    await act(async () => play(container).click());
    expect(callback).toHaveBeenCalledExactlyOnceWith("pwd");
    expect(state.open).not.toHaveBeenCalled();
  });
  it("checks a restored session and reports a server restart without recreating or replaying it", async () => {
    using harness = setupFixture();
    const { render } = harness;
    const element = message(fence());
    await using view = await render(element);
    await act(async () => play(view.container).click());
    await waitForRun("running");
    await view.unmount();
    state.sessions = [];
    await using checking = await render(element);
    expect(checking.container.textContent).toContain("checking previous terminal");
    expect(checking.container.querySelector("textarea")).toBeNull();
    await checking.unmount();
    const terminalId = state.open.mock.lastCall?.[0].input.terminalId;
    const recordedRuns: ShellRun[] = JSON.parse(
      localStorage.getItem(SHELL_RUN_STORAGE_KEY) ?? "[]",
    );
    state.queryTime = recordedRuns.find((run) => run.terminalId === terminalId)!.updatedAt + 1;
    await using missing = await render(element);
    await waitForRun("unavailable");
    expect(missing.container.textContent).toContain("The previous terminal is unavailable");
    expect(missing.container.querySelector("textarea")).toBeNull();
    expect(state.open).toHaveBeenCalledTimes(1);
    expect(state.write).toHaveBeenCalledTimes(1);
    expect(state.close).not.toHaveBeenCalled();
  });
  it("waits for the first metadata snapshot without mounting or recreating a terminal", async () => {
    using harness = setupFixture();
    const { render } = harness;
    state.queryHasSnapshot = false;
    const element = message(fence());
    await using view = await render(element);
    await act(async () => play(view.container).click());
    await waitForRun("running");
    expect(view.container.textContent).toContain("checking previous terminal");
    expect(view.container.querySelector("textarea")).toBeNull();
    await view.unmount();
    state.queryHasSnapshot = true;
    await using confirmed = await render(element);
    expect(confirmed.container.querySelector("textarea")).not.toBeNull();
    expect(state.open).toHaveBeenCalledTimes(1);
    expect(state.write).toHaveBeenCalledTimes(1);
  });
  it.each(["offline", "connecting", "no-permission", "unknown", "label-mismatch"])(
    "shows the disabled reason for %s",
    async (status) => {
      using harness = setupFixture();
      const { render } = harness;
      if (status === "unknown") state.environments = [];
      else if (status === "no-permission")
        state.environments = state.environments.map((entry) => ({ ...entry, canOperate: false }));
      else if (status === "offline" || status === "connecting")
        state.environments = state.environments.map((entry) => ({ ...entry, phase: status }));
      await using view = await render(
        message(fence(status === "label-mismatch" ? "environment=mini label=Laptop" : undefined)),
      );
      const { container } = view;
      expect(container.querySelector(`[data-shell-destination="${status}"]`)).not.toBeNull();
      expect(
        container.querySelector<HTMLButtonElement>('button[aria-label^="Run on"]')!.disabled,
      ).toBe(true);
      expect(container.querySelector("p")?.textContent).not.toBe("");
      if (status === "label-mismatch")
        expect(container.textContent).toContain("Agent claimed: Laptop");
      expect(state.open).not.toHaveBeenCalled();
    },
  );
  it("keeps a failed write visible and Run again requires a fresh terminal", async () => {
    using harness = setupFixture();
    const { render } = harness;
    state.write.mockResolvedValue({ _tag: "Failure" });
    await using view = await render(message(fence()));
    const { container } = view;
    await act(async () => play(container).click());
    await waitForRun("writeUnknown");
    expect(container.textContent).toContain("Command delivery is unknown");
    expect(container.querySelector("textarea")).not.toBeNull();
    const firstId = state.open.mock.calls[0]?.[0].input.terminalId;
    await act(async () =>
      [...container.querySelectorAll("button")]
        .find((button) => button.textContent === "Run again")!
        .click(),
    );
    await waitForRun("writeUnknown", 2);
    expect(state.open.mock.calls[1]?.[0].input.terminalId).not.toBe(firstId);
    expect(state.write).toHaveBeenCalledTimes(2);
  });
  it.each([
    [fence(), { streaming: true }],
    [fence(undefined, "pwd", false), {}],
    [fence(undefined, "pwd\nwhoami"), {}],
    [fence(undefined, "echo\u200bbad"), {}],
    [fence(undefined, "echo \\"), {}],
    [fence(), { backed: false }],
  ])("does not run incomplete, unsafe, or non-project blocks", async (text, options) => {
    using harness = setupFixture();
    const { render } = harness;
    await using view = await render(message(text, options));
    const { container } = view;
    expect(play(container).disabled).toBe(true);
    expect(state.open).not.toHaveBeenCalled();
  });
  it("keeps legacy Run in terminal and blocks destination metadata on plain shell fences", async () => {
    using harness = setupFixture();
    const { render } = harness;
    const callback = vi.fn();
    await using view = await render(message("```bash\npwd\n```", { callback }));
    const { container, rerender } = view;
    const button = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Run in terminal"]',
    )!;
    await act(async () => button.click());
    expect(callback).toHaveBeenCalledExactlyOnceWith("pwd");
    await rerender(message("```bash environment=mini\npwd\n```", { callback }));
    expect(container.querySelector('button[aria-label="Run in terminal"]')).toBeNull();
    expect(container.textContent).toContain("Use a jones-shell fence");
  });
});

describe("browser cross-context run coordination", () => {
  it("uses native Web Locks across browsing contexts and observes shared storage updates", async () => {
    using _harness = setupFixture();
    await using screen = await renderScreen(<iframe />, { router: false });
    const frame = screen.container.querySelector("iframe")!;
    const otherWindow = frame.contentWindow!;
    const locks = otherWindow.navigator.locks;
    expect(locks).toBeDefined();
    const aStore = shellRunStore();
    const bStore = new ShellRunStore(otherWindow.localStorage, Date.now, {
      exclusive: (name, work) => locks.request(name, work),
      tryExclusive: (name, work) =>
        locks.request(name, { ifAvailable: true }, (lock) => (lock ? work() : undefined)),
    });
    const a = new ShellRunController(aStore, randomUUID);
    const b = new ShellRunController(bStore, randomUUID);
    const destination: ShellDestination = {
      status: "ready",
      environmentId: "mini",
      label: "Mini",
      cwd: "/destination-only",
      reason: null,
    };
    const terminal = {
      open: vi.fn(async () => true),
      write: vi.fn(async () => false),
      close: vi.fn(async () => true),
    };
    const key = shellRunKey("source", `native-locks-${++fixture}`, 0, "mini", "pwd\n");
    const input = { key, code: "pwd\n", complete: true, destination: () => destination, terminal };
    await Promise.all([a.run(input), b.run(input)]);
    expect(terminal.open).toHaveBeenCalledTimes(1);
    expect(terminal.write).toHaveBeenCalledTimes(1);
    const otherKey = shellRunKey("source", `native-locks-${fixture}`, 1, "mini", "pwd\n");
    await b.run({ ...input, key: otherKey });
    // This notification is a real storage event from the other browsing context.
    await expect.poll(() => aStore.get(otherKey)?.phase).toBe("writeUnknown");
    const restored = new ShellRunStore(localStorage);
    expect(restored.get(key)?.phase).toBe("writeUnknown");
    expect(restored.get(otherKey)?.phase).toBe("writeUnknown");
    await new ShellRunController(restored, randomUUID).run(input);
    expect(terminal.write).toHaveBeenCalledTimes(2);
  });
});
