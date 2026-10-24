import { act, createRef, type ComponentProps, type ReactElement } from "react";
import { RegistryContext } from "@effect/atom-react";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, MessageId, ThreadId } from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { DestinationEnvironment, ShellDestination } from "./model";
import { ShellRunController } from "./controller";
import { randomUUID } from "../../lib/utils";
import {
  SHELL_RUN_STORAGE_KEY,
  type ShellRun,
  ShellRunStore,
  shellRunKey,
  legacyShellRunKey,
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
import { MessagesTimeline } from "../../components/chat/MessagesTimeline";
import { getSyntaxHighlighterPromise } from "../../lib/syntaxHighlighting";

const roots: Root[] = [];
let fixture = 0;
let registry: AtomRegistry.AtomRegistry;
let savedRuns: string | null;
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
beforeEach(() => {
  savedRuns = localStorage.getItem(SHELL_RUN_STORAGE_KEY);
  localStorage.removeItem(SHELL_RUN_STORAGE_KEY);
  shellRunStore().observe();
  registry = AtomRegistry.make();
  state.metadata = Atom.make<AsyncResult.AsyncResult<readonly TestTerminal[], never>>(
    AsyncResult.initial(),
  );
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
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
});
afterEach(async () => {
  for (const root of roots.splice(0)) await act(async () => root.unmount());
  document.body.replaceChildren();
  if (savedRuns === null) localStorage.removeItem(SHELL_RUN_STORAGE_KEY);
  else localStorage.setItem(SHELL_RUN_STORAGE_KEY, savedRuns);
  shellRunStore().observe();
  registry.dispose();
  vi.unstubAllGlobals();
});
async function render(element: ReactElement) {
  await getSyntaxHighlighterPromise("bash");
  const container = document.body.appendChild(document.createElement("div"));
  const root = createRoot(container);
  roots.push(root);
  await act(async () => {
    publishMetadata();
    root.render(<RegistryContext.Provider value={registry}>{element}</RegistryContext.Provider>);
  });
  return {
    container,
    rerender: (element: ReactElement) =>
      act(async () => {
        publishMetadata();
        root.render(
          <RegistryContext.Provider value={registry}>{element}</RegistryContext.Provider>,
        );
      }),
    unmount: () => act(async () => root.unmount()),
  };
}
function message(
  text: string,
  options: {
    source?: string;
    streaming?: boolean;
    callback?: (command: string) => void;
    backed?: boolean;
    messageId?: string | null;
    threadId?: string;
  } = {},
) {
  const threadRef = scopeThreadRef(
    EnvironmentId.make(options.source ?? "source"),
    ThreadId.make(options.threadId ?? `shell-fixture-${++fixture}`),
  );
  return (
    <ChatMarkdown
      cwd="/source/worktree"
      text={text}
      messageId={options.messageId === null ? undefined : (options.messageId ?? "message")}
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

function timeline(threadId: string) {
  const environmentId = EnvironmentId.make("source");
  const props: ComponentProps<typeof MessagesTimeline> = {
    isWorking: false,
    activeTurnInProgress: false,
    listRef: createRef(),
    latestRun: null,
    turnDiffSummaries: [],
    runs: [],
    routeThreadKey: `source:${threadId}`,
    onOpenTurnDiff: () => {},
    onOpenThread: () => {},
    onForkFromRun: async () => {},
    onRollbackCheckpoint: () => {},
    supportsConversationRollback: false,
    onRevertToTurnCount: () => {},
    isRevertingCheckpoint: false,
    onImageExpand: () => {},
    activeThreadEnvironmentId: environmentId,
    markdownCwd: "/source/worktree",
    resolvedTheme: "light",
    timestampFormat: "locale",
    workspaceRoot: "/source/worktree",
    anchorMessageId: null,
    onAnchorReady: () => {},
    onAnchorSizeChanged: () => {},
    contentInsetEndAdjustment: 0,
    liveFollowEnabled: true,
    onIsAtEndChange: () => {},
    onManualNavigation: () => {},
    onRunShellCommand: vi.fn(),
    timelineEntries: ["native-a", "native-b"].map((id) => ({
      id: `entry-${id}`,
      kind: "message",
      createdAt: "2026-03-17T19:12:28.000Z",
      message: {
        id: MessageId.make(id),
        role: "assistant",
        text: fence(),
        runId: null,
        createdAt: "2026-03-17T19:12:28.000Z",
        updatedAt: "2026-03-17T19:12:28.000Z",
        streaming: false,
      },
    })),
  };
  return (
    <div style={{ height: 1200, width: 900, display: "flex", flexDirection: "column" }}>
      <MessagesTimeline {...props} />
    </div>
  );
}

describe("destination shell Markdown UI", () => {
  it("propagates native message identity through the production timeline and keeps Close independent", async () => {
    const threadId = `production-${++fixture}`;
    const view = await render(timeline(threadId));
    const blocks = () => [
      ...view.container.querySelectorAll<HTMLElement>("[data-shell-destination]"),
    ];
    await expect.poll(() => blocks().length).toBe(2);
    await act(async () => play(blocks()[0]!).click());
    await waitForRun("running");
    expect(play(blocks()[1]!).disabled).toBe(false);
    expect(blocks()[1]!.querySelector("textarea")).toBeNull();
    await act(async () => play(blocks()[1]!).click());
    await waitForRun("running", 2);
    const runs: ShellRun[] = JSON.parse(localStorage.getItem(SHELL_RUN_STORAGE_KEY)!);
    expect(runs.map((run) => run.key).sort()).toEqual(
      ["native-a", "native-b"]
        .map((id) => shellRunKey("source", threadId, 0, "mini", "pwd\n", id))
        .sort(),
    );
    expect(new Set(runs.map((run) => run.terminalId)).size).toBe(2);
    await view.rerender(timeline(threadId));
    expect(view.container.querySelectorAll("textarea")).toHaveLength(2);
    await act(async () =>
      [...blocks()[0]!.querySelectorAll("button")]
        .find((button) => button.textContent === "Close")!
        .click(),
    );
    await act(async () => {
      await expect.poll(() => state.close.mock.calls.length).toBe(1);
      await expect.poll(() => shellRunStore().get(runs[0]!.key)?.phase).toBe("closed");
    });
    expect(blocks()[0]!.querySelector("textarea")).toBeNull();
    expect(blocks()[1]!.querySelector("textarea")).not.toBeNull();
    await view.unmount();
    shellRunStore().observe();
    const remount = await render(
      <>
        {message(fence(), { threadId, messageId: "native-a" })}
        {message(fence(), { threadId, messageId: "native-b" })}
      </>,
    );
    await expect.poll(() => remount.container.querySelectorAll("textarea").length).toBe(1);
    expect(state.write).toHaveBeenCalledTimes(2);
    expect(state.close.mock.calls[0]?.[0].input.terminalId).toBe(runs[0]!.terminalId);
  });
  it("keeps repeated blocks within one message separate", async () => {
    const { container } = await render(message(`${fence()}\n\n${fence()}`));
    const blocks = [...container.querySelectorAll<HTMLElement>("[data-shell-destination]")];
    expect(blocks).toHaveLength(2);
    await act(async () => play(blocks[0]!).click());
    await waitForRun("running");
    expect(blocks[1]!.querySelector("textarea")).toBeNull();
    await act(async () => play(blocks[1]!).click());
    await waitForRun("running", 2);
    expect(container.querySelectorAll("textarea")).toHaveLength(2);
  });
  it.each(["source", "mini"])(
    "visibly refuses a missing message identity on %s",
    async (source) => {
      const callback = vi.fn();
      const { container } = await render(message(fence(), { source, messageId: null, callback }));
      expect(play(container).disabled).toBe(true);
      expect(container.textContent).toContain("no stable identity");
      expect(callback).not.toHaveBeenCalled();
      expect(state.open).not.toHaveBeenCalled();
    },
  );
  it.each(["opening", "writing", "writeUnknown"] as const)(
    "shows ambiguous legacy %s without attaching or replaying it",
    async (phase) => {
      const threadId = `legacy-${++fixture}`;
      const legacy = {
        key: legacyShellRunKey(shellRunKey("source", threadId, 0, "mini", "pwd\n", "message"))!,
        environmentId: "mini",
        terminalId: `run-${randomUUID()}`,
        cwd: "/destination-only",
        phase,
        updatedAt: Date.now(),
      };
      localStorage.setItem(SHELL_RUN_STORAGE_KEY, JSON.stringify([legacy]));
      shellRunStore().observe();
      const { container } = await render(
        <>
          {message(fence(), { threadId, messageId: "message-a" })}
          {message(fence(), { threadId, messageId: "message-b" })}
        </>,
      );
      const blocks = [...container.querySelectorAll<HTMLElement>("[data-shell-destination]")];
      for (const block of blocks) {
        expect(play(block).disabled).toBe(true);
        expect(block.textContent).toContain("terminal cannot be safely assigned");
      }
      expect(container.querySelector("textarea")).toBeNull();
      expect(state.open).not.toHaveBeenCalled();
      await act(async () =>
        [...blocks[0]!.querySelectorAll("button")]
          .find((button) => button.textContent === "Run again")!
          .click(),
      );
      await waitForRun("running");
      expect(state.open.mock.calls[0]?.[0].input.terminalId).not.toBe(legacy.terminalId);
      expect(play(blocks[1]!).disabled).toBe(true);
      expect(blocks[1]!.querySelector("textarea")).toBeNull();
      expect(JSON.parse(localStorage.getItem(SHELL_RUN_STORAGE_KEY)!)).toContainEqual(legacy);
      expect(state.write).toHaveBeenCalledTimes(1);
      expect(state.close).not.toHaveBeenCalled();
    },
  );
  it("uses a successful waiting metadata stream to mount, focus, and preserve the native terminal", async () => {
    const element = message(fence());
    const view = await render(element);
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
    const remount = await render(element);
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
    const callback = vi.fn();
    const { container } = await render(message(fence(), { source: "mini", callback }));
    expect(container.textContent).toContain("Mini · self");
    await act(async () => play(container).click());
    expect(callback).toHaveBeenCalledExactlyOnceWith("pwd");
    expect(state.open).not.toHaveBeenCalled();
  });
  it("checks a restored session and reports a server restart without recreating or replaying it", async () => {
    const element = message(fence());
    const view = await render(element);
    await act(async () => play(view.container).click());
    await waitForRun("running");
    await view.unmount();
    state.sessions = [];
    const checking = await render(element);
    expect(checking.container.textContent).toContain("checking previous terminal");
    expect(checking.container.querySelector("textarea")).toBeNull();
    await checking.unmount();
    state.queryTime = Date.now() + 1;
    const missing = await render(element);
    await waitForRun("unavailable");
    expect(missing.container.textContent).toContain("The previous terminal is unavailable");
    expect(missing.container.querySelector("textarea")).toBeNull();
    expect(state.open).toHaveBeenCalledTimes(1);
    expect(state.write).toHaveBeenCalledTimes(1);
    expect(state.close).not.toHaveBeenCalled();
  });
  it("waits for the first metadata snapshot without mounting or recreating a terminal", async () => {
    state.queryHasSnapshot = false;
    const element = message(fence());
    const view = await render(element);
    await act(async () => play(view.container).click());
    await waitForRun("running");
    expect(view.container.textContent).toContain("checking previous terminal");
    expect(view.container.querySelector("textarea")).toBeNull();
    await view.unmount();
    state.queryHasSnapshot = true;
    const confirmed = await render(element);
    expect(confirmed.container.querySelector("textarea")).not.toBeNull();
    expect(state.open).toHaveBeenCalledTimes(1);
    expect(state.write).toHaveBeenCalledTimes(1);
  });
  it.each(["offline", "connecting", "no-permission", "unknown", "label-mismatch"])(
    "shows the disabled reason for %s",
    async (status) => {
      if (status === "unknown") state.environments = [];
      else if (status === "no-permission")
        state.environments = state.environments.map((entry) => ({ ...entry, canOperate: false }));
      else if (status === "offline" || status === "connecting")
        state.environments = state.environments.map((entry) => ({ ...entry, phase: status }));
      const { container } = await render(
        message(fence(status === "label-mismatch" ? "environment=mini label=Laptop" : undefined)),
      );
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
    state.write.mockResolvedValue({ _tag: "Failure" });
    const { container } = await render(message(fence()));
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
    const { container } = await render(message(text, options));
    expect(play(container).disabled).toBe(true);
    expect(state.open).not.toHaveBeenCalled();
  });
  it("keeps legacy Run in terminal and blocks destination metadata on plain shell fences", async () => {
    const callback = vi.fn();
    const { container, rerender } = await render(message("```bash\npwd\n```", { callback }));
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
    const frame = document.body.appendChild(document.createElement("iframe"));
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
    const key = shellRunKey("source", `native-locks-${++fixture}`, 0, "mini", "pwd\n", "message");
    const input = { key, code: "pwd\n", complete: true, destination: () => destination, terminal };
    await Promise.all([a.run(input), b.run(input)]);
    expect(terminal.open).toHaveBeenCalledTimes(1);
    expect(terminal.write).toHaveBeenCalledTimes(1);
    const otherKey = shellRunKey(
      "source",
      `native-locks-${fixture}`,
      1,
      "mini",
      "pwd\n",
      "message",
    );
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
