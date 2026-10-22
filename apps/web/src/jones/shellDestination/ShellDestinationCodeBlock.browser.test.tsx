import { act, type ReactElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { DestinationEnvironment } from "./model";

const state = vi.hoisted(() => ({
  environments: [] as DestinationEnvironment[],
  sessions: [] as { threadId: string; terminalId: string; status: string }[],
  open: vi.fn(),
  write: vi.fn(),
  close: vi.fn(),
  queryTime: 0,
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
vi.mock(import("../../state/query"), async (original) => ({
  ...(await original()),
  useEnvironmentQuery: () =>
    ({
      data: state.sessions,
      dataUpdatedAt: state.queryTime,
      isSuccess: true,
      isPending: false,
      error: null,
      failure: null,
      refresh: vi.fn(),
    }) as never,
}));
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

const roots: Root[] = [];
let fixture = 0;
beforeEach(() => {
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
  state.open.mockReset().mockImplementation(async ({ input }) => {
    state.sessions.push({
      threadId: input.threadId,
      terminalId: input.terminalId,
      status: "running",
    });
    return { _tag: "Success" };
  });
  state.write.mockReset().mockResolvedValue({ _tag: "Success" });
  state.close.mockReset().mockResolvedValue({ _tag: "Success" });
});
afterEach(async () => {
  for (const root of roots.splice(0)) await act(async () => root.unmount());
  document.body.replaceChildren();
  vi.unstubAllGlobals();
});
async function render(element: ReactElement) {
  await getSyntaxHighlighterPromise("bash");
  const container = document.body.appendChild(document.createElement("div"));
  const root = createRoot(container);
  roots.push(root);
  await act(async () => root.render(element));
  return {
    container,
    rerender: (element: ReactElement) => act(async () => root.render(element)),
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

describe("destination shell Markdown UI", () => {
  it("shows authoritative destination, uses a single native run, focuses it, and preserves it across unmount", async () => {
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
    await view.unmount();
    state.sessions = [];
    const checking = await render(element);
    expect(checking.container.textContent).toContain("checking previous terminal");
    expect(checking.container.querySelector("textarea")).toBeNull();
    await checking.unmount();
    state.queryTime = Date.now() + 1;
    const missing = await render(element);
    expect(missing.container.textContent).toContain("The previous terminal is unavailable");
    expect(missing.container.querySelector("textarea")).toBeNull();
    expect(state.open).toHaveBeenCalledTimes(1);
    expect(state.write).toHaveBeenCalledTimes(1);
    expect(state.close).not.toHaveBeenCalled();
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
    expect(container.textContent).toContain("Command delivery is unknown");
    expect(container.querySelector("textarea")).not.toBeNull();
    const firstId = state.open.mock.calls[0]?.[0].input.terminalId;
    await act(async () =>
      [...container.querySelectorAll("button")]
        .find((button) => button.textContent === "Run again")!
        .click(),
    );
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
