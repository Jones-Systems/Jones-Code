import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import { nextTerminalAttachSeedState } from "@t3tools/client-runtime/state/terminal";
import { expect, it, vi } from "vite-plus/test";
import { render } from "../../../test/render";

const state = vi.hoisted(() => ({ attach: vi.fn(), command: vi.fn(), surface: vi.fn() }));
vi.mock(import("@effect/atom-react"), async (original) => ({
  ...(await original()),
  useAtomValue: (() => null) as never,
}));
vi.mock(import("../../hooks/useSettings"), async (original) => ({
  ...(await original()),
  getClientSettings: () => DEFAULT_CLIENT_SETTINGS,
  useClientSettings: ((select: (settings: typeof DEFAULT_CLIENT_SETTINGS) => unknown) =>
    select(DEFAULT_CLIENT_SETTINGS)) as never,
}));
vi.mock(import("../../editorPreferences"), async (original) => ({
  ...(await original()),
  useOpenInPreferredEditor: (() => state.command) as never,
}));
vi.mock(import("../../state/use-atom-command"), () => ({
  useAtomCommand: (() => state.command) as never,
}));
vi.mock(import("../../state/terminalSessions"), async (original) => ({
  ...(await original()),
  useAttachedTerminalSession: state.attach as never,
}));
vi.mock(import("../../terminal/ghostty/surface"), async (original) => ({
  ...(await original()),
  GhosttyTerminalSurface: { create: state.surface } as never,
}));
vi.mock(import("../../state/session"), async (original) => ({
  ...(await original()),
  useEnvironmentScope: (() => true) as never,
  readEnvironmentScope: (() => true) as never,
}));

import { TerminalViewport } from "../../components/ThreadTerminalDrawer";

it("reattaches a destination run without the launch fields that can create a missing PTY", async () => {
  state.attach.mockReset().mockReturnValue(nextTerminalAttachSeedState());
  state.command.mockReset();
  state.surface.mockReset().mockReturnValue(new Promise(() => undefined));
  const threadRef = {
    environmentId: EnvironmentId.make("destination"),
    threadId: ThreadId.make("jones-shell-run"),
  };
  const props = {
    advancedTypography: false,
    threadRef,
    threadId: threadRef.threadId,
    terminalId: "run-00000000-0000-4000-8000-000000000001",
    terminalLabel: "Run on Mini",
    cwd: "/destination",
    onSessionExited: state.command,
    focusRequestId: 0,
    autoFocus: false,
    visible: true,
    resizeEpoch: 0,
    drawerHeight: 256,
    keybindings: [],
  };
  await using screen = await render(<TerminalViewport {...props} attachExistingOnly />, {
    router: false,
  });
  expect(state.attach).toHaveBeenLastCalledWith({
    environmentId: "destination",
    terminal: {
      threadId: "jones-shell-run",
      terminalId: props.terminalId,
      restartIfNotRunning: false,
    },
  });
  state.attach.mockReturnValue({
    ...nextTerminalAttachSeedState(),
    status: "error",
    error: "Terminal session not found",
  });
  await screen.rerender(<TerminalViewport {...props} attachExistingOnly focusRequestId={1} />);
  expect(state.attach.mock.lastCall?.[0].terminal).not.toHaveProperty("cwd");
  expect(state.command).not.toHaveBeenCalled();
  // The existing drawer and onboarding callers still opt into opening a shell.
  await screen.rerender(<TerminalViewport {...props} />);
  expect(state.attach.mock.lastCall?.[0].terminal).toEqual({
    threadId: "jones-shell-run",
    terminalId: props.terminalId,
    cwd: "/destination",
  });
});
