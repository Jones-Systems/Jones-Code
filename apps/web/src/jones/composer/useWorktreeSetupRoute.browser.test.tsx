import { EnvironmentId, ThreadId, type WorktreeSetupSnapshot } from "@t3tools/contracts";
import { RegistryContext, useAtomValue } from "@effect/atom-react";
import { AsyncResult, Atom } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";
import { useWorktreeSetup } from "../../components/chat/useWorktreeSetup";
import { appAtomRegistry } from "../../rpc/atomRegistry";

const snapshotAtom = Atom.make<AsyncResult.AsyncResult<WorktreeSetupSnapshot | null>>(
  AsyncResult.initial(),
).pipe(Atom.keepAlive);
function setSnapshot(value: WorktreeSetupSnapshot | null | undefined) {
  appAtomRegistry.set(
    snapshotAtom,
    value === undefined ? AsyncResult.initial() : AsyncResult.success(value),
  );
}
vi.mock(import("../../state/vcs"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    vcsEnvironment: { ...actual.vcsEnvironment, worktreeSetup: () => snapshotAtom },
  };
});
vi.mock(import("../../state/use-atom-command"), () => ({
  useAtomCommand: () => vi.fn(),
}));

function Fixture(props: Parameters<typeof FixtureContent>[0]) {
  return (
    <RegistryContext.Provider value={appAtomRegistry}>
      <FixtureContent {...props} />
    </RegistryContext.Provider>
  );
}

function FixtureContent({
  environment = "one",
  thread = "thread",
}: {
  environment?: string;
  thread?: string;
}) {
  const snapshot = AsyncResult.getOrElse(useAtomValue(snapshotAtom), () => null);
  // ChatView creates a new scoped reference on each render.
  const routeThreadRef = {
    environmentId: EnvironmentId.make(environment),
    threadId: ThreadId.make(thread),
  };
  const result = useWorktreeSetup({
    routeThreadRef,
    ownerKey: `${environment}:${thread}`,
    activeThread: snapshot
      ? { id: routeThreadRef.threadId, worktreePath: snapshot.worktreePath }
      : undefined,
    threadRef: null,
    serverProjection: null,
    activityRun: null,
    localPreparing: false,
    sendQueuesBehindSetup: false,
    followUpSent: false,
  });
  return <output>{result.worktreeSetup?.sequence ?? "none"}</output>;
}

it("renders a new draft and equivalent scoped references without a render loop", async () => {
  await act(() => setSnapshot(undefined));
  await using screen = await render(<Fixture />, { router: false });
  await expect.element(screen.getByText("none")).toBeVisible();
  await screen.rerender(<Fixture />);
  await expect.element(screen.getByText("none")).toBeVisible();
});

it("retains a dropped snapshot on the same route and clears it on thread or environment changes", async () => {
  const running: WorktreeSetupSnapshot = {
    threadId: ThreadId.make("thread"),
    phase: "running",
    startedAt: "2026-11-03T00:00:00.000Z",
    endedAt: null,
    branch: null,
    baseRef: null,
    worktreePath: "/synthetic/worktree",
    setupScript: null,
    stages: [],
    error: null,
    sequence: 5,
  };
  await act(() => setSnapshot(running));
  await using screen = await render(<Fixture />, { router: false });
  await expect.element(screen.getByText("5")).toBeVisible();
  await act(() => setSnapshot(undefined));
  await screen.rerender(<Fixture />);
  await expect.element(screen.getByText("5")).toBeVisible();
  await screen.rerender(<Fixture thread="other" />);
  await expect.element(screen.getByText("none")).toBeVisible();
  await act(() => setSnapshot({ ...running, threadId: ThreadId.make("other"), sequence: 6 }));
  await screen.rerender(<Fixture thread="other" />);
  await expect.element(screen.getByText("6")).toBeVisible();
  await act(() => setSnapshot(null));
  await screen.rerender(<Fixture environment="two" thread="other" />);
  await expect.element(screen.getByText("none")).toBeVisible();
});
