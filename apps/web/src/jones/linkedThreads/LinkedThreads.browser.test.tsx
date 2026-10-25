import { RegistryContext } from "@effect/atom-react";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type {
  LinkedThreadLink,
  LinkedThreadsListResult,
  ThreadEndpointRef,
} from "@t3tools/contracts/jones/linkedThreads";
import { endpointKey } from "@t3tools/client-runtime/jones/linked-threads";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import * as Cause from "effect/Cause";
import { useState } from "react";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
import type { EnvironmentPresentation } from "../../state/environments";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import type { LineageRow } from "../remoteAgentLineage/model";
import "~/index.css";

const state = vi.hoisted(() => ({
  environments: [] as EnvironmentPresentation[],
  shells: [] as EnvironmentThreadShell[],
  queries: new Map<
    string,
    Atom.Writable<AsyncResult.AsyncResult<LinkedThreadsListResult, unknown>>
  >(),
  refreshes: new Map<string, Atom.Writable<number>>(),
  list: vi.fn(),
  command: vi.fn(),
  toast: vi.fn(),
  navigate: vi.fn(),
  copy: vi.fn(),
}));
vi.mock(import("./state"), async () => {
  const { Atom, AsyncResult } = await import("effect/reactivity");
  const { endpointKey } = await import("@t3tools/client-runtime/jones/linked-threads");
  const granted = Atom.make(true);
  const command = (label: string) => ({ label, permissionAtom: () => granted });
  return {
    linkedThreads: {
      list: (target: { environmentId: string; input: { thread: ThreadEndpointRef } }) => {
        state.list(target);
        const key = JSON.stringify([target.environmentId, endpointKey(target.input.thread)]);
        if (!state.queries.has(key))
          state.queries.set(
            key,
            Atom.make<AsyncResult.AsyncResult<LinkedThreadsListResult, unknown>>(
              AsyncResult.initial(),
            ),
          );
        return state.queries.get(key)!;
      },
      refreshAtom: (subject: ThreadEndpointRef) => {
        const key = endpointKey(subject);
        if (!state.refreshes.has(key)) state.refreshes.set(key, Atom.make(0).pipe(Atom.keepAlive));
        return state.refreshes.get(key)!;
      },
      link: command("link"),
      unlink: command("unlink"),
      accept: command("accept"),
    },
  } as never;
});
vi.mock(import("../../state/environments"), async (original) => ({
  ...(await original()),
  useEnvironments: () => ({ environments: state.environments }) as never,
  useEnvironment: (id: EnvironmentId | null) =>
    state.environments.find((e) => e.environmentId === id) ?? null,
}));
vi.mock(import("../../state/entities"), async (original) => ({
  ...(await original()),
  useQuietThreadShells: () => state.shells,
  useQuietThreadShell: (ref: ThreadEndpointRef | null) =>
    state.shells.find((s) => s.environmentId === ref?.environmentId && s.id === ref?.threadId) ??
    null,
}));
vi.mock(import("../../state/use-atom-command"), () => ({
  useAtomCommand: (command: { label: string }) => (target: unknown) =>
    state.command(command.label, target),
}));
vi.mock(import("../../components/ui/toast"), async (original) => ({
  ...(await original()),
  toastManager: { add: state.toast } as never,
}));
vi.mock(import("../../hooks/useCopyToClipboard"), async (original) => ({
  ...(await original()),
  writeTextToClipboard: state.copy,
}));
vi.mock(import("@tanstack/react-router"), async (original) => ({
  ...(await original()),
  useNavigate: () => state.navigate,
}));

import { linkedThreads } from "./state";
import { useLinkedThreads } from "./useLinkedThreads";
import { buildLinkedEntries, type LinkedEntry } from "./model";
import { LinkedThreadsSection } from "./LinkedThreadsSection";
import { LinkedLineageGroups } from "./LinkedLineageGroups";
import { LinkedThreadRow } from "./LinkedThreadRow";
import { LinkThreadDialog } from "./LinkThreadDialog";
import { CopyHandoffDialog } from "./CopyHandoffDialog";

const ref = (environmentId: string, threadId: string): ThreadEndpointRef => ({
  environmentId: EnvironmentId.make(environmentId),
  threadId: ThreadId.make(threadId),
});
const origin = ref("origin", "equal-id");
const receiver = ref("receiver", "equal-id");
const link = (overrides: Partial<LinkedThreadLink> = {}): LinkedThreadLink => ({
  linkId: `tl1:${"a".repeat(64)}`,
  kind: "continuation",
  from: origin,
  to: receiver,
  basis: "handoff",
  assertedBy: "user",
  reverse: "unavailable",
  acceptance: "offered",
  handoff: null,
  remote: { title: "Receiver snapshot", environmentLabel: "VPS" },
  createdAt: "2026-10-24T10:00:00Z",
  updatedAt: "2026-10-24T10:00:00Z",
  acceptedAt: null,
  ...overrides,
});
const env = (id: string, label: string, capable = true, phase = "connected") =>
  ({
    environmentId: EnvironmentId.make(id),
    label,
    connection: { phase },
    serverConfig: {
      environment: {
        capabilities: capable ? { linkedThreads: { schemaVersion: "t3.linked-threads/v1" } } : {},
        platform: { machine: "server" },
      },
    },
  }) as EnvironmentPresentation;
const shell = (environmentId: string, id: string, title: string) =>
  ({
    environmentId: EnvironmentId.make(environmentId),
    id: ThreadId.make(id),
    title,
  }) as EnvironmentThreadShell;
let fixtureRegistry: AtomRegistry.AtomRegistry | null = null;
const queryKey = (id: string, subject = origin) => JSON.stringify([id, endpointKey(subject)]);
function seed(
  id: string,
  links: LinkedThreadLink[] = [],
  options: { subject?: ThreadEndpointRef; truncated?: boolean; failure?: boolean } = {},
) {
  const result: AsyncResult.AsyncResult<LinkedThreadsListResult, unknown> = options.failure
    ? AsyncResult.failure<LinkedThreadsListResult, Error>(
        Cause.fail(new Error("synthetic unavailable")),
      )
    : AsyncResult.success({
        environmentId: EnvironmentId.make(id),
        links,
        siblings: [],
        truncated: options.truncated ?? false,
      });
  const key = queryKey(id, options.subject);
  const existing = state.queries.get(key);
  if (existing && fixtureRegistry) fixtureRegistry.set(existing, result);
  else
    state.queries.set(
      key,
      Atom.make<AsyncResult.AsyncResult<LinkedThreadsListResult, unknown>>(result),
    );
}
function setup() {
  fixtureRegistry = null;
  state.queries.clear();
  state.refreshes.clear();
  state.list.mockClear();
  state.command.mockReset();
  state.toast.mockClear();
  state.navigate.mockClear();
  state.copy.mockReset().mockResolvedValue(true);
  state.environments = [
    env("origin", "Laptop"),
    env("receiver", "VPS"),
    env("legacy", "Legacy", false),
    env("saved", "Saved server", true, "offline"),
  ];
  state.shells = [
    shell("origin", "equal-id", "Origin title"),
    shell("receiver", "equal-id", "Live receiver"),
    shell("legacy", "legacy-thread", "Legacy target"),
  ];
  state.command.mockImplementation(async () =>
    AsyncResult.success({ link: link(), outcome: "recorded" }),
  );
  seed("origin");
  seed("receiver");
  const registry = AtomRegistry.make();
  fixtureRegistry = registry;
  return { registry, [Symbol.dispose]: () => registry.dispose() };
}
function Group(props: {
  id: string;
  label: string | null;
  rows: readonly LinkedEntry[];
  expanded: boolean;
  view: ReturnType<typeof useLinkedThreads>;
  subject: ThreadEndpointRef;
}) {
  const [expanded, setExpanded] = useState(props.expanded);
  if (props.rows.length === 0) return null;
  return (
    <div data-linked-group={props.id}>
      {props.label ? (
        <button onClick={() => setExpanded(!expanded)} aria-expanded={expanded}>
          {props.label}
        </button>
      ) : null}
      {expanded ? (
        <ul>
          {props.rows.map((entry) => (
            <LinkedThreadRow
              key={entry.key}
              entry={entry}
              view={props.view}
              subjectEnvironmentId={props.subject.environmentId}
            />
          ))}
        </ul>
      ) : null}
    </div>
  );
}
function Probe(props: { subject?: ThreadEndpointRef; active?: boolean; settled?: string }) {
  const subject = props.subject ?? origin;
  const view = useLinkedThreads(subject, props.active ?? false, props.settled ?? "");
  const [dialog, setDialog] = useState<"link" | "copy" | null>(null);
  const entries = buildLinkedEntries({
    subject,
    related: [],
    active: [],
    previous: [],
    relations: view.relations,
    snapshots: view.snapshots,
  });
  return (
    <>
      {view.visible ? (
        <LinkedThreadsSection
          key={endpointKey(subject)}
          runningCount={0}
          count={entries.length}
          incomplete={view.incomplete}
          continued={view.hasSuccessor}
          onExpand={view.onExpand}
          actions={
            <>
              {view.canLink ? (
                <button onClick={() => setDialog("link")}>Link thread…</button>
              ) : null}
              {view.canCopy ? (
                <button onClick={() => setDialog("copy")}>Copy handoff text…</button>
              ) : null}
            </>
          }
        >
          <LinkedLineageGroups
            entries={entries}
            renderGroup={(group) => (
              <Group key={group.id} {...group} subject={subject} view={view} />
            )}
          />
          {view.errors.map((id) => (
            <p key={id}>Error reading {id}</p>
          ))}
          {view.offline.map((environment) => (
            <p key={environment.environmentId}>{environment.label} may hold more links</p>
          ))}
        </LinkedThreadsSection>
      ) : null}
      {dialog === "link" ? (
        <LinkThreadDialog subject={subject} view={view} onClose={() => setDialog(null)} />
      ) : null}
      {dialog === "copy" ? (
        <CopyHandoffDialog
          subject={subject}
          title="Origin title"
          environmentLabel="Laptop"
          onClose={() => setDialog(null)}
        />
      ) : null}
    </>
  );
}
const button = (container: ParentNode, label: string) =>
  [...container.querySelectorAll("button")].find(
    (node) => node.textContent?.trim() === label || node.getAttribute("aria-label") === label,
  )!;

it("fans out only to connected capable environments, counts a deduped union with errors, and focuses Continuations", async () => {
  using fixture = setup();
  state.environments.push(env("failed", "Failed server"));
  seed("origin", [link()], { truncated: true });
  seed("receiver", [link({ acceptance: "accepted", reverse: "recorded" })]);
  seed("failed", [], { failure: true });
  await using screen = await render(
    <RegistryContext value={fixture.registry}>
      <Probe />
    </RegistryContext>,
    { router: false },
  );
  expect(
    [...new Set(state.list.mock.calls.map(([target]) => target.environmentId))].sort(),
  ).toEqual(["failed", "origin", "receiver"]);
  expect(screen.container.textContent).toContain("Linked (1+)");
  expect(screen.container.querySelector("[data-thread-relationships-panel]")).not.toBeNull();
  expect(screen.container.querySelector("li")).toBeNull();
  await act(() => button(screen.container, "Continued").click());
  expect(document.activeElement?.textContent).toBe("Continuations");
  expect(screen.container.querySelectorAll("li")).toHaveLength(1);
  expect(screen.container.textContent).toContain("Live receiver");
  expect(screen.container.textContent).toContain("accepted");
  expect(screen.container.textContent).toContain("Saved server may hold more links");
  await act(() => button(screen.container, "Open Live receiver on VPS").click());
  expect(state.navigate).toHaveBeenCalledWith({
    to: "/$environmentId/$threadId",
    params: { environmentId: "receiver", threadId: "equal-id" },
  });
});

it("retains remote snapshots offline, disables navigation and reports a one-sided copy", async () => {
  using fixture = setup();
  state.environments = state.environments.map((environment) =>
    environment.environmentId === "receiver"
      ? env("receiver", "VPS", true, "offline")
      : environment,
  );
  state.shells = [shell("origin", "equal-id", "Wrong local title")];
  seed("origin", [link()]);
  await using screen = await render(
    <RegistryContext value={fixture.registry}>
      <Probe />
    </RegistryContext>,
    { router: false },
  );
  await act(() => button(screen.container, "Linked threads").click());
  expect(button(screen.container, "Open Receiver snapshot on VPS").disabled).toBe(true);
  expect(screen.container.textContent).toContain("One-sided");
  expect(screen.container.textContent).not.toContain("Wrong local title");
});

it("refreshes on run settlement, reconnect and stale expansion, with no refresh on unchanged renders", async () => {
  using fixture = setup();
  let clock = 1_800_000_000_000;
  using _now = vi.spyOn(Date, "now").mockImplementation(() => clock);
  const ui = (active: boolean, settled = "") => (
    <RegistryContext value={fixture.registry}>
      <Probe active={active} settled={settled} />
    </RegistryContext>
  );
  await using screen = await render(ui(true), { router: false });
  const revision = () => fixture.registry.get(linkedThreads.refreshAtom(origin));
  const first = revision();
  await screen.rerender(ui(true));
  expect(revision()).toBe(first);
  await screen.rerender(ui(false, "run-1"));
  expect(revision()).toBe(first + 1);
  state.environments = state.environments.map((environment) =>
    environment.environmentId === "saved" ? env("saved", "Saved server") : environment,
  );
  seed("saved");
  await screen.rerender(ui(false, "run-1"));
  expect(revision()).toBe(first + 2);
  clock += 31_000;
  await act(() => button(screen.container, "Linked threads").click());
  expect(revision()).toBe(first + 3);
  await screen.rerender(ui(false, "run-1"));
  expect(revision()).toBe(first + 3);
});

it("keeps legacy environments lineage-only and still reads a capable receiver's reciprocal copy", async () => {
  using fixture = setup();
  state.environments = [env("origin", "Laptop", false), env("receiver", "VPS", false)];
  const ui = (
    <RegistryContext value={fixture.registry}>
      <Probe />
    </RegistryContext>
  );
  await using screen = await render(ui, { router: false });
  expect(screen.container.querySelector("section")).toBeNull();
  expect(state.list).not.toHaveBeenCalled();
  state.environments = [env("origin", "Laptop", false), env("receiver", "VPS")];
  seed("receiver", [link({ remote: { title: "Origin snapshot", environmentLabel: "Laptop" } })]);
  await screen.rerender(
    <RegistryContext value={fixture.registry}>
      <Probe />
    </RegistryContext>,
  );
  expect(screen.container.textContent).toContain("Linked (1)");
  expect(button(screen.container, "Link thread…")).toBeUndefined();
  expect(button(screen.container, "Copy handoff text…")).toBeUndefined();
});

it("links a legacy target locally, reports unavailable mirroring and preserves receiver-only Accept", async () => {
  using fixture = setup();
  await using screen = await render(
    <RegistryContext value={fixture.registry}>
      <Probe />
    </RegistryContext>,
    { router: false },
  );
  await act(() => button(screen.container, "Link thread…").click());
  await page.getByRole("radio", { name: /Legacy target/ }).click();
  await page.getByRole("button", { name: "Link thread", exact: true }).click();
  expect(
    state.command.mock.calls.map(([operation, target]) => [
      operation,
      target.environmentId,
      target.input.reverse,
    ]),
  ).toEqual([
    ["link", "origin", undefined],
    ["link", "origin", "unavailable"],
  ]);
  expect(state.command.mock.calls[0]?.[1].input).toMatchObject({
    kind: "continuation",
    from: origin,
    to: ref("legacy", "legacy-thread"),
  });
  expect(state.toast).toHaveBeenCalledWith(
    expect.objectContaining({ description: expect.stringContaining("Legacy: unavailable") }),
  );
  seed("origin", [link()]);
  await screen.rerender(
    <RegistryContext value={fixture.registry}>
      <Probe />
    </RegistryContext>,
  );
  await act(() => button(screen.container, "Linked threads").click());
  await page.getByRole("button", { name: "Link actions: Continued in" }).click();
  await expect
    .element(page.getByRole("menuitem", { name: "Accept handoff" }))
    .not.toBeInTheDocument();
  await page.getByRole("menuitem", { name: "Unlink", exact: true }).click();
  expect(state.command.mock.calls.some(([operation]) => operation === "unlink")).toBe(true);
});

it("accepts cross-environment handoffs with one receiver RPC, reports success and unions the accepted copy", async () => {
  using fixture = setup();
  const offered = link({ reverse: "recorded" });
  const accepted = link({
    reverse: "recorded",
    acceptance: "accepted",
    acceptedAt: "2026-10-24T11:00:00Z",
    updatedAt: "2026-10-24T11:00:00Z",
  });
  seed("origin", [offered], { subject: receiver });
  seed("receiver", [offered], { subject: receiver });
  state.command.mockResolvedValue(AsyncResult.success({ link: accepted, outcome: "recorded" }));
  await using screen = await render(
    <RegistryContext value={fixture.registry}>
      <Probe subject={receiver} />
    </RegistryContext>,
    { router: false },
  );
  await act(() => button(screen.container, "Linked threads").click());
  await page.getByRole("button", { name: "Link actions: Continues from" }).click();
  await page.getByRole("menuitem", { name: "Accept handoff" }).click();
  expect(state.command.mock.calls).toEqual([
    [
      "accept",
      {
        environmentId: receiver.environmentId,
        input: { from: origin, to: receiver, linkId: offered.linkId },
      },
    ],
  ]);
  expect(state.toast).toHaveBeenCalledWith({
    type: "success",
    title: "Accepted here · origin unchanged",
    description:
      "VPS: accepted · Laptop: origin unchanged (origin copy unchanged; the receiving agent’s t3_thread_link accept updates it through its environment link)",
  });
  await act(() => seed("receiver", [accepted], { subject: receiver }));
  expect(screen.container.querySelector("[data-handoff-acceptance='accepted']")).not.toBeNull();
  expect(screen.container.querySelector("[data-handoff-acceptance='offered']")).toBeNull();
  expect(screen.container.textContent).not.toContain("One-sided");
});

it("keeps an origin-only recorded link offered without a false one-sided marker after receiver acceptance", async () => {
  using fixture = setup();
  state.environments = state.environments.map((environment) =>
    environment.environmentId === "receiver"
      ? env("receiver", "VPS", true, "offline")
      : environment,
  );
  seed("origin", [link({ reverse: "recorded" })]);
  await using screen = await render(
    <RegistryContext value={fixture.registry}>
      <Probe />
    </RegistryContext>,
    { router: false },
  );
  await act(() => button(screen.container, "Linked threads").click());
  expect(screen.container.querySelector("[data-handoff-acceptance='offered']")).not.toBeNull();
  expect(screen.container.querySelector("[data-handoff-acceptance='accepted']")).toBeNull();
  expect(screen.container.textContent).not.toContain("One-sided");
});

it("reports a receiver acceptance transport failure as an error without calling the origin", async () => {
  using fixture = setup();
  seed("origin", [link()], { subject: receiver });
  seed("receiver", [link()], { subject: receiver });
  state.command.mockResolvedValue(
    AsyncResult.failure(Cause.fail(new Error("synthetic receiver disconnected"))),
  );
  await using screen = await render(
    <RegistryContext value={fixture.registry}>
      <Probe subject={receiver} />
    </RegistryContext>,
    { router: false },
  );
  await act(() => button(screen.container, "Linked threads").click());
  await page.getByRole("button", { name: "Link actions: Continues from" }).click();
  await page.getByRole("menuitem", { name: "Accept handoff" }).click();
  expect(state.command).toHaveBeenCalledTimes(1);
  expect(state.command.mock.calls[0]?.[1].environmentId).toBe(receiver.environmentId);
  expect(state.toast).toHaveBeenCalledWith(
    expect.objectContaining({
      type: "error",
      description: expect.stringContaining("synthetic receiver disconnected"),
    }),
  );
  expect(screen.container.querySelector("[data-handoff-acceptance='offered']")).not.toBeNull();
});

it("copies a portable handoff with the shared envelope", async () => {
  using fixture = setup();
  await using screen = await render(
    <RegistryContext value={fixture.registry}>
      <Probe subject={receiver} />
    </RegistryContext>,
    { router: false },
  );
  await act(() => button(screen.container, "Copy handoff text…").click());
  await page.getByRole("textbox", { name: "Goal", exact: true }).fill("Finish the implementation");
  await page.getByRole("textbox", { name: "Scope", exact: true }).fill("UI only");
  await page.getByRole("textbox", { name: "Status", exact: true }).fill("Ready for review");
  await page.getByRole("textbox", { name: /Sources/ }).fill("commit: abc123\nfile: src/app.ts");
  await page.getByRole("button", { name: "Copy handoff text", exact: true }).click();
  expect(state.copy).toHaveBeenCalledWith(
    expect.stringContaining("**Thread handoff** (t3.thread-handoff/v1)"),
    "handoff text",
  );
  const copied = state.copy.mock.calls[0]?.[0] as string;
  expect(copied).toContain("not assigned (portable)");
  expect(copied).toContain("offered, not transferred");
  expect(copied).toContain("environment `receiver`");
  expect(copied).toContain("UI only");
  expect(copied).toContain("file: src/app.ts");
});

it("retains a delegated row when a fork and continuation target the same endpoint", () => {
  const task: LineageRow = {
    kind: "local",
    status: "running",
    row: {
      threadId: receiver.threadId,
      fromThreadId: origin.threadId,
      depth: 1,
      edge: {
        sourceThreadId: origin.threadId,
        targetThreadId: receiver.threadId,
        kind: "subagent",
        status: "running",
      },
    },
  };
  const fork: LineageRow = {
    ...task,
    row: { ...task.row, edge: { ...task.row.edge, kind: "fork" } },
  };
  const subject = ref("origin", "parent");
  const child = ref("origin", "equal-id");
  const relation = {
    endpoint: child,
    label: "Continued in",
    group: "continuations" as const,
    link: link({ from: subject, to: child }),
  };
  const entries = buildLinkedEntries({
    subject,
    active: [task],
    previous: [],
    related: [fork],
    relations: [relation],
    snapshots: new Map(),
  });
  expect(entries).toHaveLength(1);
  expect(entries[0]?.group).toBe("delegated");
  expect(entries[0]?.lineage).toEqual([task]);
  expect(entries[0]?.labels).toEqual(["Delegated task", "Continued in", "Fork"]);
});
