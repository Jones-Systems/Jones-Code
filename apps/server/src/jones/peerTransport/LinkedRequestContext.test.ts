import { describe, expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
  type LinkedRequestContext,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type { McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import { encodeLinkedMessageOrigin, withLinkedMessageOrigin } from "./LinkedMessageOrigin.ts";
import {
  IncomingLinkedRequestContext,
  LinkedRequestSentAtOverride,
  PEER_REQUEST_HEADER_BUDGET,
  REQUEST_CONTEXT_MAX_ACCEPTED_CHARS,
  REQUEST_CONTEXT_MAX_SENT_CHARS,
  UNCONTROLLED_HEADER_ALLOWANCE,
  bindLinkedRequestContext,
  encodeLinkedRequestContext,
  fitLinkedRequestContext,
  normalizeAdvisoryTitle,
  readBoundLinkedRequestContext,
  requestHeaderBytes,
} from "./LinkedRequestContext.ts";

const scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("receiver"),
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
  requestNamespace: "client:session-a",
  client: { sessionId: "session-a", label: "Synthetic peer", access: "auto", linked: true },
  thread: undefined,
};
const base: LinkedRequestContext = {
  schemaVersion: "t3.linked-request-context/v1",
  sentAt: "2026-10-26T12:34:56.789Z",
};
const context: LinkedRequestContext = {
  ...base,
  thread: { threadId: "thread-a", title: "Thread 👩‍💻" },
  parent: { threadId: "parent", title: "Parent", environmentId: "source" },
  project: { projectId: "project", title: "Project", icon: { kind: "emoji", emoji: "🧪" } },
};
const raw = (value: unknown) => Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
const originScope = withLinkedMessageOrigin(
  scope,
  encodeLinkedMessageOrigin({
    schemaVersion: "t3.linked-message-origin/v1",
    environmentId: "source",
    thread: { threadId: "thread-a", title: "Origin" },
  }),
)!;

describe("linked request context boundary (K0-2)", () => {
  it("binds only display data to the actual authenticated session and strips unknown fields", () => {
    const bound = bindLinkedRequestContext(
      scope,
      raw({
        ...context,
        credentials: "discard",
        project: { ...context.project, workspaceRoot: "discard" },
      }),
    );
    expect(bound).toEqual({ context, linkSessionId: "session-a" });
    expect(scope.client?.remoteOrigin).toBeUndefined();
    expect(scope.thread).toBeUndefined();
  });

  it("ignores headers outside linked, unscoped client requests", () => {
    const header = encodeLinkedRequestContext(context);
    const thread = {
      threadId: ThreadId.make("local"),
      providerSessionId: "provider",
      providerInstanceId: ProviderInstanceId.make("codex"),
    };
    for (const other of [
      { ...scope, client: undefined },
      { ...scope, client: { ...scope.client!, linked: false } },
      { ...scope, client: { sessionId: "ordinary", label: "Client", access: "auto" as const } },
      { ...scope, thread },
      { ...scope, thread, client: undefined },
    ])
      expect(bindLinkedRequestContext(other, header)).toBeUndefined();
  });

  it.each([
    undefined,
    "",
    "!invalid!",
    "e31",
    `${raw(base)}=`,
    `${raw(base)},${raw(base)}`,
    Buffer.from([0xff]).toString("base64url"),
    Buffer.from('{"sentAt":"\ufffd"', "utf8").toString("base64url"),
    raw([]),
    raw(null),
    raw("text"),
    raw({ ...base, schemaVersion: "future" }),
    raw({ ...base, sentAt: "not-a-time" }),
    raw({ ...base, thread: { threadId: "" } }),
    raw({ ...base, parent: { threadId: "parent", title: "\u202eunsafe" } }),
    raw({ ...base, project: { projectId: "x".repeat(257) } }),
    "a".repeat(REQUEST_CONTEXT_MAX_ACCEPTED_CHARS + 1),
  ])("leniently drops malformed/adversarial context %j", (header) => {
    expect(bindLinkedRequestContext(scope, header)).toBeUndefined();
  });

  it("accepts exactly 4096 canonical characters and drops a larger valid encoding", () => {
    const unpadded = { ...base, padding: "" };
    const padding = "p".repeat(3072 - Buffer.byteLength(JSON.stringify(unpadded), "utf8"));
    const header = raw({ ...unpadded, padding });
    expect(header).toHaveLength(4096);
    expect(bindLinkedRequestContext(scope, header)?.context).toEqual(base);
    expect(
      bindLinkedRequestContext(scope, raw({ ...unpadded, padding: `${padding}p` })),
    ).toBeUndefined();
  });

  it("retries once without only the icon, retaining every other valid context part", () => {
    const badIcon = {
      ...context,
      project: { ...context.project, icon: { kind: "emoji", emoji: 123 } },
    };
    const { icon: _icon, ...project } = context.project!;
    expect(bindLinkedRequestContext(scope, raw(badIcon))?.context).toEqual({ ...context, project });
    expect(
      bindLinkedRequestContext(
        scope,
        raw({
          ...badIcon,
          project: { ...badIcon.project, projectId: "" },
        }),
      ),
    ).toBeUndefined();
    expect(
      bindLinkedRequestContext(
        scope,
        raw({
          ...context,
          project: { ...context.project, icon: { kind: "new-kind", data: 1 } },
        }),
      )?.context.project?.icon,
    ).toBeNull();
    expect(
      bindLinkedRequestContext(
        scope,
        raw({
          ...context,
          project: { ...context.project, icon: null },
        }),
      )?.context.project?.icon,
    ).toBeNull();
  });

  it("requires matching origin/context threads unless the context is base-only", () => {
    expect(
      bindLinkedRequestContext(originScope, encodeLinkedRequestContext(context))?.context,
    ).toEqual(context);
    expect(
      bindLinkedRequestContext(originScope, encodeLinkedRequestContext(base))?.context,
    ).toEqual(base);
    for (const mismatch of [
      { ...context, thread: { threadId: "thread-b" } },
      { ...base, parent: context.parent! },
      { ...base, project: context.project! },
    ])
      expect(
        bindLinkedRequestContext(originScope, encodeLinkedRequestContext(mismatch)),
      ).toBeUndefined();
    const noOriginThread = withLinkedMessageOrigin(
      scope,
      encodeLinkedMessageOrigin({
        schemaVersion: "t3.linked-message-origin/v1",
        environmentId: "source",
      }),
    )!;
    expect(
      bindLinkedRequestContext(noOriginThread, encodeLinkedRequestContext(context))?.context,
    ).toEqual(context);
  });

  it.effect(
    "reads the Reference at call time and rejects foreign sessions or unlinked callers",
    () =>
      Effect.gen(function* () {
        const read = readBoundLinkedRequestContext(scope);
        expect(yield* read).toBeUndefined();
        expect(yield* LinkedRequestSentAtOverride).toBeUndefined();
        for (const linkSessionId of ["session-a", "session-b"]) {
          const bound = { context, linkSessionId };
          expect(
            yield* read.pipe(Effect.provideService(IncomingLinkedRequestContext, bound)),
          ).toEqual(linkSessionId === "session-a" ? bound : undefined);
        }
        expect(
          yield* readBoundLinkedRequestContext({
            ...scope,
            client: { ...scope.client!, linked: false },
          }).pipe(
            Effect.provideService(IncomingLinkedRequestContext, {
              context,
              linkSessionId: "session-a",
            }),
          ),
        ).toBeUndefined();
        expect(
          yield* readBoundLinkedRequestContext({ ...scope, client: undefined }).pipe(
            Effect.provideService(IncomingLinkedRequestContext, {
              context,
              linkSessionId: "session-a",
            }),
          ),
        ).toBeUndefined();
        expect(
          yield* readBoundLinkedRequestContext({
            ...scope,
            thread: {
              threadId: ThreadId.make("local"),
              providerSessionId: "provider",
              providerInstanceId: ProviderInstanceId.make("codex"),
            },
          }).pipe(
            Effect.provideService(IncomingLinkedRequestContext, {
              context,
              linkSessionId: "session-a",
            }),
          ),
        ).toBeUndefined();
        expect(yield* read).toBeUndefined();
      }),
  );

  it("fits exactly, then drops optional fields in the frozen order without mutating the input", () => {
    const full: LinkedRequestContext = {
      ...base,
      thread: { threadId: "t".repeat(256), title: "T".repeat(120) },
      parent: { threadId: "p".repeat(256), title: "P".repeat(120), environmentId: "e".repeat(256) },
      project: {
        projectId: "j".repeat(256),
        title: "J".repeat(120),
        icon: { kind: "lucide", name: "folder-code", color: "blue" },
      },
    };
    const original = JSON.stringify(full);
    const { icon: _icon, ...projectWithoutIcon } = full.project!;
    const { title: _parentTitle, ...parentWithoutTitle } = full.parent!;
    const { title: _projectTitle, ...projectWithoutTitle } = projectWithoutIcon;
    const { title: _threadTitle, ...threadWithoutTitle } = full.thread!;
    const stages: ReadonlyArray<LinkedRequestContext> = [
      full,
      { ...full, project: projectWithoutIcon },
      { ...full, project: projectWithoutIcon, parent: parentWithoutTitle },
      { ...full, project: projectWithoutTitle, parent: parentWithoutTitle },
      { ...base, thread: full.thread!, project: projectWithoutTitle },
      { ...base, thread: full.thread! },
      { ...base, thread: threadWithoutTitle },
      base,
    ];
    for (const [index, stage] of stages.entries()) {
      const encoded = encodeLinkedRequestContext(stage);
      expect(fitLinkedRequestContext(full, encoded.length)).toBe(encoded);
      expect(fitLinkedRequestContext(full, encoded.length - 1)).toBe(
        stages[index + 1] === undefined
          ? undefined
          : encodeLinkedRequestContext(stages[index + 1]!),
      );
    }
    expect(
      fitLinkedRequestContext(full, REQUEST_CONTEXT_MAX_SENT_CHARS)!.length,
    ).toBeLessThanOrEqual(2048);
    expect(fitLinkedRequestContext(base, 0)).toBeUndefined();
    expect(JSON.stringify(full)).toBe(original);
  });

  it("normalizes unsafe titles while preserving joiners and avoiding cut surrogate pairs", () => {
    expect(normalizeAdvisoryTitle(undefined)).toBeUndefined();
    expect(normalizeAdvisoryTitle(null)).toBeUndefined();
    expect(normalizeAdvisoryTitle(" \n\u202e\ud800 ")).toBeUndefined();
    expect(normalizeAdvisoryTitle("  A\t\u202e\u2028 B  \n C ")).toBe("A B C");
    expect(normalizeAdvisoryTitle("👩‍💻 می‌روم")).toBe("👩‍💻 می‌روم");
    expect(normalizeAdvisoryTitle("a\u202e\u200dب\u200cت")).toBe("a \u200dب\u200cت");
    expect(normalizeAdvisoryTitle("a".repeat(120))).toHaveLength(120);
    expect(normalizeAdvisoryTitle(`${"a".repeat(118)}😀rest`)).toBe(`${"a".repeat(118)}…`);
    expect(normalizeAdvisoryTitle(`${"a".repeat(117)}😀rest`)).toBe(`${"a".repeat(117)}😀…`);
    expect(normalizeAdvisoryTitle(`${"a".repeat(118)} rest`)).toBe(`${"a".repeat(118)}…`);
  });

  it("counts actual UTF-8 bytes plus colon/space/CRLF per header", () => {
    expect(requestHeaderBytes({})).toBe(0);
    expect(requestHeaderBytes({ x: "é😀", accept: "json" })).toBe(25);
    expect(PEER_REQUEST_HEADER_BUDGET).toBe(8192);
    expect(UNCONTROLLED_HEADER_ALLOWANCE).toBe(1024);
    expect(REQUEST_CONTEXT_MAX_SENT_CHARS).toBe(2048);
    expect(REQUEST_CONTEXT_MAX_ACCEPTED_CHARS).toBe(4096);
  });
});
