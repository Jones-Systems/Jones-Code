import * as NodeCryptoServices from "@effect/platform-node/NodeCrypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Independently verify raw fragment bytes in the synchronous synthetic consumer fixture.
import * as NodeCrypto from "node:crypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  EnvironmentId,
  MessageId,
  RunId,
  ThreadId,
  TurnItemId,
  OrchestratorMcpThreadReadInput,
  OrchestratorMcpThreadReadResult,
  type OrchestratorMcpThreadTimelineItem,
  type OrchestrationV2TurnItem,
  type OrchestrationV2ProjectedTurnItem,
  THREAD_CONTENT_EVIDENCE_CAPABILITY,
  THREAD_CONTENT_EVIDENCE_VERSION,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as OrchestratorMcpService from "../../mcp/OrchestratorMcpService.ts";
import { OrchestratorToolkit } from "../../mcp/toolkits/orchestrator/tools.ts";
import { liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import * as ThreadLaunchService from "../../orchestration-v2/ThreadLaunchService.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderRegistry from "../../provider/ProviderRegistry.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as RemoteDelegation from "../../peer/RemoteDelegation.ts";
import * as ScheduledTaskService from "../../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../../secrets/SecretRequests.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import { fixture } from "../taskContinuation/testkit.ts";
import { prepareThreadContentEvidence } from "./threadContentEvidence.ts";

const decodeRead = Schema.decodeUnknownSync(OrchestratorMcpThreadReadInput);
const encodeResult = Schema.encodeSync(OrchestratorMcpThreadReadResult);
const decodeResult = Schema.decodeUnknownSync(OrchestratorMcpThreadReadResult);
const decodeToolRead = Schema.decodeUnknownSync(
  OrchestratorToolkit.tools.t3_thread_read.parametersSchema,
);

const optIn = { version: THREAD_CONTENT_EVIDENCE_VERSION } as const;
const environmentId = EnvironmentId.make("env");
const threadId = ThreadId.make("thread");
const itemId = TurnItemId.make("item");
const item = (text: string | null): OrchestratorMcpThreadTimelineItem => ({
  position: 0,
  visibility: "local",
  sourceThreadId: ThreadId.make("source"),
  itemId,
  runId: null,
  messageId: null,
  createdBy: null,
  creationSource: null,
  type: "reasoning",
  status: "completed",
  title: null,
  text,
  textTruncated: false,
  nextTextOffset: null,
  updatedAt: "2026-11-01T00:00:00.000Z",
});
const prepare = (
  items: ReadonlyArray<OrchestratorMcpThreadTimelineItem>,
  request: Partial<OrchestratorMcpThreadReadInput> = {},
  maxChars = 2,
  hasMore = false,
) =>
  prepareThreadContentEvidence({
    items,
    request: { threadId, contentEvidence: optIn, ...request },
    environmentId,
    maxChars,
    hasMore,
  });
const success = (result: ReturnType<typeof prepare>) => {
  if (Result.isFailure(result)) throw result.failure;
  return result.success;
};
const available = (value: OrchestratorMcpThreadTimelineItem) => {
  const evidence = value.contentEvidence;
  if (evidence?.availability !== "available") throw new Error("Expected available evidence");
  return evidence;
};
const failureCode = (result: ReturnType<typeof prepare>) => {
  if (Result.isSuccess(result)) throw new Error("Expected refusal");
  return result.failure.code;
};

// Golden hashes generated independently with Python hashlib/struct and surrogatepass.
const vectors = [
  [
    "",
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "faee29b6f26b111a56283edeeab67719fe8fe3a78adfea5e7c891aec346f5f24",
  ],
  [
    "ASCII",
    "161638584b9de08ac08753c210432cd71e38e9d81e8fbbe27d277334b86627a4",
    "b6b6d344f172a8ad1d906f016a370d9a6e8163f491c2b7b7772e59f8365c7dc4",
  ],
  [
    "café\r\n\0",
    "97be82a6181758bb931c3b4dcecb58ccc8e08150c95d88d816141af1242d7012",
    "b9436f0d99ab07692f57b28f47d4889dfbd50695ce033e0b9cc9fcaf8c842c89",
  ],
  [
    "😀",
    "81c96284b596ed3155a3f0f11da0dd13494dc91618323a3bff4c9701229015cc",
    "13b4e9fed3b572769756a8ca80e40468f7b1c6f39ff539432eadcbab70d24c73",
  ],
  [
    "\ud800x\udfff",
    "0b32ba6db9cd83f6ecd561f760d680baf3cba0cae52d350c42cfd93bd460f4b7",
    "10cb17520750bbef92563fbc090a24177e55b40726ef73d72467375707a26c50",
  ],
  [
    "A😀B\ud800\r\n\0",
    "ad21214d514d054443a280dda6806811950b2c215563d41514c03b79d9a58548",
    "aa096be45ecaef15069b32573ee7a638536a57cc578d5c13e34f6822764f666f",
  ],
] as const;

function assertCoverage(fragments: ReadonlyArray<OrchestratorMcpThreadTimelineItem>) {
  assert.isAbove(fragments.length, 0);
  const first = available(fragments[0]!);
  let offset = 0;
  const bytes: Array<Buffer> = [];
  for (const [index, fragment] of fragments.entries()) {
    const evidence = available(fragment);
    assert.equal(evidence.contentRevision, first.contentRevision);
    assert.equal(evidence.totalUtf16Length, first.totalUtf16Length);
    assert.equal(evidence.contentSha256, first.contentSha256);
    assert.equal(evidence.startOffset, offset);
    assert.equal(fragment.text?.length, evidence.endOffset - offset);
    offset = evidence.endOffset;
    const final = index === fragments.length - 1;
    assert.equal(fragment.nextTextOffset, final ? null : offset);
    assert.equal(fragment.textTruncated, !final);
    if (!final) assert.isAbove(evidence.endOffset, evidence.startOffset);
    bytes.push(Buffer.from(fragment.text!, "utf16le"));
  }
  assert.equal(offset, first.totalUtf16Length);
  assert.equal(
    NodeCrypto.createHash("sha256").update(Buffer.concat(bytes)).digest("hex"),
    first.contentSha256,
  );
}

describe("rendered content evidence", () => {
  it.each(vectors)(
    "matches portable UTF-16LE digest and revision for %j",
    (text, digest, revision) => {
      const result = success(prepare([item(text)]))[0]!;
      const evidence = available(result);
      assert.equal(evidence.contentSha256, digest);
      assert.equal(evidence.contentRevision, revision);
      assert.equal(evidence.totalUtf16Length, text.length);
      assert.equal(evidence.endOffset, Math.min(2, text.length));
    },
  );

  it("binds the entire content and every identity field despite identical timestamps and prefixes", () => {
    const original = item("aa-middle-zz");
    const expectedRevision = available(success(prepare([original]))[0]!).contentRevision;
    const changes: Array<Partial<OrchestratorMcpThreadTimelineItem>> = [
      { text: "aa-MIDDLE-zz" },
      { text: "a" },
      { sourceThreadId: ThreadId.make("foreign") },
      { runId: RunId.make("foreign") },
      { visibility: "inherited" },
      { position: 1 },
      { type: "compaction" },
      { status: "failed" },
      { title: "" },
      { updatedAt: "2026-11-01T00:00:00.001Z" },
      { messageId: MessageId.make("message") },
    ];
    for (const change of changes) {
      assert.equal(
        failureCode(
          prepare([{ ...original, ...change }], {
            itemId,
            textOffset: 2,
            contentEvidence: { ...optIn, expectedRevision },
          }),
        ),
        "content_revision_mismatch",
      );
    }
    for (const identity of [
      { environmentId: EnvironmentId.make("foreign") },
      {
        request: {
          threadId: ThreadId.make("foreign"),
          itemId,
          contentEvidence: { ...optIn, expectedRevision },
        },
      },
    ]) {
      const result = prepareThreadContentEvidence({
        items: [original],
        request: { threadId, itemId, contentEvidence: { ...optIn, expectedRevision } },
        environmentId,
        maxChars: 2,
        hasMore: false,
        ...identity,
      });
      assert.equal(failureCode(result), "content_revision_mismatch");
    }
    const otherItemId = TurnItemId.make("other");
    assert.equal(
      failureCode(
        prepare([{ ...original, itemId: otherItemId }], {
          itemId: otherItemId,
          contentEvidence: { ...optIn, expectedRevision },
        }),
      ),
      "content_revision_mismatch",
    );
    // A revision from a different renderer cannot be reused, even for identical text.
    assert.equal(
      failureCode(
        prepare([original], {
          itemId,
          contentEvidence: {
            ...optIn,
            expectedRevision: "0c971247cd6129ff4faec53c76c3a2284c13762569b6e183de928c164a4cae2f",
          },
        }),
      ),
      "content_revision_mismatch",
    );
  });

  it("assembles exact contiguous slices including a split astral pair and rejects false coverage", () => {
    const original = item("A😀B\ud800\r\n\0");
    const first = success(prepare([original]))[0]!;
    const expectedRevision = available(first).contentRevision;
    const fragments = [first];
    while (fragments.at(-1)!.nextTextOffset !== null) {
      fragments.push(
        success(
          prepare([original], {
            itemId,
            textOffset: fragments.at(-1)!.nextTextOffset!,
            contentEvidence: { ...optIn, expectedRevision },
          }),
        )[0]!,
      );
    }
    assert.equal(first.text, "A\ud83d");
    assert.equal(fragments[1]!.text, "\ude00B");
    assertCoverage(fragments);
    const atEnd = success(
      prepare([original], {
        itemId,
        textOffset: original.text!.length,
        contentEvidence: { ...optIn, expectedRevision },
      }),
    )[0]!;
    assert.equal(atEnd.text, "");
    assert.equal(atEnd.nextTextOffset, null);
    assert.equal(available(atEnd).endOffset, original.text!.length);
    assert.equal(
      failureCode(
        prepare([original], {
          itemId,
          textOffset: original.text!.length + 1,
          contentEvidence: { ...optIn, expectedRevision },
        }),
      ),
      "invalid_request",
    );
    for (const invalid of [
      fragments.slice(1),
      fragments.slice(0, -1),
      [first, ...fragments],
      [...fragments, atEnd],
      [first, ...fragments.slice(2)],
    ])
      assert.throws(() => assertCoverage(invalid));
    const mixed = fragments.map((part, index) =>
      index === 1
        ? { ...part, contentEvidence: { ...available(part), contentRevision: "0".repeat(64) } }
        : part,
    );
    assert.throws(() => assertCoverage(mixed));
    const long = item("x".repeat(50_001));
    const capped = success(prepare([long], {}, 50_000))[0]!;
    assert.equal(capped.nextTextOffset, 50_000);
    assert.equal(available(capped).endOffset, 50_000);
  });

  it("keeps null compaction, empty and whitespace distinct, and bounds added hashing", () => {
    const cap = THREAD_CONTENT_EVIDENCE_CAPABILITY.maxRenderedUtf16Length;
    const inputs = [
      { ...item(null), type: "compaction" },
      item(""),
      item(" "),
      item("x".repeat(cap + 1)),
      item("a".repeat(cap)),
      item("b".repeat(cap - 1)),
      item("c"),
      item("d"),
      item(""),
    ];
    const result = success(prepare(inputs));
    assert.equal(result[0]!.text, null);
    assert.equal(result[0]!.contentEvidence?.availability, "unavailable");
    assert.equal(available(result[1]!).totalUtf16Length, 0);
    assert.equal(available(result[2]!).totalUtf16Length, 1);
    assert.deepEqual(
      result.map((entry) =>
        entry.contentEvidence?.availability === "unavailable"
          ? entry.contentEvidence.reason
          : "available",
      ),
      [
        "source_content_null",
        "available",
        "available",
        "item_hash_limit",
        "available",
        "available",
        "page_hash_budget",
        "page_hash_budget",
        "available",
      ],
    );
    for (const text of [null, "x".repeat(cap + 1)]) {
      assert.equal(
        failureCode(
          prepare([item(text)], {
            itemId,
            contentEvidence: { ...optIn, expectedRevision: "0".repeat(64) },
          }),
        ),
        "content_evidence_unavailable",
      );
    }
    const legacy = success(prepare(inputs, { contentEvidence: undefined }));
    assert.deepEqual<ReadonlyArray<OrchestratorMcpThreadTimelineItem>>(
      result.map(({ contentEvidence: _evidence, ...rest }) => rest),
      legacy,
    );
  });

  it("refuses ambiguous selections, invalid selectors and offsets without changing legacy behavior", () => {
    for (const rows of [
      [],
      [item("ok"), { ...item("ok"), sourceThreadId: ThreadId.make("other") }],
    ]) {
      assert.equal(failureCode(prepare(rows, { itemId })), "invalid_request");
    }
    assert.equal(failureCode(prepare([item("ok")], { itemId }, 2, true)), "invalid_request");
    for (const request of [
      { itemId, textOffset: 1 },
      { itemId, afterPosition: 0 },
      { textOffset: 1 },
      { itemId, textOffset: -1 },
      { itemId, textOffset: 0.5 },
      { contentEvidence: { ...optIn, expectedRevision: "0".repeat(64) } },
    ])
      assert.equal(failureCode(prepare([item("ok")], request)), "invalid_request");
    assert.equal(
      success(prepare([item("ok")], { itemId, textOffset: 50, contentEvidence: undefined }))[0]!
        .text,
      "",
    );
    assert.equal(
      success(prepare([item("okay")], { textOffset: 50, contentEvidence: undefined }))[0]!.text,
      "ok",
    );
  });
});

const serviceLayer = (
  management: Partial<ThreadManagementService.ThreadManagementService["Service"]>,
) =>
  OrchestratorMcpService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        NodeServices.layer,
        NodeCryptoServices.layer,
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          readRemoteTaskObservation: () => Effect.undefined,
          ...management,
        }),
        Layer.mock(ThreadLaunchService.ThreadLaunchService)({}),
        Layer.mock(GitVcsDriver.GitVcsDriver)({}),
        ServerSettings.layerTest(),
        Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([]),
        }),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(RemoteDelegation.RemoteDelegation)({}),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
      ),
    ),
  );

function readFixture() {
  const f = fixture();
  const primary: OrchestrationV2TurnItem = {
    id: itemId,
    threadId: f.child.thread.id,
    runId: f.run.id,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "completed",
    title: null,
    startedAt: f.now,
    completedAt: f.now,
    updatedAt: f.now,
    type: "assistant_message",
    messageId: MessageId.make("result"),
    text: "aa-middle-zz",
    streaming: false,
  };
  const parent = {
    ...f.parent,
    contextTransfers: [
      {
        ...f.child.contextTransfers[0]!,
        type: "subagent_result" as const,
        sourceThreadId: f.child.thread.id,
        targetThreadId: f.parent.thread.id,
        sourcePoint: { threadId: f.child.thread.id, runId: f.run.id },
      },
    ],
  };
  let rows: ReadonlyArray<OrchestrationV2ProjectedTurnItem> = [
    {
      position: 7,
      visibility: "local",
      sourceThreadId: f.child.thread.id,
      sourceItemId: itemId,
      item: primary,
    },
  ];
  const child = { ...f.child, turnItems: [primary] };
  let hasMore = false;
  let acknowledgements = 0;
  const selections: Array<unknown> = [];
  const layer = serviceLayer({
    getThreadRecords: (id) => Effect.succeed(id === parent.thread.id ? parent : child),
    getThreadShell: () => Effect.succeed(liveThreadShell(child.thread.id, { activeRunId: null })),
    getProjectThreadRecords: () => Effect.succeed(child),
    getTimelinePage: (_id, selection) =>
      Effect.sync(() => {
        selections.push(selection);
        return { items: rows, hasMore, totalItems: 9 };
      }),
    dispatch: (command) =>
      Effect.sync(() => {
        assert.equal(command.type, "delegated_task.completion-delivery.acknowledge");
        acknowledgements++;
        return { sequence: 1, storedEvents: [] };
      }),
  });
  return {
    ...f,
    layer,
    primary,
    selections,
    get acknowledgements() {
      return acknowledgements;
    },
    setRows(value: typeof rows) {
      rows = value;
    },
    setHasMore(value: boolean) {
      hasMore = value;
    },
    row: () => rows[0]!,
  };
}

describe("native read service content evidence", () => {
  it.effect(
    "refuses stale revisions and ambiguity before acknowledging, preserving successful legacy ack",
    () =>
      Effect.gen(function* () {
        const f = readFixture();
        yield* Effect.gen(function* () {
          const service = yield* OrchestratorMcpService.OrchestratorMcpService;
          const request = {
            threadId: f.child.thread.id,
            itemId,
            contentEvidence: optIn,
            maxCharsPerItem: 2,
          };
          const first = yield* service.readThread(f.scope, request);
          const expectedRevision = available(first.items[0]!).contentRevision;
          assert.equal(f.acknowledgements, 0);
          f.setRows([{ ...f.row(), item: { ...f.primary, text: "aa-MIDDLE-zz" } }]);
          const stale = yield* service
            .readThread(f.scope, {
              ...request,
              maxCharsPerItem: 50,
              contentEvidence: { ...optIn, expectedRevision },
            })
            .pipe(Effect.flip);
          assert.equal(stale.code, "content_revision_mismatch");
          assert.equal(f.acknowledgements, 0);
          f.setRows([f.row(), { ...f.row(), sourceThreadId: ThreadId.make("other-source") }]);
          assert.equal(
            (yield* service.readThread(f.scope, request).pipe(Effect.flip)).code,
            "invalid_request",
          );
          assert.equal(f.acknowledgements, 0);
          f.setRows([{ ...f.row(), item: f.primary }]);
          const legacy = yield* service.readThread(f.scope, {
            threadId: f.child.thread.id,
            maxCharsPerItem: 50,
          });
          assert.equal(f.acknowledgements, 1);
          assert.isUndefined(legacy.items[0]!.contentEvidence);
          assert.equal(legacy.items[0]!.text, "aa-middle-zz");
          const current = yield* service.readThread(f.scope, {
            ...request,
            maxCharsPerItem: 50,
            contentEvidence: { ...optIn, expectedRevision },
          });
          assert.equal(f.acknowledgements, 2);
          assert.deepEqual<ReadonlyArray<OrchestratorMcpThreadTimelineItem>>(
            current.items.map(({ contentEvidence: _e, ...value }) => value),
            legacy.items,
          );
        }).pipe(Effect.provide(f.layer));
      }),
  );

  it.effect("refuses missing content and invalid ranges before any result ack", () =>
    Effect.gen(function* () {
      const f = readFixture();
      yield* Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const first = yield* service.readThread(f.scope, {
          threadId: f.child.thread.id,
          itemId,
          maxCharsPerItem: 2,
          contentEvidence: optIn,
        });
        const expectedRevision = available(first.items[0]!).contentRevision;
        const request = {
          threadId: f.child.thread.id,
          itemId,
          maxCharsPerItem: 50,
          contentEvidence: { ...optIn, expectedRevision },
        };
        for (const selection of [
          { ...request, textOffset: 100 },
          { ...request, afterPosition: 0 },
        ]) {
          assert.equal(
            (yield* service.readThread(f.scope, selection).pipe(Effect.flip)).code,
            "invalid_request",
          );
        }
        f.setHasMore(true);
        assert.equal(
          (yield* service.readThread(f.scope, request).pipe(Effect.flip)).code,
          "invalid_request",
        );
        f.setHasMore(false);
        f.setRows([{ ...f.row(), item: { ...f.primary, type: "compaction", driver: null } }]);
        assert.equal(
          (yield* service.readThread(f.scope, { ...request, textOffset: 1 }).pipe(Effect.flip))
            .code,
          "content_evidence_unavailable",
        );
        f.setRows([{ ...f.row(), item: { ...f.primary, text: "x".repeat(4_000_001) } }]);
        assert.equal(
          (yield* service.readThread(f.scope, request).pipe(Effect.flip)).code,
          "content_evidence_unavailable",
        );
        assert.equal(f.acknowledgements, 0);
      }).pipe(Effect.provide(f.layer));
    }),
  );

  it.effect(
    "advertises and serializes evidence, preserves native inventory, and renders each value once",
    () =>
      Effect.gen(function* () {
        const f = readFixture();
        let renders = 0;
        const compaction: OrchestrationV2TurnItem = {
          ...f.primary,
          id: TurnItemId.make("compaction"),
          type: "compaction",
          driver: null,
          get summary() {
            renders++;
            return "summary\r\n😀";
          },
        };
        f.setRows([
          f.row(),
          { ...f.row(), position: 8, sourceItemId: compaction.id, item: compaction },
        ]);
        f.setHasMore(true);
        yield* Effect.gen(function* () {
          const service = yield* OrchestratorMcpService.OrchestratorMcpService;
          const capabilities = yield* service.capabilities(f.scope, {});
          assert.deepEqual(
            capabilities.features.threadContentEvidence,
            THREAD_CONTENT_EVIDENCE_CAPABILITY,
          );
          const input = decodeRead({
            threadId: f.child.thread.id,
            environmentId: "remote-hint",
            contentEvidence: optIn,
            view: "activity",
            limit: 2,
            runLimit: 1,
            maxCharsPerItem: 2,
            afterPosition: 6,
          });
          const result = yield* service.readThread(f.scope, input);
          assert.equal(renders, 1);
          assert.equal(result.nextPosition, 8);
          assert.equal(result.hasMore, true);
          assert.equal(result.thread.itemCount, 9);
          assert.equal(result.recentRuns.length, 1);
          assert.deepEqual(
            result.items.map((entry) => entry.status),
            ["completed", "completed"],
          );
          assert.equal(available(result.items[1]!).environmentId, f.scope.environmentId);
          assert.equal(available(result.items[1]!).threadId, f.child.thread.id);
          assert.deepEqual(f.selections, [{ afterPosition: 6, limit: 2, view: "activity" }]);
          const encoded = encodeResult(result);
          assert.deepEqual(decodeResult(JSON.parse(JSON.stringify(encoded))), result);
          assert.deepEqual(decodeToolRead(input), input);
          assert.equal(f.acknowledgements, 0);
        }).pipe(Effect.provide(f.layer));
      }),
  );

  it.effect(
    "keeps legacy surrogate-safe paging alongside negotiated revision-bound UTF-16 slices",
    () =>
      Effect.gen(function* () {
        const f = readFixture();
        f.setRows([{ ...f.row(), item: { ...f.primary, text: "😀X" } }]);
        yield* Effect.gen(function* () {
          const service = yield* OrchestratorMcpService.OrchestratorMcpService;
          const request = { threadId: f.child.thread.id, itemId, maxCharsPerItem: 1 };
          const legacy = yield* service.readThread(f.scope, request);
          assert.equal(legacy.items[0]!.text, "😀");
          assert.equal(legacy.items[0]!.nextTextOffset, 2);
          assert.isTrue(legacy.items[0]!.textTruncated);
          assert.isUndefined(legacy.items[0]!.contentEvidence);
          assert.equal(f.acknowledgements, 0);
          const first = yield* service.readThread(f.scope, { ...request, contentEvidence: optIn });
          const original = available(first.items[0]!);
          assert.equal(
            original.contentSha256,
            "be6dd4279c40d9e26a89e012d3c01c9164d1a6484693464b8efd5dbcce7ecedc",
          );
          assert.equal(first.items[0]!.text, "\ud83d");
          assert.equal(first.items[0]!.nextTextOffset, 1);
          const second = yield* service.readThread(f.scope, {
            ...request,
            textOffset: 1,
            contentEvidence: { ...optIn, expectedRevision: original.contentRevision },
          });
          assert.equal(second.items[0]!.text, "\ude00");
          assert.equal(second.items[0]!.nextTextOffset, 2);
          assert.equal(f.acknowledgements, 0);
          const last = yield* service.readThread(f.scope, {
            ...request,
            textOffset: 2,
            contentEvidence: { ...optIn, expectedRevision: original.contentRevision },
          });
          assert.equal(last.items[0]!.text, "X");
          assertCoverage([first.items[0]!, second.items[0]!, last.items[0]!]);
          // Continuations do not acknowledge delivery; only the complete offset-zero read does.
          assert.equal(f.acknowledgements, 0);
          const full = yield* service.readThread(f.scope, {
            ...request,
            maxCharsPerItem: 3,
            contentEvidence: { ...optIn, expectedRevision: original.contentRevision },
          });
          assert.equal(full.items[0]!.text, "😀X");
          assert.isFalse(full.items[0]!.textTruncated);
          assert.equal(f.acknowledgements, 1);
        }).pipe(Effect.provide(f.layer));
      }),
  );
});
