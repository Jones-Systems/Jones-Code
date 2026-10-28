import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";
import { loadScenario } from "./scenario-api.mjs";
import {
  assertQueueState,
  assertDeliveries,
  assertReplayScope,
  assertControlUnchanged,
  assertDraft,
  editAndCancel,
  queueRpc,
  resolveQueueCase,
  assertFullQueueEvidence,
  openProviderSettings,
  withQueueCleanup,
  selectQueueTarget,
} from "./scenarios/queue-edit-cancel.mjs";

const fixture = {
  threadId: "target",
  m1: "JE06-unit-M1",
  m2Original: "JE06-unit-M2-original",
  m2Edited: "JE06-unit-M2-edited",
  m3: "JE06-unit-M3",
  draft: "JE06-unit-D",
  ids: [1, 2, 3].map((i) => ({ messageId: `m${i}`, runId: `r${i}` })),
};
function state(phase) {
  const complete = phase === "complete",
    queued = phase === "queued";
  return {
    thread: { id: "target" },
    messages: [fixture.m1, queued ? fixture.m2Original : fixture.m2Edited, fixture.m3]
      .map((text, i) => ({ id: `m${i + 1}`, role: "user", text }))
      .concat(
        complete
          ? [fixture.m1, fixture.m2Edited].map((text, i) => ({
              id: `a${i}`,
              role: "assistant",
              text: `ACK ${text}`,
            }))
          : [],
      ),
    runs: [
      complete ? "completed" : "running",
      complete ? "completed" : "queued",
      queued ? "queued" : "cancelled",
    ].map((status, i) => ({ id: `r${i + 1}`, userMessageId: `m${i + 1}`, status })),
    attempts: [{ id: "a3", runId: "r3", status: queued ? "pending" : "cancelled" }],
    nodes: [{ runId: "r3", status: queued ? "pending" : "cancelled" }],
    visibleTurnItems: [],
    providerTurns: complete ? [{ id: "p1" }, { id: "p2" }] : [{ id: "p1" }],
  };
}
const deliveries = () =>
  [fixture.m1, fixture.m2Edited].flatMap((marker, i) => [
    { event: "delivery", marker, ordinal: i + 1 },
    { event: "complete", marker, reason: "end_turn", ordinal: i + 1 },
  ]);

NodeTest.test("queue scenario loads through the native harness", async () => {
  const loaded = await loadScenario(
    NodeURL.fileURLToPath(new URL("./scenarios/queue-edit-cancel.mjs", import.meta.url)),
  );
  NodeAssert.equal(loaded.run.constructor.name, "AsyncFunction");
  NodeAssert.match(loaded.identity.sha256, /^[a-f0-9]{64}$/);
});
NodeTest.test("queue, edited/cancelled, and completed states retain durable identities", () => {
  for (const phase of ["queued", "edited", "complete"])
    NodeAssert.deepEqual(assertQueueState(state(phase), fixture, phase), fixture.ids);
  assertDeliveries(deliveries(), fixture, true);
});
NodeTest.test("empty, stale, duplicate and wrong-thread projections cannot pass", () => {
  const changes = [
    (p) => {
      p.messages = [];
    },
    (p) => {
      p.runs = [];
    },
    (p) => {
      p.thread.id = "control";
    },
    (p) => {
      p.messages.push({ ...p.messages[0], id: "duplicate" });
    },
    (p) => {
      p.runs.push({ ...p.runs[0], id: "duplicate" });
    },
    (p) => {
      p.messages[1].text = fixture.m2Original;
    },
    (p) => {
      p.messages[1].id = "replaced";
      p.runs[1].userMessageId = "replaced";
    },
    (p) => {
      p.runs[1].id = "replacement-run";
    },
    (p) => {
      p.runs[2].status = "completed";
    },
    (p) => {
      p.attempts = [];
    },
    (p) => {
      p.nodes[0].status = "running";
    },
    (p) => {
      p.visibleTurnItems.push({ item: { type: "user_message", runId: "r3" } });
    },
    (p) => {
      p.providerTurns.push({ id: "p3" });
    },
    (p) => {
      p.messages.pop();
    },
  ];
  for (const mutate of changes) {
    const value = state("complete");
    mutate(value);
    NodeAssert.throws(() => assertQueueState(value, fixture, "complete"));
  }
});
NodeTest.test(
  "ledger rejects duplicate, unedited, cancelled, unsent, missing and incomplete deliveries",
  () => {
    for (const marker of [fixture.m1, fixture.m2Original, fixture.m3, fixture.draft]) {
      const rows = deliveries();
      rows.push({ event: "delivery", marker });
      NodeAssert.throws(() => assertDeliveries(rows, fixture, true), /expected deliveries/);
    }
    const stale = deliveries();
    stale[2].marker = fixture.m2Original;
    NodeAssert.throws(() => assertDeliveries(stale, fixture, true));
    NodeAssert.throws(() => assertDeliveries([], fixture, true));
    NodeAssert.throws(
      () =>
        assertDeliveries(
          deliveries().filter((r) => r.event !== "complete"),
          fixture,
          true,
        ),
      /terminal/,
    );
    NodeAssert.throws(() => assertDeliveries(deliveries(), fixture, false));
  },
);
NodeTest.test(
  "native replay preserves all captured fields and refuses other commands, targets and IDs",
  async () => {
    const captured = {
      type: "message.dispatch",
      commandId: "command",
      messageId: "m2",
      threadId: fixture.threadId,
      text: fixture.m2Original,
      dispatchMode: "queue_after_active",
    };
    NodeAssert.deepEqual(assertReplayScope(structuredClone(captured), captured, fixture), captured);
    for (const patch of [
      { type: "queued-run.cancel" },
      { threadId: "control" },
      { commandId: "replacement" },
      { messageId: "replacement" },
      { text: fixture.m2Edited },
      { dispatchMode: "immediate" },
    ]) {
      NodeAssert.throws(() => assertReplayScope({ ...captured, ...patch }, captured, fixture));
    }
    const endpoint = { httpBaseUrl: "http://127.0.0.1:1", wsBaseUrl: "ws://127.0.0.1:1" };
    await NodeAssert.rejects(queueRpc({ endpoint, action: "delete" }), /Unsupported/);
    await NodeAssert.rejects(
      queueRpc({ endpoint, action: "replay", threadId: "control", replay: captured, captured }),
      /scope/,
    );
    await NodeAssert.rejects(
      queueRpc({
        endpoint: { ...endpoint, httpBaseUrl: "http://example.com" },
        action: "providers",
      }),
      /loopback/,
    );
  },
);
NodeTest.test("control changes and lost ordinary drafts fail their own oracles", () => {
  assertControlUnchanged({ threads: [{ id: "control" }] }, { threads: [{ id: "control" }] });
  NodeAssert.throws(
    () => assertControlUnchanged({ messages: [] }, { messages: [{ text: "unexpected" }] }),
    /byte-stable/,
  );
  assertDraft(fixture.draft, fixture.draft);
  NodeAssert.throws(() => assertDraft(fixture.m2Edited, fixture.draft), /restored/);
});

function editContext(
  skip = false,
  { omitCancelled = false, cancelDurably = true, advanceClock } = {},
) {
  const projection = state("queued");
  let draft = "",
    savedDraft = "",
    removed = false;
  const actions = [];
  const editor = {
    fill: async (text) => {
      draft = text;
      actions.push(["fill", text]);
    },
    innerText: async () => draft,
  };
  const button = (name) => ({
    click: async () => {
      actions.push(["click", name]);
      if (name === "Edit queued message") {
        savedDraft = draft;
        draft = fixture.m2Original;
      }
      if (name === "Update queued message") {
        projection.messages[1].text = draft;
        draft = savedDraft;
      }
      if (name === "Remove queued message") {
        removed = true;
        if (cancelDurably) {
          projection.runs[2].status = "cancelled";
          projection.attempts[0].status = "cancelled";
          projection.nodes[0].status = "cancelled";
        }
      }
    },
  });
  const row = {
    filter: () => row,
    getByRole: (_, { name }) => button(name),
    waitFor: async ({ state }) => {
      NodeAssert.equal(state, "hidden");
      NodeAssert.equal(removed, true, "The selected queue row must disappear");
    },
  };
  const ctx = {
    assert: NodeAssert.ok,
    page: {
      getByTestId: () => editor,
      getByRole: (_, { name }) => button(name),
      locator: () => row,
    },
  };
  const readBounded = async () => {
    advanceClock?.();
    if (!omitCancelled || !removed) return projection;
    return {
      ...projection,
      messages: projection.messages.filter((message) => message.id !== "m3"),
      runs: projection.runs.filter((run) => run.id !== "r3"),
      attempts: [],
      nodes: [],
    };
  };
  return {
    run: () =>
      editAndCancel(ctx, fixture, readBounded, {
        skipEditGesture: skip,
        readDurable: async () => {
          advanceClock?.();
          return projection;
        },
      }),
    actions,
    projection,
    readBounded,
  };
}
NodeTest.test(
  "bounded omission of M3 passes only with its durable cancellation",
  async (context) => {
    context.mock.timers.enable({ apis: ["Date"], now: 0 });
    const scenario = editContext(false, {
      omitCancelled: true,
      advanceClock: () => context.mock.timers.tick(15001),
    });
    await scenario.run();
    NodeAssert.equal(
      (await scenario.readBounded()).runs.some((run) => run.id === "r3"),
      false,
    );
    NodeAssert.equal(scenario.projection.runs[2].status, "cancelled");
    NodeAssert.deepEqual(assertQueueState(scenario.projection, fixture, "edited"), fixture.ids);
  },
);
NodeTest.test(
  "bounded and UI absence cannot certify a durable M3 that is still queued",
  async (context) => {
    context.mock.timers.enable({ apis: ["Date"], now: 0 });
    const scenario = editContext(false, {
      omitCancelled: true,
      cancelDurably: false,
      advanceClock: () => context.mock.timers.tick(15001),
    });
    await NodeAssert.rejects(scenario.run(), /M3 cancellation/);
    NodeAssert.equal(scenario.projection.runs[2].status, "queued");
  },
);
NodeTest.test(
  "actual edit/cancel phase restores D and performs the selected gestures in order",
  async () => {
    const scenario = editContext();
    await scenario.run();
    NodeAssert.deepEqual(scenario.actions, [
      ["fill", fixture.draft],
      ["click", "Edit queued message"],
      ["fill", fixture.m2Edited],
      ["click", "Update queued message"],
      ["click", "Remove queued message"],
    ]);
  },
);
NodeTest.test(
  "no-edit variant fails the unchanged edited-text oracle after real phase ordering",
  async () => {
    const scenario = editContext(true);
    await NodeAssert.rejects(scenario.run(), /M2 retains exactly the edited text/);
    NodeAssert.deepEqual(scenario.actions, [
      ["fill", fixture.draft],
      ["click", "Remove queued message"],
    ]);
  },
);

NodeTest.test(
  "staged queue case is closed and cannot conflict with the existing function seam",
  () => {
    NodeAssert.equal(resolveQueueCase(), "workflow");
    NodeAssert.equal(resolveQueueCase({}, { gateOnly: true }), "gate-only");
    NodeAssert.equal(resolveQueueCase({}, { skipEditGesture: true }), "no-edit-negative");
    for (const queueCase of ["workflow", "gate-only", "no-edit-negative"])
      NodeAssert.equal(resolveQueueCase({ queueCase }), queueCase);
    NodeAssert.throws(() => resolveQueueCase({ queueCase: "typo" }), /Unknown staged/);
    NodeAssert.throws(
      () => resolveQueueCase({ queueCase: "gate-only" }, { skipEditGesture: true }),
      /conflicts/,
    );
    NodeAssert.throws(
      () => resolveQueueCase({}, { gateOnly: true, skipEditGesture: true }),
      /combined/,
    );
    NodeAssert.throws(() => resolveQueueCase({}, { gateOnly: "true" }), /Invalid/);
  },
);

NodeTest.test("a passing readiness gate cannot be relabelled as a full queue qualification", () => {
  const full = {
    queueCase: "workflow",
    variant: "queue-edit-cancel-replay",
    providerGate: { status: "ready" },
    durable: { users: 3, runs: 3, providerTurns: 2, ids: fixture.ids },
    replay: { commandId: "native-command", messageId: "m2" },
  };
  assertFullQueueEvidence(full);
  for (const patch of [
    { queueCase: "gate-only" },
    { variant: "gate-only" },
    { queueCase: "no-edit-negative" },
    { durable: undefined },
    { providerGate: { status: "warning" } },
    { replay: { commandId: "native-command", messageId: "m3" } },
  ])
    NodeAssert.throws(() => assertFullQueueEvidence({ ...full, ...patch }));
});

NodeTest.test(
  "provider settings navigation preserves the Electron document and selects its hash route",
  async () => {
    for (const initial of [
      "http://127.0.0.1:43123/shell/index.html?desktop=1#/",
      "file:///app/client/index.html#/threads/previous",
    ]) {
      let current = initial;
      let reloaded = false;
      let ready = false;
      await openProviderSettings({
        page: {
          url: () => current,
          goto: async (url) => {
            current = url;
          },
          getByRole: (role, options) => {
            NodeAssert.equal(role, "heading");
            NodeAssert.deepEqual(options, { name: "Providers", exact: true });
            return {
              waitFor: async (options) => {
                NodeAssert.equal(options.state, "visible");
                NodeAssert.ok(reloaded, "Readiness follows the restored renderer fixture");
                NodeAssert.equal(new URL(current).hash, "#/settings/providers");
                ready = true;
              },
            };
          },
        },
        reload: async () => {
          reloaded = true;
        },
      });
      const before = new URL(initial);
      const after = new URL(current);
      NodeAssert.equal(after.hash, "#/settings/providers");
      before.hash = "";
      after.hash = "";
      NodeAssert.equal(after.href, before.href, "The Electron document URL must remain intact");
      NodeAssert.ok(ready, "Providers must be visibly ready before configuring the stub");
    }
  },
);

NodeTest.test(
  "provider settings navigation refuses a missing visible Providers surface",
  async () => {
    await NodeAssert.rejects(
      openProviderSettings({
        page: {
          url: () => "http://127.0.0.1:43123/#/",
          goto: async () => {},
          getByRole: () => ({
            waitFor: async () => {
              throw new Error("Providers surface unavailable");
            },
          }),
        },
        reload: async () => {},
      }),
      /Providers surface unavailable/,
    );
  },
);

NodeTest.test(
  "queue cleanup runs after early return and preserves both failure identities",
  async () => {
    const failure = new Error("Scenario failed");
    const cleanupFailure = new Error("Cleanup failed");
    let cleaned = 0;
    await withQueueCleanup(
      async () => undefined,
      async () => {
        cleaned++;
      },
    );
    NodeAssert.equal(cleaned, 1);
    await NodeAssert.rejects(
      withQueueCleanup(
        async () => {
          throw failure;
        },
        async () => {
          cleaned++;
        },
      ),
      (error) => error === failure,
    );
    NodeAssert.equal(cleaned, 2);
    await NodeAssert.rejects(
      withQueueCleanup(
        async () => undefined,
        async () => {
          throw cleanupFailure;
        },
      ),
      (error) => error === cleanupFailure,
    );
    await NodeAssert.rejects(
      withQueueCleanup(
        async () => {
          throw failure;
        },
        async () => {
          throw cleanupFailure;
        },
      ),
      (error) => {
        NodeAssert.ok(error instanceof AggregateError);
        NodeAssert.deepEqual(error.errors, [failure, cleanupFailure]);
        NodeAssert.equal(error.cause, cleanupFailure);
        return true;
      },
    );
  },
);

function sidebarSelectionContext({
  legacy = false,
  wrongRoute = false,
  inactive = false,
  duplicate = false,
} = {}) {
  const rows = [
    {
      id: "control",
      name: "JE06 control, JE06 queue project",
      key: "environment:control",
      active: false,
    },
    {
      id: "target",
      name: legacy ? "JE06 queue target" : "JE06 queue target, JE06 queue project",
      key: legacy ? "" : "environment:target",
      active: false,
    },
  ];
  if (duplicate) rows.push({ ...rows[1] });
  const clicks = [];
  let url = new URL("http://127.0.0.1:1/#/");
  let composerReady = false;
  const locator = (read) => ({
    read,
    getByRole: (role, { name }) => {
      NodeAssert.equal(role, "button");
      return locator(() =>
        read().filter((row) =>
          typeof name === "string" ? row.name === name : name.test(row.name),
        ),
      );
    },
    and: (other) => locator(() => read().filter((row) => other.read().includes(row))),
    click: async () => {
      NodeAssert.equal(read().length, 1, "Click must identify exactly one row");
      const row = read()[0];
      clicks.push(row.id);
      url.hash = `/environment/${wrongRoute ? "control" : row.id}`;
      if (!inactive) row.active = true;
    },
    waitFor: async () => {
      NodeAssert.equal(read().length, 1, "Exactly one active target must be visible");
    },
  });
  return {
    clicks,
    composerReady: () => composerReady,
    ctx: {
      page: {
        getByRole: (role, options) => {
          NodeAssert.equal(role, "navigation");
          NodeAssert.deepEqual(options, { name: "Threads", exact: true });
          return locator(() => rows);
        },
        locator: (selector) => {
          if (selector.startsWith("li[data-thread-item")) {
            const suffix = selector.match(/\$="([^"]+)"/)[1];
            return locator(() => rows.filter((row) => row.key.endsWith(suffix)));
          }
          NodeAssert.equal(selector, '[aria-current="page"], [data-active="true"]');
          return locator(() => rows.filter((row) => row.active));
        },
        waitForURL: async (matches) =>
          NodeAssert.ok(matches(url), "Active route must identify the target, not the control"),
        getByTestId: (id) => {
          NodeAssert.equal(id, "composer-editor");
          return {
            waitFor: async () => {
              composerReady = true;
            },
          };
        },
      },
    },
  };
}

NodeTest.test(
  "sidebar selection reaches the unique target with current and legacy accessible names",
  async () => {
    for (const legacy of [false, true]) {
      const context = sidebarSelectionContext({ legacy });
      await selectQueueTarget(context.ctx, fixture);
      NodeAssert.deepEqual(context.clicks, ["target"]);
      NodeAssert.equal(context.composerReady(), true);
    }
  },
);

NodeTest.test(
  "sidebar selection rejects duplicate names, a control route, and an inactive target",
  async () => {
    for (const options of [{ duplicate: true }, { wrongRoute: true }, { inactive: true }]) {
      const context = sidebarSelectionContext(options);
      await NodeAssert.rejects(selectQueueTarget(context.ctx, fixture));
      NodeAssert.equal(context.composerReady(), false);
      NodeAssert.ok(context.clicks.every((id) => id === "target"));
    }
  },
);
