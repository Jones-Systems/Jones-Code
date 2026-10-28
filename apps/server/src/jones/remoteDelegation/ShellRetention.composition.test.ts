import { expect, it } from "@effect/vitest";
import {
  EventId,
  RuntimeRequestId,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import { isDelegatedTaskShownUnderParent } from "@t3tools/shared/jones/threadAttention";
import * as Effect from "effect/Effect";

import {
  createRetentionHarness,
  environmentId,
  now,
  parent,
  remoteTask,
} from "./ShellRetention.testkit.ts";

const flags = [
  { omitIdleSubagents: true, omitOlderSettled: false },
  { omitIdleSubagents: false, omitOlderSettled: true },
  { omitIdleSubagents: true, omitOlderSettled: true },
];
const parentsOf = (snapshot: OrchestrationV2ShellSnapshot) =>
  new Map(snapshot.threads.map((row) => [row.id, row]));

it.effect.each([false, true])(
  "composes real destination HTTP with reduced origin HTTP and WS (nested=%s)",
  (nestedOwner) =>
    Effect.gen(function* () {
      const origin = yield* createRetentionHarness({ nestedOwner });
      const destination = yield* createRetentionHarness({ bind: false });
      const remoteChild = remoteTask.remoteChild!;
      const delegatedThread = {
        ...parent,
        id: remoteChild.threadId,
        title: remoteTask.title ?? "Remote task",
        lineage: {
          rootThreadId: remoteChild.threadId,
          parentThreadId: null,
          relationshipToParent: null,
        },
        settledOverride: null,
        settledAt: null,
        delegatedFrom: {
          environmentId,
          threadId: origin.owner.id,
          title: origin.owner.title,
        },
      };
      yield* destination.eventSink.write({
        events: [
          {
            id: EventId.make("create-destination-delegate"),
            type: "thread.created",
            threadId: delegatedThread.id,
            occurredAt: now,
            payload: delegatedThread,
          },
        ],
      });
      // Decode the destination's actual route output, never a fabricated shell.
      const destinationSnapshot = yield* destination.http();
      const task = destinationSnapshot.threads.find((row) => row.id === remoteChild.threadId);
      expect(task?.delegatedFrom).toEqual(delegatedThread.delegatedFrom);
      if (task === undefined) return yield* Effect.die("destination delegate is missing");
      const asking: OrchestrationV2ThreadShell = {
        ...task,
        status: "waiting",
        activityRunStatus: "waiting",
        pendingRuntimeRequest: {
          id: RuntimeRequestId.make("live-delegate-approval"),
          kind: "command",
          createdAt: now,
        },
      };
      for (const options of flags) {
        const query = `?omitIdleSubagents=${Number(options.omitIdleSubagents)}&omitOlderSettled=${Number(options.omitOlderSettled)}`;
        for (const snapshot of [yield* origin.http(query), yield* origin.ws(options)]) {
          const parents = parentsOf(snapshot);
          expect(isDelegatedTaskShownUnderParent(task, parents, new Map())).toBe(true);
          expect(isDelegatedTaskShownUnderParent(asking, parents, new Map())).toBe(false);
        }
      }

      // The origin's root changes through the real projection as well.
      yield* origin.eventSink.write({
        events: [
          {
            id: EventId.make("unsettle-origin-root"),
            type: "thread.unsettled",
            threadId: parent.id,
            occurredAt: now,
            payload: { ...parent, settledOverride: null, settledAt: null },
          },
        ],
      });
      for (const options of flags) {
        const query = `?omitIdleSubagents=${Number(options.omitIdleSubagents)}&omitOlderSettled=${Number(options.omitOlderSettled)}`;
        for (const snapshot of [yield* origin.http(query), yield* origin.ws(options)]) {
          expect(isDelegatedTaskShownUnderParent(asking, parentsOf(snapshot), new Map())).toBe(
            true,
          );
        }
      }
    }),
);
