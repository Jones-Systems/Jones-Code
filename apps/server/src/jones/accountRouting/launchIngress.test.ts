import { it, expect } from "@effect/vitest";
import {
  CommandId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2ThreadLaunchInput,
} from "@t3tools/contracts";
import * as NodePath from "@effect/platform-node/NodePath";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ProjectCloneTracker from "../../project/ProjectCloneTracker.ts";
import { autoLaunchRequest, type AutoLaunchDecision } from "./accountBinding.ts";
import { makeAutoLaunchResolver } from "./launchIngress.ts";
import { AccountAutoRoutingError } from "./selectAutoAccount.ts";

const input = {
  accountRouting: { mode: "auto" },
  commandId: CommandId.make("synthetic-auto-request"),
  threadId: ThreadId.make("synthetic-auto-thread"),
  title: "Synthetic launch",
  projectId: ProjectId.make("test-project"),
  modelSelection: { instanceId: ProviderInstanceId.make("representative"), model: "test-model" },
  workspaceStrategy: { type: "root" },
  runtimeMode: "full-access",
  interactionMode: "default",
  initialMessage: { text: "A synthetic prompt", attachments: [] },
} satisfies OrchestrationV2ThreadLaunchInput;
const winner = { ...input.modelSelection, instanceId: ProviderInstanceId.make("winner") };
const quotaRefusal = new AccountAutoRoutingError({
  code: "no_available_allowance",
  message: "Synthetic quota refusal",
});
const projectRoot = "/checkouts/root";
const layer = Layer.mergeAll(
  Layer.mock(ProjectStore.ProjectStoreV2)({
    get: (id) =>
      id === input.projectId
        ? Effect.succeedSome({ workspaceRoot: projectRoot } as ProjectStore.ProjectRow)
        : Effect.succeedNone,
  }),
  Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
  FileSystem.layerNoop({
    realPath: (path) => Effect.succeed(path === "/worktree-alias" ? "/checkouts/worktree" : path),
  }),
  NodePath.layer,
);

it.effect(
  "passes the canonical checkout to the account observer and preserves manual selection",
  () =>
    Effect.gen(function* () {
      const resolve = makeAutoLaunchResolver((selection) => {
        expect(selection.cwd).toBe("/checkouts/worktree");
        expect(selection.modelSelection).toBe(input.modelSelection);
        return Effect.succeed({ modelSelection: winner });
      });
      const { accountRouting: _auto, ...manual } = input;
      expect((yield* resolve(manual, {})).modelSelection).toBe(input.modelSelection);
      const checkout = yield* resolve(
        {
          ...input,
          workspaceStrategy: { type: "existing_worktree", worktreePath: "/worktree-alias" },
        },
        {
          readAutoLaunch: () => Effect.undefined,
          commitAutoLaunchRefusal: () => Effect.die("unexpected refusal"),
        },
      );
      expect(checkout.modelSelection).toBe(winner);
      expect(checkout.workspace).toEqual({ projectRoot, worktreePath: "/checkouts/worktree" });
    }).pipe(Effect.provide(layer)),
);

it.effect("uses rejected evidence only for the original project, target and launch payload", () =>
  Effect.gen(function* () {
    const probes = yield* Ref.make(0);
    const retained = yield* Ref.make<AutoLaunchDecision | undefined>(undefined);
    const resolve = makeAutoLaunchResolver(() =>
      Ref.update(probes, (count) => count + 1).pipe(Effect.andThen(Effect.fail(quotaRefusal))),
    );
    const receipts: Parameters<typeof resolve>[1] = {
      readAutoLaunch: () => Ref.get(retained),
      commitAutoLaunchRefusal: ({ request, workspace }, refusal) => {
        const decision = {
          ...workspace,
          projectId: request.projectId,
          threadId: request.threadId ?? null,
          digest: request.digest,
          refusal,
        };
        return Ref.set(retained, decision).pipe(Effect.as(decision));
      },
    };
    for (const same of [input, input]) {
      const error = yield* resolve(same, receipts).pipe(Effect.flip);
      expect(error.message).toBe(quotaRefusal.message);
      expect(error.deliveryEvidence?.creation.status).toBe("rejected");
      expect(error.deliveryEvidence?.initialMessage.status).toBe("rejected");
    }
    for (const changed of [
      { ...input, projectId: ProjectId.make("different-project") },
      { ...input, threadId: ThreadId.make("different-thread") },
      { ...input, initialMessage: { ...input.initialMessage, text: "Different prompt" } },
      { ...input, workspaceStrategy: { type: "worktree" } as const },
    ]) {
      const error = yield* resolve(changed, receipts).pipe(Effect.flip);
      expect(error.deliveryEvidence?.creation.status).toBe("unknown");
      expect(error.deliveryEvidence?.initialMessage.status).toBe("unknown");
    }
    expect(yield* Ref.get(probes)).toBe(1);
  }).pipe(Effect.provide(layer)),
);

it.effect("surfaces uncertainty if an exact refusal cannot be committed", () =>
  Effect.gen(function* () {
    const resolve = makeAutoLaunchResolver(() => Effect.fail(quotaRefusal));
    const receipts: Parameters<typeof resolve>[1] = {
      readAutoLaunch: () => Effect.undefined,
      commitAutoLaunchRefusal: () => Effect.undefined,
    };
    const error = yield* resolve(input, receipts).pipe(Effect.flip);
    expect(error.deliveryEvidence?.creation.status).toBe("unknown");
    const accepted: AutoLaunchDecision = {
      projectId: input.projectId,
      threadId: input.threadId,
      digest: autoLaunchRequest(input)!.digest,
      projectRoot,
      worktreePath: null,
      refusal: null,
    };
    const raced = yield* resolve(input, {
      ...receipts,
      commitAutoLaunchRefusal: () => Effect.succeed(accepted),
    });
    expect(raced.modelSelection).toBe(input.modelSelection);
    expect(raced.binding).toBeUndefined();
  }).pipe(Effect.provide(layer)),
);
