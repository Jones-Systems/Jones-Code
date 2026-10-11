// @effect-diagnostics nodeBuiltinImport:off
import * as Fs from "node:fs/promises";
import * as Path from "node:path";
import * as Effect from "effect/Effect";
import type { ServiceLauncherClient } from "../../cloud/serviceLauncherClient.ts";
import { parseServiceState } from "../../cloud/serviceProtocol.ts";
import { currentQualifiedRuntimeBinding } from "../cloud/qualifiedRuntime.ts";
import { operationBinding, reconcileUpdateOperation } from "./launcherOperation.ts";
import {
  PlannedContinuityError,
  type PlannedUpdateContinuity,
  type PlannedUpdateProof,
} from "./PlannedUpdateContinuity.ts";

export function activatePlannedUpdateContinuity(input: {
  readonly baseDir: string;
  readonly dbPath: string;
  readonly launcher: ServiceLauncherClient["Service"];
  readonly continuity: PlannedUpdateContinuity["Service"];
}) {
  return Effect.gen(function* () {
    const outcome = input.launcher.qualifiedStartupOutcome;
    if (
      !input.launcher.managed ||
      input.launcher.qualifiedOperations !== true ||
      input.launcher.currentVersion === undefined ||
      outcome === undefined ||
      (outcome.status !== "committed" && outcome.status !== "rolled-back")
    )
      return;
    const version = input.launcher.currentVersion;
    const proof = yield* Effect.tryPromise({
      try: async (): Promise<PlannedUpdateProof> => {
        const baseDir = await Fs.realpath(input.baseDir);
        const state = parseServiceState(
          await Fs.readFile(Path.join(baseDir, "runtime", "service-state.json"), "utf8"),
        );
        const update = state?.update;
        if (
          state?.activeVersion !== version ||
          update?.id !== outcome.id ||
          update.status !== outcome.status ||
          update.qualified === undefined
        )
          throw new Error("Current native update generation differs from the startup outcome.");
        const native = await reconcileUpdateOperation(baseDir, outcome.id, update);
        if (native.state !== outcome.status)
          throw new Error("Native operation outcome is not proven.");
        const current = await currentQualifiedRuntimeBinding(baseDir, version);
        if (current.dbPath !== (await Fs.realpath(input.dbPath)))
          throw new Error("Running database differs from planned update binding.");
        return {
          operationId: outcome.id,
          outcome: outcome.status,
          binding: operationBinding(update.qualified),
          current,
        };
      },
      catch: (cause) => new PlannedContinuityError({ cause }),
    });
    yield* input.continuity.activate(proof);
  });
}
