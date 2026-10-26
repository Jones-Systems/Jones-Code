import type { ModelSelection, ProjectId, ServerProvider, ServerSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import type { ProviderSwitchPlanV2 } from "../../orchestration-v2/ProviderSwitchService.ts";
import {
  codexFailoverSelections,
  type CodexFailoverCandidate,
} from "../provider/codex/CodexFailoverSelection.ts";
import { selectionAdmissionReason } from "../threadConfiguration/selectionAdmission.ts";

export type CodexFailoverMetadata = Pick<
  CodexFailoverCandidate,
  "instanceId" | "driver" | "continuationKey" | "enabled"
>;

export interface CodexFailoverTarget {
  readonly savedSelection: ModelSelection;
  readonly continuationSelection: ModelSelection;
}

export interface CodexFailoverAdmissionInput<E, R> extends CodexFailoverTarget {
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly settings: Pick<ServerSettings, "projectSettingsOverrides" | "providerModelPreferences">;
  readonly projectId: ProjectId;
  readonly metadata: ReadonlyArray<CodexFailoverMetadata>;
  readonly now: number;
  readonly plan: (
    selection: ModelSelection,
  ) => Effect.Effect<Pick<ProviderSwitchPlanV2, "transition">, E, R>;
}

function selectionsAdmitted<E, R>(input: CodexFailoverAdmissionInput<E, R>): boolean {
  const target = input.savedSelection.instanceId;
  if (input.continuationSelection.instanceId !== target) return false;
  const metadata = input.metadata.find((row) => row.instanceId === target);
  const provider = input.providers.find((row) => row.instanceId === target);
  if (
    metadata?.driver !== "codex" ||
    !metadata.enabled ||
    provider?.driver !== "codex" ||
    provider.auth.status === "unauthenticated" ||
    provider.availability === "unavailable" ||
    provider.usageLimits?.windows.some(
      (window) =>
        window.usedPercent >= 100 &&
        window.resetsAt !== undefined &&
        Date.parse(window.resetsAt) > input.now,
    )
  )
    return false;
  return [input.savedSelection, input.continuationSelection].every(
    (selection) => selectionAdmissionReason({ ...input, selection }) === null,
  );
}

export const readmitCodexFailoverTarget = Effect.fn("CodexFailover.readmitTarget")(function* <E, R>(
  input: CodexFailoverAdmissionInput<E, R>,
) {
  if (!selectionsAdmitted(input)) return null;
  const plan = yield* Effect.option(input.plan(input.continuationSelection));
  if (Option.isNone(plan) || plan.value.transition.type !== "restart_and_resume") return null;
  return {
    savedSelection: input.savedSelection,
    continuationSelection: input.continuationSelection,
  } satisfies CodexFailoverTarget;
});

export const selectCodexFailoverTarget = Effect.fn("CodexFailover.selectTarget")(function* <E, R>(
  input: CodexFailoverAdmissionInput<E, R> & {
    readonly visited: ReadonlySet<ModelSelection["instanceId"]>;
  },
) {
  const source = input.metadata.find((row) => row.instanceId === input.savedSelection.instanceId);
  if (source === undefined) return null;
  const candidates = input.metadata.map((row) => ({
    ...row,
    enabled:
      row.enabled &&
      input.providers.some(
        (provider) => provider.instanceId === row.instanceId && provider.enabled,
      ),
    projectEnabled:
      input.settings.projectSettingsOverrides[input.projectId]?.providerInstances?.[row.instanceId]
        ?.enabled !== false,
  }));
  const proposals = codexFailoverSelections({
    selection: input.savedSelection,
    source,
    candidates,
    visited: input.visited,
  });
  for (const savedSelection of proposals) {
    const admitted = yield* readmitCodexFailoverTarget({
      ...input,
      savedSelection,
      continuationSelection: {
        ...input.continuationSelection,
        instanceId: savedSelection.instanceId,
      },
    });
    if (admitted !== null) return admitted;
  }
  return null;
});
