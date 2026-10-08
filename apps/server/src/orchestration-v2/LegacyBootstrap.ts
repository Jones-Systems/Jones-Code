import { type RecordedStoredLifecycleEvent as OrchestrationV2StoredEvent } from "./RecordedTypes.ts";
// @effect-diagnostics-next-line nodeBuiltinImport:off - Pure deciders and persisted receipt keys require synchronous SHA-256.
import * as NodeCrypto from "node:crypto";
import { CommandId, OrchestrationV2LegacyBootstrapPolicy, type ThreadId } from "@t3tools/contracts";

export function legacyBootstrapCreateCommandId(
  threadId: ThreadId,
  releaseCommandId: CommandId,
): CommandId {
  const digest = NodeCrypto.createHash("sha256")
    .update(JSON.stringify([threadId, releaseCommandId]))
    .digest("hex");
  return CommandId.make(`server:queue-bootstrap:v1:${digest}:create`);
}

import * as Schema from "effect/Schema";

export const sameLegacyBootstrapPolicy = Schema.toEquivalence(OrchestrationV2LegacyBootstrapPolicy);

export function legacyBootstrapBirth(input: {
  policy: OrchestrationV2LegacyBootstrapPolicy;
  claimEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
  birthEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
}) {
  const { policy } = input;
  if (
    policy.createCommandId !==
      legacyBootstrapCreateCommandId(policy.threadId, policy.releaseCommandId) ||
    policy.birthCommandId !== `${policy.createCommandId}:initial-message` ||
    policy.runId === undefined
  )
    return { type: "mismatched" as const };
  const claims = input.claimEvents.filter(
    ({ event }) =>
      event.type === (policy.ownsNewThread ? "thread.created" : "thread.metadata-updated"),
  );
  const births = input.birthEvents.filter(({ event }) => event.type === "run.created");
  if (claims.length > 1 || births.length > 1) return { type: "ambiguous" as const };
  if (claims.length !== 1 || births.length !== 1) return { type: "missing" as const };
  const claim = claims[0]!;
  const birth = births[0]!;
  if (
    !(claim.event.type === "thread.created" || claim.event.type === "thread.metadata-updated") ||
    birth.event.type !== "run.created"
  )
    return { type: "missing" as const };
  const run = birth.event.payload;
  const claimPolicy = claim.event.payload.legacyBootstrapClaim;
  const birthPolicy = run.legacyBootstrap;
  const { runId: _runId, ...claimExpected } = policy;
  if (
    claim.commandId !== policy.createCommandId ||
    birth.commandId !== policy.birthCommandId ||
    claim.event.threadId !== policy.threadId ||
    claim.event.payload.projectId !== policy.projectId ||
    birth.event.threadId !== policy.threadId ||
    run.threadId !== policy.threadId ||
    run.userMessageId !== policy.messageId ||
    run.id !== policy.runId ||
    claimPolicy === undefined ||
    birthPolicy === undefined ||
    !sameLegacyBootstrapPolicy(claimPolicy, claimExpected) ||
    !sameLegacyBootstrapPolicy(birthPolicy, policy) ||
    claim.sequence >= birth.sequence
  )
    return { type: "mismatched" as const };
  return {
    type: "valid" as const,
    run,
    sequence: birth.sequence,
    claimEventId: claim.event.id,
    claimSequence: claim.sequence,
    birthEventId: birth.event.id,
  };
}

export function canonicalLegacyPayload(value: unknown): string {
  const ordered = (input: unknown): unknown =>
    Array.isArray(input)
      ? input.map(ordered)
      : input !== null && typeof input === "object"
        ? Object.fromEntries(
            Object.entries(input)
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([key, entry]) => [key, ordered(entry)]),
          )
        : input;
  return JSON.stringify(ordered(value));
}

export function legacyPayloadHash(canonicalPayload: string): string {
  return NodeCrypto.createHash("sha256").update(canonicalPayload).digest("hex");
}

export function legacyPreparationGeneration(input: {
  readonly runId: string;
  readonly birthEventId: string;
  readonly birthSequence: number;
}): string {
  return legacyPayloadHash(canonicalLegacyPayload(input));
}

export function legacyPreparationEffectId(input: {
  readonly generation: string;
  readonly effect: unknown;
}): string {
  return `legacy-preparation:${legacyPayloadHash(canonicalLegacyPayload(input))}`;
}

export function transitionLegacyPreparation(input: {
  readonly current: import("./RecordedTypes.ts").LegacyPreparation | undefined;
  readonly update: import("./RecordedTypes.ts").LegacyPreparationUpdate;
  readonly commandId: CommandId;
}) {
  const { current, update, commandId } = input;
  const rejected = (reason: string) => ({ type: "rejected" as const, reason });
  if (update.type === "initialize") {
    const next = update.preparation;
    if (
      commandId !== `${next.policy.createCommandId}:preparation:${next.generation}:initialize` ||
      next.steps.length !== 0 ||
      (next.setup.status !== "unresolved" && next.setup.status !== "opted_out")
    )
      return rejected("Preparation initialization has another identity or prior effects.");
    if (current !== undefined && canonicalLegacyPayload(current) !== canonicalLegacyPayload(next))
      return rejected("Preparation initialization cannot replace the recorded generation.");
    return { type: "accepted" as const, preparation: current ?? next };
  }
  if (current === undefined) return rejected("Preparation has no authenticated initialization.");
  if (update.type === "setup-policy") {
    if (
      current.setup.status !== "unresolved" ||
      update.setup.status === "unresolved" ||
      commandId !==
        `${current.policy.createCommandId}:preparation:${current.generation}:setup-policy`
    )
      return rejected("Preparation setup policy is already bound or has another identity.");
    return { type: "accepted" as const, preparation: { ...current, setup: update.setup } };
  }
  const step = update.step;
  const effectId = legacyPreparationEffectId({
    generation: current.generation,
    effect: step.effect,
  });
  const stem = `${current.policy.createCommandId}:preparation:${current.generation}:${effectId}`;
  if (
    step.effectId !== effectId ||
    step.inputHash !== legacyPayloadHash(canonicalLegacyPayload(step.effect)) ||
    step.intentCommandId !== `${stem}:intent` ||
    step.intentEventId !== `${stem}:intent:event`
  )
    return rejected("Preparation effect input or intent identity changed.");
  const existing = current.steps.find((entry) => entry.effectId === effectId);
  if (update.type === "intent") {
    if (step.effect.kind.startsWith("setup.")) {
      if (current.setup.status !== "resolved")
        return rejected("Preparation setup definition is unavailable.");
      const definition = current.setup.definition;
      const effect = step.effect;
      if (
        effect.kind === "setup.open" &&
        canonicalLegacyPayload(effect.input) !== canonicalLegacyPayload(definition)
      )
        return rejected("Setup spawn differs from the captured definition.");
      if (
        (effect.kind === "setup.write" || effect.kind === "setup.completion") &&
        (effect.input.terminalId !== definition.terminalId ||
          effect.input.generation !== definition.generation ||
          effect.input.definitionHash !== definition.definitionHash ||
          effect.input.completionToken !== definition.completionToken)
      )
        return rejected("Setup control, generation, definition or completion token changed.");
      if (effect.kind === "setup.write" && effect.input.commandLine !== definition.commandLine)
        return rejected("Setup write differs from the captured command bytes.");
    }
    if (
      commandId !== step.intentCommandId ||
      existing !== undefined ||
      step.state !== "intent" ||
      step.outcomeCommandId !== undefined ||
      step.outcomeEventId !== undefined ||
      step.evidence !== undefined ||
      current.steps.some(
        (entry) =>
          entry.effect.kind === step.effect.kind ||
          ["intent", "unknown", "known_partial"].includes(entry.state),
      )
    )
      return rejected("Preparation intent would repeat or pass an unresolved effect.");
    if (step.effect.kind === "setup.open" && current.setup.status !== "resolved")
      return rejected("Preparation setup definition must be captured before terminal opening.");
    if (step.effect.kind === "branch.rename") {
      const effect = step.effect;
      const lastClaim = [...current.steps]
        .reverse()
        .find(
          (entry) =>
            entry.state === "known_succeeded" &&
            (entry.evidence?.type === "worktree_claim" ||
              (entry.evidence?.type === "settled_git" && entry.evidence.claim !== undefined)),
        );
      const claim =
        lastClaim?.evidence !== undefined && "claim" in lastClaim.evidence
          ? lastClaim.evidence.claim
          : undefined;
      if (
        claim === undefined ||
        canonicalLegacyPayload(effect.input.claim) !== canonicalLegacyPayload(claim) ||
        current.commonDirectory !== claim.commonDirectory ||
        effect.input.oldRef !== claim.headRef ||
        effect.input.oldOid !== claim.headOid ||
        !effect.input.targetRef.startsWith("refs/heads/") ||
        effect.input.targetRef === effect.input.oldRef ||
        canonicalLegacyPayload(effect.input.args) !==
          canonicalLegacyPayload([
            "branch",
            "-m",
            "--",
            effect.input.oldRef.slice("refs/heads/".length),
            effect.input.targetRef.slice("refs/heads/".length),
          ])
      )
        return rejected(
          "Rename intent differs from its exact durable material claim or chosen target.",
        );
    }
    let commonDirectory = current.commonDirectory;
    if (
      step.effect.kind === "worktree.add" ||
      step.effect.kind === "worktree.submodules" ||
      step.effect.kind === "worktree.base-config"
    ) {
      if (commonDirectory === null && step.effect.kind === "worktree.add")
        commonDirectory = step.effect.input.commonDirectory;
      if (commonDirectory !== step.effect.input.commonDirectory)
        return rejected("Worktree intent differs from its immutable common-directory binding.");
    }
    return {
      type: "accepted" as const,
      preparation: { ...current, commonDirectory, steps: [...current.steps, step] },
    };
  }
  if (
    existing === undefined ||
    existing.state !== "intent" ||
    step.state === "intent" ||
    commandId !== `${stem}:outcome` ||
    step.outcomeCommandId !== commandId ||
    step.outcomeEventId !== `${commandId}:event` ||
    step.evidence === undefined ||
    canonicalLegacyPayload({
      ...step,
      state: existing.state,
      evidence: undefined,
      outcomeCommandId: undefined,
      outcomeEventId: undefined,
    }) !== canonicalLegacyPayload(existing)
  )
    return rejected("Preparation outcome has no exact pending intent.");
  const evidence = step.evidence;
  const terminalEvidenceMatches = () =>
    "terminalId" in step.effect.input &&
    "terminalId" in evidence &&
    evidence.terminalId === step.effect.input.terminalId &&
    evidence.generation === step.effect.input.generation;
  const claimMatches = (claim: {
    readonly path: string;
    readonly commonDirectory: string;
    readonly registeredPath: string;
    readonly headRef: string;
    readonly headOid: string;
  }) => {
    const effect = step.effect;
    if (
      effect.kind === "worktree.add" ||
      effect.kind === "worktree.submodules" ||
      effect.kind === "worktree.base-config"
    )
      return (
        claim.path === effect.input.worktreePath &&
        claim.registeredPath === claim.path &&
        claim.commonDirectory === effect.input.commonDirectory &&
        claim.headRef === effect.input.targetRef &&
        claim.headOid === effect.input.baseCommitOid
      );
    if (effect.kind === "branch.rename")
      return (
        canonicalLegacyPayload(claim) ===
        canonicalLegacyPayload({ ...effect.input.claim, headRef: effect.input.targetRef })
      );
    if (effect.kind === "worktree.remove")
      return (
        claim.path === effect.input.claim.path &&
        claim.commonDirectory === effect.input.claim.commonDirectory &&
        claim.registeredPath === effect.input.claim.registeredPath &&
        claim.headRef === effect.input.claim.headRef &&
        claim.headOid === effect.input.claim.headOid
      );
    return false;
  };
  let validEvidence = false;
  switch (step.state) {
    case "unknown":
      validEvidence = evidence.type === "unknown";
      break;
    case "known_no_effect_failure":
      validEvidence =
        evidence.type === "never_invoked" &&
        evidence.owner ===
          (step.effect.kind.startsWith("worktree.") || step.effect.kind === "branch.rename"
            ? "git"
            : step.effect.kind === "terminal.close"
              ? "terminal"
              : "setup");
      break;
    case "known_partial":
      validEvidence =
        step.effect.kind === "worktree.add" &&
        evidence.type === "worktree_claim" &&
        claimMatches(evidence.claim);
      break;
    case "known_cancelled_cleaned":
      validEvidence =
        step.effect.kind === "terminal.close"
          ? evidence.type === "control_stopped" && terminalEvidenceMatches()
          : step.effect.kind === "worktree.remove" &&
            evidence.type === "settled_git" &&
            evidence.exitCode === 0 &&
            evidence.claim !== undefined &&
            claimMatches(evidence.claim);
      break;
    case "known_completed_failure":
      validEvidence =
        step.effect.kind === "setup.completion" &&
        evidence.type === "setup_completion" &&
        evidence.exitCode !== null &&
        evidence.exitCode !== 0 &&
        terminalEvidenceMatches();
      break;
    case "known_started":
      validEvidence =
        step.effect.kind === "setup.write" &&
        evidence.type === "terminal_write" &&
        evidence.inputCount > 0 &&
        terminalEvidenceMatches();
      break;
    case "known_succeeded":
      if (step.effect.kind === "worktree.add")
        validEvidence = evidence.type === "worktree_claim" && claimMatches(evidence.claim);
      else if (
        step.effect.kind === "worktree.submodules" ||
        step.effect.kind === "worktree.base-config" ||
        step.effect.kind === "branch.rename"
      )
        validEvidence =
          evidence.type === "settled_git" &&
          evidence.exitCode === 0 &&
          evidence.claim !== undefined &&
          claimMatches(evidence.claim);
      else if (step.effect.kind === "setup.open")
        validEvidence =
          evidence.type === "terminal_generation" &&
          terminalEvidenceMatches() &&
          evidence.shell === step.effect.input.shell &&
          canonicalLegacyPayload(evidence.shellArgs) ===
            canonicalLegacyPayload(step.effect.input.shellArgs);
      else if (step.effect.kind === "setup.completion")
        validEvidence =
          evidence.type === "setup_completion" &&
          evidence.exitCode === 0 &&
          terminalEvidenceMatches();
      break;
  }
  if (!validEvidence)
    return rejected("Preparation outcome lacks affirmative owner evidence for its state.");
  return {
    type: "accepted" as const,
    preparation: {
      ...current,
      steps: current.steps.map((entry) => (entry.effectId === effectId ? step : entry)),
    },
  };
}

/** Candidate only; release also requires exact accepted raw intent/outcome receipts. */
export function legacyNeverInvokedSetupOpen(run: import("./RecordedTypes.ts").RecordedRun) {
  const preparation = run.legacyPreparation;
  const policy = run.legacyBootstrap;
  if (
    preparation === undefined ||
    policy === undefined ||
    preparation.setup.status !== "resolved" ||
    !sameLegacyBootstrapPolicy(preparation.policy, policy) ||
    run.id !== policy.runId ||
    run.threadId !== policy.threadId ||
    run.userMessageId !== policy.messageId ||
    preparation.generation !==
      legacyPreparationGeneration({
        runId: run.id,
        birthEventId: preparation.birthEventId,
        birthSequence: preparation.birthSequence,
      })
  )
    return undefined;
  const definition = preparation.setup.definition;
  const steps = preparation.steps.filter((step) => step.effect.kind.startsWith("setup."));
  const opened = steps[0];
  if (
    steps.length !== 1 ||
    opened?.effect.kind !== "setup.open" ||
    opened.state !== "known_no_effect_failure" ||
    opened.evidence?.type !== "never_invoked" ||
    opened.evidence.owner !== "setup" ||
    opened.evidence.reason !== "input_validation_failed" ||
    opened.outcomeCommandId === undefined ||
    opened.outcomeEventId === undefined ||
    opened.inputHash !== legacyPayloadHash(canonicalLegacyPayload(opened.effect)) ||
    opened.effectId !==
      legacyPreparationEffectId({ generation: preparation.generation, effect: opened.effect }) ||
    canonicalLegacyPayload(opened.effect.input) !== canonicalLegacyPayload(definition) ||
    definition.generation !==
      legacyPayloadHash(
        canonicalLegacyPayload({
          preparationGeneration: preparation.generation,
          terminalId: definition.terminalId,
        }),
      ) ||
    !(
      definition.shell.length === 0 ||
      definition.shell.includes("\0") ||
      definition.shellArgs.some((arg) => arg.includes("\0"))
    )
  )
    return undefined;
  return { preparation, definition, opened };
}

export function legacyPreparationReleaseBlocker(input: {
  readonly run: import("./RecordedTypes.ts").RecordedRun;
}): string | undefined {
  const { run } = input;
  const preparation = run.legacyPreparation;
  if (preparation === undefined) return "Legacy preparation evidence is unavailable.";
  if (preparation.setup.status === "unresolved")
    return "Legacy setup policy has not been captured.";
  const neverInvokedSetup = legacyNeverInvokedSetupOpen(run);
  if (
    preparation.steps.some(
      (step) =>
        !["known_succeeded", "known_started"].includes(step.state) &&
        step !== neverInvokedSetup?.opened,
    )
  )
    return "Legacy preparation has an unresolved, failed or cancelled owner outcome.";
  if (
    preparation.steps.some(
      (step) => step.effect.kind === "terminal.close" || step.effect.kind === "worktree.remove",
    )
  )
    return "Legacy cancelled preparation cannot release.";
  const add = preparation.steps.find((step) => step.effect.kind === "worktree.add");
  if (
    run.workspacePreparation?.type === "worktree" &&
    (add?.state !== "known_succeeded" || add.evidence?.type !== "worktree_claim")
  )
    return "Legacy worktree creation has no affirmative material claim.";
  if (run.workspacePreparation?.type !== "worktree" && add !== undefined)
    return "Legacy checkout evidence belongs to another workspace strategy.";
  if (preparation.setup.status !== "resolved") {
    if (preparation.steps.some((step) => step.effect.kind.startsWith("setup.")))
      return "Legacy setup effects conflict with the captured skip policy.";
    return undefined;
  }
  if (neverInvokedSetup !== undefined) return undefined;
  const definition = preparation.setup.definition;
  const opened = preparation.steps.find((step) => step.effect.kind === "setup.open");
  const written = preparation.steps.find((step) => step.effect.kind === "setup.write");
  if (
    opened?.effect.kind !== "setup.open" ||
    opened.state !== "known_succeeded" ||
    canonicalLegacyPayload(opened.effect.input) !== canonicalLegacyPayload(definition) ||
    written?.effect.kind !== "setup.write" ||
    written.state !== "known_started" ||
    written.effect.input.terminalId !== definition.terminalId ||
    written.effect.input.generation !== definition.generation ||
    written.effect.input.definitionHash !== definition.definitionHash ||
    written.effect.input.commandLine !== definition.commandLine ||
    written.effect.input.completionToken !== definition.completionToken
  )
    return "Legacy setup lacks exact captured spawn and accepted write outcomes.";
  const completed = preparation.steps.find((step) => step.effect.kind === "setup.completion");
  if (
    !definition.async &&
    (completed?.effect.kind !== "setup.completion" ||
      completed.state !== "known_succeeded" ||
      completed.effect.input.terminalId !== definition.terminalId ||
      completed.effect.input.generation !== definition.generation ||
      completed.effect.input.definitionHash !== definition.definitionHash ||
      completed.effect.input.completionToken !== definition.completionToken)
  )
    return "Legacy synchronous setup has no exact successful completion.";
  return undefined;
}

/** Candidate only; the receiving owner must authenticate receipts and executing control before D. */
export function legacyNeverInvokedWorkspaceFailure(run: import("./RecordedTypes.ts").RecordedRun) {
  const preparation = run.legacyPreparation;
  const policy = run.legacyBootstrap;
  if (
    preparation === undefined ||
    policy === undefined ||
    !policy.ownsNewThread ||
    !sameLegacyBootstrapPolicy(preparation.policy, policy) ||
    run.id !== policy.runId ||
    run.threadId !== policy.threadId ||
    run.userMessageId !== policy.messageId ||
    run.startedAt !== null ||
    run.legacyReleaseDecision !== undefined ||
    preparation.generation !==
      legacyPreparationGeneration({
        runId: run.id,
        birthEventId: preparation.birthEventId,
        birthSequence: preparation.birthSequence,
      }) ||
    !["opted_out", "no_script"].includes(preparation.setup.status) ||
    preparation.steps.length !== 1
  )
    return undefined;
  const step = preparation.steps[0]!;
  if (
    step.effect.kind !== "worktree.add" ||
    step.effect.input.before === undefined ||
    step.state !== "known_no_effect_failure" ||
    step.evidence?.type !== "never_invoked" ||
    step.evidence.owner !== "git" ||
    step.evidence.reason !== "input_validation_failed" ||
    step.outcomeCommandId === undefined ||
    step.outcomeEventId === undefined ||
    step.inputHash !== legacyPayloadHash(canonicalLegacyPayload(step.effect)) ||
    step.effectId !==
      legacyPreparationEffectId({ generation: preparation.generation, effect: step.effect }) ||
    preparation.commonDirectory !== step.effect.input.commonDirectory
  )
    return undefined;
  return { preparation, step, policy };
}
