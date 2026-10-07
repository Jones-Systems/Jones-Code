import type {
  T3WorkstreamListResult,
  WorkstreamReadContext,
  WorkstreamReferencePage,
  WorkstreamsRegistrationContextResponse,
} from "@t3tools/contracts";
import type { LiveT3Placements } from "@t3tools/client-runtime/state/workstreams";
import type { WorkstreamListView } from "../../state/workstreams";
import { canEditWorkstreams } from "../../components/workstreams/nativeWorkstreamActions";

export interface WorkstreamActionSnapshot {
  readonly data: T3WorkstreamListResult;
  readonly references: WorkstreamReferencePage;
  readonly placements: LiveT3Placements | null;
  readonly registrationContext: WorkstreamsRegistrationContextResponse | null;
}

export interface WorkstreamReferenceController extends Pick<
  WorkstreamListView,
  "data" | "loading" | "runBindingOperation" | "loadDetail" | "loadReference"
> {
  readonly registrationContext: WorkstreamsRegistrationContextResponse | null;
  readonly loadActionSnapshot: (options?: {
    readonly signal?: AbortSignal;
  }) => Promise<WorkstreamActionSnapshot>;
  readonly retry: () => Promise<void>;
}

export class WorkstreamActionError extends Error {
  constructor(
    readonly reason: "activation" | "stale" | "denied" | "unknown" | "ambiguous" | "invalid-pr",
  ) {
    super(workstreamActionMessage(reason));
    this.name = "WorkstreamActionError";
  }
}

function workstreamActionMessage(reason: WorkstreamActionError["reason"]): string {
  switch (reason) {
    case "activation":
      return "Reference preparation is unavailable until its source is activated.";
    case "stale":
      return "Workstreams or reference verification changed. Reload and take a new action.";
    case "denied":
      return "The Workstream action was denied. No further changes were submitted.";
    case "ambiguous":
      return "Conflicting references require resolution before this action can continue.";
    case "invalid-pr":
      return "Enter a full GitHub pull request URL.";
    case "unknown":
      return "The effect is unknown. Retry reloads metadata and checks the existing command; it does not resubmit.";
  }
}

export const workstreamFailureMessage = (cause: unknown): string =>
  cause instanceof WorkstreamActionError ? cause.message : workstreamActionMessage("unknown");

export function assertWorkstreamReadContext(
  data: T3WorkstreamListResult,
  context: WorkstreamReadContext,
): void {
  if (
    context.owner_id !== data.binding.ownerId ||
    context.server_generation !== data.binding.serverGeneration ||
    context.registry_version !== data.binding.registryVersion
  )
    throw new WorkstreamActionError("stale");
}

export function assertWorkstreamActionSnapshot(snapshot: WorkstreamActionSnapshot): void {
  const { data, references, placements, registrationContext } = snapshot;
  if (!canEditWorkstreams(data)) throw new WorkstreamActionError("stale");
  assertWorkstreamReadContext(data, references.context);
  for (const context of [placements?.context, registrationContext]) {
    if (!context) continue;
    assertWorkstreamReadContext(data, context);
    if (
      context.principal_id !== data.binding.principalId ||
      context.authorization_revision !== data.binding.authorizationRevision
    )
      throw new WorkstreamActionError("stale");
  }
  if (registrationContext && registrationContext.state !== "ready")
    throw new WorkstreamActionError("activation");
  if (references.next_cursor !== null) throw new WorkstreamActionError("stale");
}
