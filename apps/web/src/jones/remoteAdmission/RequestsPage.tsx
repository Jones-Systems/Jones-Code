import { Link } from "@tanstack/react-router";
import type { EnvironmentId } from "@t3tools/contracts";
import { WorkspacePageContainer } from "../../components/WorkspacePageContainer";
import { Button } from "../../components/ui/button";
import { useEnvironments } from "../../state/environments";
import { groupAdmissions } from "./presentation";
import { RequestReviewRow } from "./RequestReviewRow";
import { actionableAdmissions, admissionBindingKey, requestBlockedReason } from "./store";
import { useAdmission } from "./useAdmission";

export function RequestsPage() {
  const { environments, isReady } = useEnvironments();
  return (
    <main className="h-full overflow-y-auto">
      <WorkspacePageContainer>
        <header className="space-y-2">
          <h1 className="text-xl font-semibold">Requests</h1>
          <p className="text-sm text-muted-foreground">
            Review incoming tasks on their receiving environment. Dismissing a notification leaves
            requests here.
          </p>
          <Link to="/settings/connections" className="text-xs underline underline-offset-2">
            Configure background evaluation
          </Link>
        </header>
        {!isReady ? (
          <p role="status">Loading environments…</p>
        ) : environments.length === 0 ? (
          <p role="status">Connect a receiving environment to review its requests.</p>
        ) : null}
        {environments.map((environment) => (
          <EnvironmentRequests
            key={environment.environmentId}
            environmentId={environment.environmentId}
            label={environment.label}
          />
        ))}
      </WorkspacePageContainer>
    </main>
  );
}

export function EnvironmentRequests({
  environmentId,
  label,
}: {
  readonly environmentId: EnvironmentId;
  readonly label: string;
}) {
  const state = useAdmission(environmentId);
  const { snapshot, store, loadFailed, error } = state;
  const groups = groupAdmissions(snapshot?.pending ?? []);
  const held = actionableAdmissions(state).length;
  return (
    <section className="min-w-0 space-y-3" aria-label={`Requests on ${label}`}>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-sm font-medium wrap-anywhere">
          {label}{" "}
          <span className="text-muted-foreground">
            ·{" "}
            {snapshot
              ? `${held} need review${loadFailed ? " (last known)" : ""}`
              : loadFailed
                ? "inaccessible"
                : "loading"}
          </span>
        </h2>
        <Button
          size="sm"
          variant="outline"
          onClick={() => {
            void store.refresh();
          }}
        >
          Refresh
        </Button>
      </div>
      {loadFailed ? (
        <p role="alert" className="text-sm text-muted-foreground">
          Requests are inaccessible or could not be loaded for this session. Owner review requires
          authorization on {label}; this connection may not support it. Retrying automatically.
          {snapshot ? " Showing last known state; actions are disabled." : ""}
        </p>
      ) : !snapshot ? (
        <p role="status" className="text-sm text-muted-foreground">
          Loading requests…
        </p>
      ) : snapshot.pending.length === 0 ? (
        <p className="text-sm text-muted-foreground">No pending requests on this environment.</p>
      ) : null}
      {error ? (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      ) : null}
      {state.atLimit ? (
        <p role="status" className="text-xs text-muted-foreground">
          Showing at most 200 pending requests, oldest first. More may be waiting; resolve these and
          refresh to load later requests.
        </p>
      ) : null}
      {groups.map((group) => (
        <div key={group.key} className="min-w-0 space-y-2">
          <h3 className="text-xs font-medium text-muted-foreground wrap-anywhere">
            {group.source} · {group.category} · {group.records.length}
          </h3>
          {group.category === "Evaluator service failure" ? (
            <p className="text-xs text-muted-foreground">
              Evaluation did not complete. These requests are held; this is not a finding of unsafe
              content.
            </p>
          ) : null}
          {group.records.map((record) => (
            <RequestReviewRow
              key={admissionBindingKey(record)}
              record={record}
              policyRevision={snapshot!.policy.revision}
              blocked={requestBlockedReason(state, admissionBindingKey(record))}
              onDecide={(input) => {
                void store.decide(input);
              }}
            />
          ))}
        </div>
      ))}
    </section>
  );
}
