export function pullRequestReadPause(error: unknown): { retryAt: number | null } | null {
  if (
    typeof error !== "object" ||
    error === null ||
    !("_tag" in error) ||
    error._tag !== "PullRequestOperationError" ||
    !("reason" in error) ||
    error.reason !== "rate-limited"
  ) {
    return null;
  }
  return {
    retryAt:
      "retryAt" in error && typeof error.retryAt === "number" && Number.isFinite(error.retryAt)
        ? error.retryAt
        : null,
  };
}

export function PullRequestReadPauseBanner({
  detailFailure,
  activityFailure,
  hasDetail,
  observedAt,
}: {
  readonly detailFailure: unknown;
  readonly activityFailure: unknown;
  readonly hasDetail: boolean;
  readonly observedAt: number | undefined;
}) {
  const pause = pullRequestReadPause(detailFailure) ?? pullRequestReadPause(activityFailure);
  if (!pause) return null;
  return (
    <div role="status" className="shrink-0 border-b px-3 py-2 text-xs text-muted-foreground">
      Pull request reads are paused by the provider.
      {pause.retryAt !== null ? (
        <>
          {" Retry after "}
          <time dateTime={new Date(pause.retryAt).toISOString()}>
            {new Date(pause.retryAt).toLocaleString()}
          </time>
          .
        </>
      ) : null}
      {hasDetail ? (
        observedAt !== undefined ? (
          <>
            {" Showing details last observed "}
            <time dateTime={new Date(observedAt).toISOString()}>
              {new Date(observedAt).toLocaleString()}
            </time>
            .
          </>
        ) : (
          " Showing previously loaded details; observation time is unavailable."
        )
      ) : null}
    </div>
  );
}
