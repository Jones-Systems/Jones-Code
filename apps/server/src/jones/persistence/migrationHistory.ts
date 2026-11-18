type MigrationIdentity = readonly [id: number, name: string];
interface RecordedMigration {
  readonly id: number;
  readonly name: string;
}

// Released foreign chains are preservation allowlists, never feature adoption.
const foreignNames = [
  "V2NativeAcceptance",
  "DeletionWorktreeAdmission",
  "OrdinaryCheckoutOwnership",
  "AttachmentCleanup",
  "OrdinaryCheckoutExecutionLifetime",
  "ImportedApplicationAttachments",
  "CommandNormalizationWitness",
];

/** Pure inspection shared by startup and candidate preflight. Never repairs a ledger. */
export function inspectMigrationHistory(
  manifest: ReadonlyArray<MigrationIdentity>,
  recorded: ReadonlyArray<RecordedMigration>,
  owner: "upstream" | "Jones",
):
  | { readonly pending: readonly number[]; readonly error?: undefined }
  | { readonly error: string } {
  const names = new Map(manifest);
  const ids = new Set(recorded.map(({ id }) => id));
  if (
    names.size !== manifest.length ||
    ids.size !== recorded.length ||
    manifest.some(
      ([id]) => !Number.isSafeInteger(id) || id < 1 || (owner === "Jones" && id > 6 && id < 100),
    )
  ) {
    return { error: `Invalid ${owner} migration manifest or duplicate history.` };
  }
  const foreign = recorded.filter(({ id }) => id >= 7 && id < 100).sort((a, b) => a.id - b.id);
  for (const { id, name } of recorded) {
    if ((owner === "upstream" || id >= 100) && id > Math.max(0, ...names.keys()))
      return { error: `Candidate older than this database (${owner} migration ${id}).` };
    const expected =
      owner === "Jones" && id >= 7 && id < 100
        ? id === 7 && foreign.length === 1 && name === "ThreadCreationLookupIndex"
          ? name
          : foreignNames[id - 7]
        : names.get(id);
    if (!Number.isSafeInteger(id) || expected === undefined || expected !== name) {
      return { error: `Unrecognized ${owner} migration history at ${id}_${name}.` };
    }
    if (
      (owner === "upstream" || id <= 6 || id >= 100) &&
      manifest.some(([prior]) => prior < id && !ids.has(prior))
    ) {
      return { error: `${owner} migration history has a missing prefix.` };
    }
  }
  if (
    owner === "Jones" &&
    foreign.length > 0 &&
    ([1, 2, 3, 4, 5, 6].some((id) => !ids.has(id)) ||
      foreign.some(({ id }, index) => id !== index + 7))
  ) {
    return { error: "Jones migration history has a missing foreign prefix." };
  }
  return { pending: manifest.filter(([id]) => !ids.has(id)).map(([id]) => id) };
}

type MigrationRow = { readonly id: number; readonly name: string };

// Previews used V2 ids 53/54; preserve their schema and import progress while reserving main's ids.
function v2PreviewId(history: ReadonlyArray<MigrationRow>): number | "invalid" | undefined {
  const later = history.filter((row) => row.id >= 53);
  const legacy = later.find(
    (row) => row.name === "OrchestrationV2" && (row.id === 53 || row.id === 54),
  );
  if (!legacy) return undefined;
  const valid = later.every(
    (row) =>
      row === legacy ||
      (legacy.id === 54 &&
        ((row.id === 53 && row.name === "PullRequestFilesViewed") ||
          (row.id === 55 && row.name === "RemoveRedundantProjectionIndexes"))),
  );
  return valid ? legacy.id : "invalid";
}

/** The history as startup's preview reconciliation leaves it, or null when startup rejects it. */
export function reconciledV2PreviewHistory(
  history: ReadonlyArray<MigrationRow>,
): ReadonlyArray<MigrationRow> | null {
  const previewId = v2PreviewId(history);
  if (previewId === undefined) return history;
  if (previewId === "invalid") return null;
  return [
    ...history.filter((row) => row.id < 53),
    { id: 53, name: "PullRequestFilesViewed" },
    { id: 54, name: "ProjectionThreadsAutoSettleDisabledAt" },
    { id: 55, name: "OrchestrationV2" },
    ...(history.some((row) => row.id === 55)
      ? [{ id: 56, name: "RemoveRedundantProjectionIndexes" }]
      : []),
  ];
}
