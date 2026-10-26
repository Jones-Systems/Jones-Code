import type { EnvironmentId, OrchestrationV2ThreadShell, ThreadId } from "@t3tools/contracts";
import type { ComposerThreadItem } from "../composerThreadItems.ts";

export interface JonesComposerThreadCandidate {
  readonly environmentId: EnvironmentId;
  readonly id: ThreadId;
  readonly title: string;
  readonly updatedAt: string;
  readonly archivedAt: string | null;
  readonly lineage?: Pick<OrchestrationV2ThreadShell["lineage"], "relationshipToParent">;
}

export interface JonesComposerThreadMatchInput {
  readonly shells: ReadonlyArray<JonesComposerThreadCandidate>;
  readonly environmentId: EnvironmentId;
  readonly excludeThreadId: ThreadId | null;
  readonly query: string;
  /** Accepted for callers sharing the upstream interface; Jones ranking uses updatedAt. */
  readonly now?: number;
}

function normalizeTitle(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim();
}

function matchRelevance(title: string, query: string, terms: ReadonlyArray<string>): number {
  const words = title.split(" ");
  if (!terms.every((term) => words.some((word) => word.includes(term)))) return 0;
  if (title === query) return 5;
  if (` ${title} `.includes(` ${query} `)) return 4;
  if (terms.every((term) => words.includes(term))) return 3;
  if (terms.every((term) => words.some((word) => word.startsWith(term)))) return 2;
  return 1;
}

export function rankComposerThreadCandidates<
  T extends { readonly title: string; readonly updatedAt: string },
>(candidates: ReadonlyArray<T>, query: string): T[] {
  const normalizedQuery = normalizeTitle(query);
  if (!normalizedQuery) return [];
  const terms = normalizedQuery.split(" ");

  return candidates
    .map((candidate) => ({
      candidate,
      relevance: matchRelevance(normalizeTitle(candidate.title), normalizedQuery, terms),
    }))
    .filter(({ relevance }) => relevance > 0)
    .sort(
      (left, right) =>
        right.relevance - left.relevance ||
        right.candidate.updatedAt.localeCompare(left.candidate.updatedAt),
    )
    .map(({ candidate }) => candidate);
}

export function matchJonesComposerThreadItems(
  input: JonesComposerThreadMatchInput,
): ComposerThreadItem[] {
  return rankComposerThreadCandidates(
    input.shells.filter(
      (shell) =>
        shell.environmentId === input.environmentId &&
        shell.id !== input.excludeThreadId &&
        shell.archivedAt === null &&
        shell.lineage?.relationshipToParent !== "subagent",
    ),
    input.query,
  )
    .slice(0, 5)
    .map((shell) => ({
      id: `thread:${shell.environmentId}:${shell.id}`,
      type: "thread",
      thread: { environmentId: shell.environmentId, threadId: shell.id },
      label: shell.title,
      description: "Thread",
    }));
}
