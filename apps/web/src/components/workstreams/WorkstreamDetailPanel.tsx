import { ExternalLinkIcon, RefreshCwIcon } from "lucide-react";
import { canonicalGitHubPullRequestUrl, type WorkstreamPrLocator } from "@t3tools/contracts";

import { Badge } from "../ui/badge";
import { Button } from "../ui/button";
import type {
  WorkstreamHistoryEntry,
  WorkstreamMemberPresentation,
  WorkstreamPresentation,
  WorkstreamReceipts,
} from "./types";
import { WorkstreamHistoryList } from "./WorkstreamHistoryList";
import { WorkstreamReceiptStatus } from "./WorkstreamReceiptStatus";

export interface WorkstreamRelationshipPresentation {
  readonly workstreamId: string;
  readonly name: string;
  readonly relation: "continues" | "continued-by" | "supersedes" | "superseded-by";
}

export interface LinkedPullRequestPresentation {
  readonly ref: string;
  readonly label: string;
  readonly locator: WorkstreamPrLocator;
  readonly status: "open" | "closed" | "merged" | "unknown";
  readonly refreshing: boolean;
}

export interface WorkstreamDetailPanelProps {
  readonly workstream: WorkstreamPresentation;
  readonly relationships: readonly WorkstreamRelationshipPresentation[];
  readonly linkedPullRequests: readonly LinkedPullRequestPresentation[];
  readonly history: readonly WorkstreamHistoryEntry[];
  readonly hasMoreHistory: boolean;
  readonly receipts: WorkstreamReceipts;
  readonly onSelectMember: (member: WorkstreamMemberPresentation) => void;
  readonly onSelectRelationship: (workstreamId: string) => void;
  readonly onRefreshPullRequest: (ref: string) => void;
  readonly onLoadMoreMembers: () => void;
  readonly onLoadMoreHistory: () => void;
}

export function WorkstreamDetailPanel(props: WorkstreamDetailPanelProps) {
  return (
    <article aria-labelledby="workstream-detail-title" className="space-y-6 p-4">
      <header>
        <div className="flex flex-wrap items-center gap-2">
          <h2 className="font-semibold font-semibold text-lg" id="workstream-detail-title">
            {props.workstream.name}
          </h2>
          <Badge variant="outline">{props.workstream.lifecycle}</Badge>
        </div>
        <p className="mt-1 text-muted-foreground text-sm">
          {props.workstream.memberCount} members · owner order {props.workstream.order + 1}
        </p>
      </header>

      <section aria-labelledby="workstream-members-title">
        <h3 className="font-medium text-sm" id="workstream-members-title">
          Current members
        </h3>
        <ul className="mt-2 divide-y divide-border/70 rounded-lg border border-border/70">
          {props.workstream.members.map((member) => (
            <li key={member.ref}>
              <button
                className="flex w-full items-center gap-2 px-3 py-2 text-left hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                onClick={() => props.onSelectMember(member)}
                type="button"
              >
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-sm">{member.title}</span>
                  <span className="block truncate text-muted-foreground text-xs">
                    {member.kind}
                    {member.subtitle ? ` · ${member.subtitle}` : ""}
                  </span>
                </span>
                <Badge variant={member.association === "primary" ? "info" : "secondary"}>
                  {member.association}
                </Badge>
              </button>
            </li>
          ))}
        </ul>
        {props.workstream.hasMoreMembers && (
          <Button className="mt-3" onClick={props.onLoadMoreMembers} size="xs" variant="outline">
            Load more members
          </Button>
        )}
      </section>

      <section aria-labelledby="workstream-relations-title">
        <h3 className="font-medium text-sm" id="workstream-relations-title">
          Continuation and supersession
        </h3>
        {props.relationships.length === 0 ? (
          <p className="mt-2 text-muted-foreground text-sm">No relationships recorded.</p>
        ) : (
          <ul className="mt-2 space-y-1">
            {props.relationships.map((relationship) => (
              <li key={`${relationship.relation}:${relationship.workstreamId}`}>
                <Button
                  onClick={() => props.onSelectRelationship(relationship.workstreamId)}
                  size="xs"
                  variant="ghost"
                >
                  {relationship.relation} · {relationship.name}
                </Button>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section aria-labelledby="workstream-prs-title">
        <h3 className="font-medium text-sm" id="workstream-prs-title">
          Manually linked pull requests
        </h3>
        <ul className="mt-2 space-y-2">
          {props.linkedPullRequests.map((pullRequest) => (
            <li
              className="flex items-center gap-2 rounded-lg border border-border/70 p-2"
              key={pullRequest.ref}
            >
              <a
                className="min-w-0 flex-1 truncate text-sm hover:underline"
                href={canonicalGitHubPullRequestUrl(pullRequest.locator)}
              >
                {pullRequest.label} <ExternalLinkIcon aria-hidden className="inline size-3" />
              </a>
              <Badge variant="outline">{pullRequest.status}</Badge>
              <Button
                aria-label={`Refresh status for ${pullRequest.label}`}
                disabled={pullRequest.refreshing}
                onClick={() => props.onRefreshPullRequest(pullRequest.ref)}
                size="icon-xs"
                variant="ghost-muted"
              >
                <RefreshCwIcon />
              </Button>
            </li>
          ))}
        </ul>
      </section>

      <WorkstreamReceiptStatus receipts={props.receipts} />
      <WorkstreamHistoryList
        entries={props.history}
        hasMore={props.hasMoreHistory}
        onLoadMore={props.onLoadMoreHistory}
      />
    </article>
  );
}
