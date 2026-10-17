import type {
  NativeWorkstreamThreadAssurance,
  WorkstreamDtoPage,
} from "@t3tools/client-runtime/state/workstreams";
import type { EnvironmentHttpAuthHeaders } from "@t3tools/client-runtime/authorization";
import type {
  WorkstreamCommand,
  WorkstreamReceipt,
  WorkstreamAppearance,
  WorkstreamAppearanceWrite,
} from "@t3tools/contracts";
import type * as Effect from "effect/Effect";
import type { WorkstreamClient, WorkstreamUrls, WorkstreamUrlSelector } from "./gateway";
import type { MobileWorkstreamGroup, MobileWorkstreamSnapshot } from "./projection";

export interface MobileWorkstreams {
  readonly saveAppearance: (
    snapshot: MobileWorkstreamSnapshot,
    input: WorkstreamAppearanceWrite,
  ) => Promise<WorkstreamAppearance>;
  readonly groups: readonly MobileWorkstreamGroup[];
  readonly secondaryLabelsByKey: ReadonlyMap<string, readonly string[]>;
  readonly assuranceByKey: ReadonlyMap<string, NativeWorkstreamThreadAssurance>;
  /** Only these rows may be labeled Unassigned; other ungrouped rows are unknown. */
  readonly confirmedUnassignedKeys: ReadonlySet<string>;
  readonly conflictingThreadKeys: ReadonlySet<string>;
  /** Some shown Workstreams are a last good load that could not be refreshed. */
  readonly stale: boolean;
  readonly snapshots: readonly MobileWorkstreamSnapshot[];
  readonly enabled: boolean;
  readonly toggleEnabled: () => void;
  readonly collapsedKeys: ReadonlySet<string>;
  readonly toggleGroup: (key: string) => void;
  readonly bindingRevision: string;
  readonly readiness: string;
  readonly error: string | null;
  readonly refresh: () => void;
  readonly reorderGroup: (
    group: MobileWorkstreamGroup,
    direction: -1 | 1,
  ) => Promise<WorkstreamReceipt>;
  readonly submit: (
    snapshot: MobileWorkstreamSnapshot,
    action: WorkstreamCommand["action"],
  ) => Promise<WorkstreamReceipt>;
  readonly read: <A, E>(
    snapshot: MobileWorkstreamSnapshot,
    url: WorkstreamUrlSelector,
    run: (client: WorkstreamClient, headers: EnvironmentHttpAuthHeaders) => Effect.Effect<A, E>,
  ) => Promise<A>;
  readonly pages: <Item>(
    snapshot: MobileWorkstreamSnapshot,
    url: (urls: WorkstreamUrls, cursor?: string) => string,
    run: (
      client: WorkstreamClient,
      headers: EnvironmentHttpAuthHeaders,
      cursor?: string,
    ) => Effect.Effect<WorkstreamDtoPage<Item>, unknown>,
  ) => Promise<WorkstreamDtoPage<Item>>;
}
