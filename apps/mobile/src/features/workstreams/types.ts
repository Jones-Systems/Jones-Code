import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import type {
  MobileThreadOrderSnapshot,
  MobileThreadOrderSource,
} from "../../lib/threadOrderScope";
import type { WorkstreamDtoPage } from "@t3tools/client-runtime/state/workstreams";
import type { EnvironmentHttpAuthHeaders } from "@t3tools/client-runtime/authorization";
import type { WorkstreamCommand, WorkstreamReceipt } from "@t3tools/contracts";
import type * as Effect from "effect/Effect";
import type { WorkstreamClient } from "./gateway";
import type { MobileWorkstreamGroup, MobileWorkstreamSnapshot } from "./projection";

export interface MobileWorkstreams {
  readonly orderSnapshot: MobileThreadOrderSnapshot;
  readonly orderSource: MobileThreadOrderSource;
  readonly removePrimary: (thread: EnvironmentThreadShell) => Promise<void>;
  readonly groups: readonly MobileWorkstreamGroup[];
  readonly secondaryLabelsByKey: ReadonlyMap<string, readonly string[]>;
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
    path: string,
    run: (client: WorkstreamClient, headers: EnvironmentHttpAuthHeaders) => Effect.Effect<A, E>,
  ) => Promise<A>;
  readonly pages: <Item>(
    snapshot: MobileWorkstreamSnapshot,
    path: string,
    run: (
      client: WorkstreamClient,
      headers: EnvironmentHttpAuthHeaders,
      cursor?: string,
    ) => Effect.Effect<WorkstreamDtoPage<Item>, unknown>,
  ) => Promise<WorkstreamDtoPage<Item>>;
}
