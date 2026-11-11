import type { OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import * as Option from "effect/Option";

import { EMPTY_THREAD_HISTORY_META, type ThreadHistoryMeta } from "./threadHistoryMerge.ts";

export type EnvironmentThreadStatus = "empty" | "cached" | "synchronizing" | "live" | "deleted";

export interface EnvironmentThreadState {
  readonly data: Option.Option<OrchestrationV2ThreadProjection>;
  /** Global orchestration event sequence applied to data; absent evidence is unknown. */
  readonly appliedSequence?: number | null;
  readonly status: EnvironmentThreadStatus;
  readonly error: Option.Option<string>;
  /**
   * Errors here are thread/history-local and must not be promoted into the environment
   * disconnect path.
   */
  readonly history: ThreadHistoryMeta;
}

export const EMPTY_ENVIRONMENT_THREAD_STATE: EnvironmentThreadState = {
  data: Option.none(),
  appliedSequence: null,
  status: "empty",
  error: Option.none(),
  history: EMPTY_THREAD_HISTORY_META,
};
