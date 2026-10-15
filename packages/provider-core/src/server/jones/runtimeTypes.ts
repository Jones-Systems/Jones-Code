import type { ProviderRuntimeBinding } from "@t3tools/contracts";

declare const privateGuard: unique symbol;
export interface NativeProviderExecutionGuard {
  readonly [privateGuard]: true;
}

export type ProviderRuntimeObservation =
  | {
      readonly status: "working" | "monitoring" | "busy" | "idle";
      readonly binding: ProviderRuntimeBinding;
      readonly observedAt: string;
      readonly backgroundCoverage?: "complete" | "partial";
    }
  | { readonly status: "unknown"; readonly reason: string };

export type ProviderGoalReasonCode =
  | "goal_null"
  | "goal_present"
  | "no_session"
  | "session_stopped"
  | "instance_mismatch"
  | "native_cursor_missing"
  | "unsupported"
  | "timeout"
  | "malformed"
  | "goal_field_omitted"
  | "rpc_error"
  | "context_changed";

export interface ProviderGoalReadResult {
  readonly nativeThreadId: string | null;
  readonly state: "inactive" | "active" | "unknown";
  readonly reasonCode: ProviderGoalReasonCode;
}
