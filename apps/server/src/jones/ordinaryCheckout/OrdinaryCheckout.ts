import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type {
  Admission,
  Binding,
  Birth,
  CheckoutError,
  Evidence,
  LifecycleEvent,
  Owner,
  Relation,
  Source,
  Target,
  Use,
} from "./CheckoutProtocol.ts";

export interface CommandIdentity {
  readonly commandId: string;
  readonly commandType: string;
  readonly receiptThreadId: string;
  readonly command: unknown;
  readonly subjectThreadId: string;
  readonly origin: {
    readonly kind: "command" | "queue" | "delegated" | "scheduled" | "prepared";
    readonly parentThreadId: string | null;
    readonly requestId: string | null;
  };
}
export interface Accept extends CommandIdentity {
  readonly admissionId: string;
  readonly target: Target;
  readonly birth: Birth;
  readonly receiptSequence: number;
  readonly basisEventId: string;
  readonly basisSequence: number;
  readonly relation: Relation;
  readonly leaseId: string;
  readonly now: string;
  readonly expiresAt: string;
  readonly expectedOwner: { readonly epoch: number; readonly revision: number } | null;
  readonly effects: readonly Link[];
}
export interface Link {
  readonly effectId: string;
  readonly request: unknown;
  readonly role: "launch" | "control" | "checkpoint" | "rollback" | "continuation" | "preparation";
}
export interface DerivedLink {
  readonly operationId: string;
  readonly expectedRevision: number;
  readonly admissionId: string;
  readonly basisEventId: string;
  readonly basisSequence: number;
  readonly link: Link;
  readonly now: string;
}
export type Replay =
  | { readonly kind: "absent" }
  | { readonly kind: "historical"; readonly receiptSequence: number }
  | { readonly kind: "accepted"; readonly admissions: readonly Admission[] };
export interface Conflict {
  readonly kind: "ordinary" | "native" | "deletion" | "legacy";
  readonly checkoutKey: string;
  readonly owner: Owner | null;
}
export interface EventInput {
  readonly binding: Binding;
  readonly evidence: Evidence;
  readonly expectedRevision: number;
}
export interface TransitionInput {
  readonly operationId: string;
  readonly expectedRevision: number;
  readonly target: Target;
  readonly binding: Binding;
  readonly evidence: Evidence;
  readonly expectedOwner: { readonly epoch: number; readonly revision: number };
}

// DC-B supplies this only while it owns the newly accepted receipt transaction.
// It validates the complete decoded command, subject/run relation and planned target set.
// Historical receipt replay never creates this capability.
export class CheckoutAcceptanceAuthority extends Context.Service<
  CheckoutAcceptanceAuthority,
  {
    readonly assertNewAcceptance: (input: Accept) => Effect.Effect<void, CheckoutError>;
    readonly assertDerivedLink: (
      input: DerivedLink,
      admission: Admission,
    ) => Effect.Effect<void, CheckoutError>;
  }
>()("t3/jones/ordinaryCheckout/OrdinaryCheckout/CheckoutAcceptanceAuthority") {}

// The existing executor owner must provide this service. No JSON decoder, digest,
// persisted row or default layer can issue a current capability or retirement proof.
export class CheckoutExecutorAuthority extends Context.Service<
  CheckoutExecutorAuthority,
  {
    readonly assertCurrent: (binding: Binding) => Effect.Effect<void, CheckoutError>;
    readonly reconcile: (
      binding: Binding,
      evidence: Evidence,
    ) => Effect.Effect<void, CheckoutError>;
  }
>()("t3/jones/ordinaryCheckout/OrdinaryCheckout/CheckoutExecutorAuthority") {}

// Explicit owner disposition is independent of physical retirement evidence.
export class CheckoutOwnershipAuthority extends Context.Service<
  CheckoutOwnershipAuthority,
  {
    readonly authorize: (input: {
      readonly action: "release" | "target_transition" | "handoff";
      readonly owner: Owner;
      readonly operationId: string;
      readonly nextThreadId: string | null;
    }) => Effect.Effect<void, CheckoutError>;
  }
>()("t3/jones/ordinaryCheckout/OrdinaryCheckout/CheckoutOwnershipAuthority") {}

type Result<A> = Effect.Effect<A, CheckoutError>;
type Executed<A> = Effect.Effect<A, CheckoutError, CheckoutExecutorAuthority>;
export class OrdinaryCheckout extends Context.Service<
  OrdinaryCheckout,
  {
    readonly replay: (identity: CommandIdentity) => Result<Replay>;
    readonly accept: (
      input: Accept,
    ) => Effect.Effect<Admission, CheckoutError, CheckoutAcceptanceAuthority>;
    readonly acceptMany: (
      inputs: readonly Accept[],
    ) => Effect.Effect<readonly Admission[], CheckoutError, CheckoutAcceptanceAuthority>;
    readonly linkDerived: (
      input: DerivedLink,
    ) => Effect.Effect<Use, CheckoutError, CheckoutAcceptanceAuthority>;
    readonly readOwner: (checkoutKey: string) => Result<Owner | null>;
    readonly readConflict: (checkoutKey: string) => Result<readonly Conflict[]>;
    readonly readAdmission: (admissionId: string) => Result<Admission | null>;
    readonly readEffect: (effectId: string) => Result<readonly Admission[]>;
    readonly reserve: (input: {
      readonly admissionId: string;
      readonly source: Source;
      readonly now: string;
      readonly binding: Binding;
      readonly evidence: Evidence;
    }) => Executed<Use>;
    readonly start: (input: EventInput & { readonly target: Target }) => Executed<Use>;
    readonly join: (input: EventInput & { readonly target: Target }) => Executed<Use>;
    readonly renew: (
      input: EventInput & {
        readonly target: Target;
        readonly expiresAt: string;
        readonly expectedOwnerRevision: number;
      },
    ) => Executed<Use>;
    readonly reconcileEvidence: (input: EventInput) => Executed<Use>;
    readonly complete: (input: {
      readonly operationId: string;
      readonly expectedRevision: number;
      readonly now: string;
      readonly target: Target;
    }) => Result<Use>;
    readonly markRestartUnknown: (input: {
      readonly operationId: string;
      readonly expectedRevision: number;
      readonly now: string;
    }) => Result<Use>;
    readonly listUnsettled: (after: string | null, limit: number) => Result<readonly Use[]>;
    readonly events: (operationId: string) => Result<readonly LifecycleEvent[]>;
    readonly release: (
      input: TransitionInput,
    ) => Effect.Effect<
      Owner,
      CheckoutError,
      CheckoutExecutorAuthority | CheckoutOwnershipAuthority
    >;
    readonly handoff: (
      input: TransitionInput & {
        readonly next: {
          readonly target: Target;
          readonly birth: Birth;
          readonly leaseId: string;
          readonly expiresAt: string;
        };
      },
    ) => Effect.Effect<
      Owner,
      CheckoutError,
      CheckoutExecutorAuthority | CheckoutOwnershipAuthority
    >;
    readonly transitionBranch: (
      input: TransitionInput & { readonly after: Target },
    ) => Effect.Effect<
      Owner,
      CheckoutError,
      CheckoutExecutorAuthority | CheckoutOwnershipAuthority
    >;
  }
>()("t3/jones/ordinaryCheckout/OrdinaryCheckout") {}
