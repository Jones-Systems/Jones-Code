import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const ProviderControlDeadline = Context.Reference<number | undefined>(
  "jones/provider/ProviderControlDeadline",
  { defaultValue: () => undefined },
);

export class ProviderControlOutcomeUnknown extends Schema.TaggedError<ProviderControlOutcomeUnknown>()(
  "ProviderControlOutcomeUnknown",
  {
    driver: Schema.String,
    providerSessionId: Schema.String,
    nativeThreadId: Schema.String,
    operation: Schema.Literals(["interrupt", "retire"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return "Native provider retirement is unconfirmed; new work is fenced pending reconciliation.";
  }
}

const isProviderControlOutcomeUnknown = Schema.is(ProviderControlOutcomeUnknown);

// Adapter and service errors retain their immediate cause. Do not classify diagnostic strings.
export function findProviderControlOutcomeUnknown(
  input: unknown,
): ProviderControlOutcomeUnknown | undefined {
  const pending: Array<unknown> = [input];
  const seen = new Set<unknown>();
  for (let visited = 0; visited < 64 && pending.length > 0; visited++) {
    const current = pending.pop();
    if (current === null || typeof current !== "object" || seen.has(current)) continue;
    if (isProviderControlOutcomeUnknown(current)) return current;
    seen.add(current);
    if (Cause.isCause(current)) {
      for (const reason of current.reasons.slice(0, 16)) {
        if (Cause.isFailReason(reason)) pending.push(reason.error);
        else if (Cause.isDieReason(reason)) pending.push(reason.defect);
      }
    } else {
      pending.push(Reflect.get(current, "cause"));
    }
  }
  return undefined;
}

// These receipts follow exact queued objects, never a wire field. Retirement waits
// for the raw provider-turn SQL write AND processing of its following root terminal.
// That second write can intentionally be rejected for superseded history; it must
// still reach the ingestor before the captured producer is retired.
const terminalIngestion = new WeakMap<
  object,
  { readonly requiresWrite: boolean; readonly confirm: () => void }
>();

export interface ProviderControlTerminalDrain {
  readonly capture: (event: {
    readonly type:
      | "turn.terminal"
      | "provider_turn.updated"
      | "turn_item.updated"
      | "node.updated"
      | "subagent.updated";
  }) => void;
  readonly awaitIngested: Effect.Effect<void>;
}

export function makeProviderControlTerminalDrain(): ProviderControlTerminalDrain {
  const pending = new Set<Deferred.Deferred<void>>();
  return {
    capture: (event) => {
      if (terminalIngestion.has(event)) return;
      const persisted = Deferred.makeUnsafe<void>();
      pending.add(persisted);
      terminalIngestion.set(event, {
        requiresWrite: event.type !== "turn.terminal",
        confirm: () => {
          pending.delete(persisted);
          Deferred.doneUnsafe(persisted, Effect.void);
        },
      });
    },
    // The owner joins after its raw stream has drained, without holding a producer
    // permit. Completed turns leave the set immediately, so healthy reuse is O(1).
    awaitIngested: Effect.suspend(() =>
      Effect.forEach([...pending], Deferred.await, { discard: true }),
    ),
  };
}

export function confirmProviderControlTerminalIngested(
  event: object,
  storedEventCount: number,
): void {
  const receipt = terminalIngestion.get(event);
  if (receipt === undefined || (receipt.requiresWrite && storedEventCount === 0)) return;
  terminalIngestion.delete(event);
  receipt.confirm();
}
