import {
  ImportedHistoryStart,
  type ImportedHistoryReview,
  type ImportedHistoryOutcome,
  EnvironmentId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { importedHistoryCanonicalJson } from "@t3tools/shared/jones/importedHistoryCanonical";

export type ImportedHistoryCorrelation = {
  readonly environmentId: EnvironmentId;
  readonly draftIdentity: string;
  readonly command: ImportedHistoryStart;
  readonly commandDigest: string;
  readonly deliveryDigest: string;
};
export type ImportedHistoryChoiceState =
  | {
      readonly status: "pending" | "rejected" | "unknown";
      readonly intentAccepted: boolean;
      readonly reason: string | null;
    }
  | {
      readonly status: "held";
      readonly intentAccepted: false;
      readonly effectOutcome: "known_no_effect";
      readonly reason: string;
    };
export class ImportedHistoryLockUnavailable extends Error {
  readonly _tag = "ImportedHistoryLockUnavailable";
  constructor() {
    super("Cross-tab coordination is unavailable. No start was submitted.");
  }
}
const lockUnavailableState = (
  error: ImportedHistoryLockUnavailable,
): ImportedHistoryChoiceState => ({
  status: "held",
  intentAccepted: false,
  effectOutcome: "known_no_effect",
  reason: error.message,
});
export function resolveImportedHistoryReview(review: ImportedHistoryReview | null | undefined) {
  return review?.status === "available" &&
    review.reviewedBasis !== null &&
    /^[0-9a-f]{64}$/.test(review.reviewedBasis)
    ? { status: "available" as const, reviewedBasis: review.reviewedBasis, reason: null }
    : {
        status: "unavailable" as const,
        reviewedBasis: null,
        reason: review?.reason ?? "Imported history review is unavailable.",
      };
}
export function resolveImportedHistoryOutcome(
  outcome: ImportedHistoryOutcome | null | undefined,
  expected: ImportedHistoryCorrelation,
): ImportedHistoryChoiceState {
  const unknown = (reason: string): ImportedHistoryChoiceState => ({
    status: "unknown",
    intentAccepted: false,
    reason,
  });
  if (outcome == null)
    return unknown("Response unavailable. Observe this same command before continuing.");
  if (
    outcome.threadId !== expected.command.threadId ||
    outcome.commandId !== expected.command.commandId ||
    outcome.reviewedBasis !== expected.command.reviewedBasis ||
    outcome.commandDigest !== expected.commandDigest ||
    outcome.deliveryDigest !== expected.deliveryDigest
  ) {
    return unknown("The observation does not match the reviewed command.");
  }
  if (outcome.status === "rejected")
    return {
      status: "rejected",
      intentAccepted: false,
      reason: outcome.reason ?? "The reviewed choice was rejected.",
    };
  if (outcome.status === "accepted")
    return {
      status: "pending",
      intentAccepted: true,
      reason: "Choice accepted; execution is not confirmed.",
    };
  return unknown(outcome.reason ?? "The choice needs reconciliation.");
}
export function encodeImportedHistoryCorrelation(value: ImportedHistoryCorrelation): string {
  return importedHistoryCanonicalJson({
    ...value,
    command: Schema.encodeSync(ImportedHistoryStart)(value.command),
  });
}
export function decodeImportedHistoryCorrelation(raw: string): ImportedHistoryCorrelation {
  const shape = Schema.Struct({
    environmentId: EnvironmentId,
    draftIdentity: Schema.String,
    command: ImportedHistoryStart,
    commandDigest: Schema.String,
    deliveryDigest: Schema.String,
  });
  const decoded = Schema.decodeUnknownSync(shape)(JSON.parse(raw));
  return decoded;
}
export interface ImportedHistoryCorrelationStorage {
  withLock: <A>(operation: () => Promise<A>) => Promise<A>;
  read: () => ImportedHistoryCorrelation | null;
  reserve: (value: ImportedHistoryCorrelation) => void;
  remove: (value: ImportedHistoryCorrelation) => void;
}
export function createImportedHistoryChoiceController(
  storage: ImportedHistoryCorrelationStorage,
  transport: {
    start: (value: ImportedHistoryStart) => Promise<ImportedHistoryOutcome | null>;
    observe: (
      value: Pick<ImportedHistoryStart, "threadId" | "commandId">,
    ) => Promise<ImportedHistoryOutcome | null>;
  },
) {
  let inFlight = false;
  const observe = async (correlation: ImportedHistoryCorrelation) => {
    try {
      return resolveImportedHistoryOutcome(
        await transport.observe(correlation.command),
        correlation,
      );
    } catch {
      return resolveImportedHistoryOutcome(null, correlation);
    }
  };
  return {
    async start(
      value: ImportedHistoryCorrelation,
      unchanged: () => boolean,
    ): Promise<ImportedHistoryChoiceState> {
      if (inFlight)
        return {
          status: "unknown",
          intentAccepted: false,
          reason: "The choice is already being observed.",
        };
      inFlight = true;
      let submitted = false;
      try {
        return await storage.withLock(async (): Promise<ImportedHistoryChoiceState> => {
          const existing = storage.read();
          if (existing !== null) {
            const state = await observe(existing);
            if (state.status === "rejected") storage.remove(existing);
            return state;
          }
          if (!unchanged())
            return {
              status: "rejected",
              intentAccepted: false,
              reason: "The draft or target changed. Review it again.",
            };
          storage.reserve(value);
          const readback = storage.read();
          if (
            readback === null ||
            encodeImportedHistoryCorrelation(readback) !== encodeImportedHistoryCorrelation(value)
          )
            throw new Error("Correlation readback failed.");
          if (!unchanged())
            return {
              status: "unknown",
              intentAccepted: false,
              reason:
                "The draft or target changed after reservation. Observe the reserved command.",
            };
          let outcome: ImportedHistoryOutcome | null;
          try {
            submitted = true;
            outcome = await transport.start(value.command);
          } catch {
            outcome = null;
          }
          const state = resolveImportedHistoryOutcome(outcome, value);
          if (state.status === "rejected") storage.remove(value);
          return state;
        });
      } catch (error) {
        if (error instanceof ImportedHistoryLockUnavailable) return lockUnavailableState(error);
        return {
          status: "unknown",
          intentAccepted: false,
          reason: submitted
            ? "The choice was submitted; observe the reserved command before continuing."
            : "The choice could not be saved. No new start was submitted.",
        };
      } finally {
        inFlight = false;
      }
    },
    async observe(): Promise<ImportedHistoryChoiceState> {
      if (inFlight)
        return {
          status: "unknown",
          intentAccepted: false,
          reason: "The choice is already being observed.",
        };
      inFlight = true;
      try {
        return await storage.withLock(async (): Promise<ImportedHistoryChoiceState> => {
          const saved = storage.read();
          if (saved === null)
            return {
              status: "unknown",
              intentAccepted: false,
              reason: "No saved command is available.",
            };
          const state = await observe(saved);
          if (state.status === "rejected") storage.remove(saved);
          return state;
        });
      } catch (error) {
        if (error instanceof ImportedHistoryLockUnavailable) return lockUnavailableState(error);
        return {
          status: "unknown",
          intentAccepted: false,
          reason: "The saved choice could not be observed.",
        };
      } finally {
        inFlight = false;
      }
    },
  };
}
