import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import { TrimmedNonEmptyString } from "../baseSchemas.ts";
import { PreviewTabId } from "../preview.ts";
import {
  PreviewAutomationClientId,
  PreviewAutomationConnectionId,
  PreviewAutomationRuntimeIdentity,
} from "../previewAutomation.ts";

const Handle = Schema.Struct({
  requestId: TrimmedNonEmptyString.check(Schema.isMaxLength(128)),
  clientId: PreviewAutomationClientId,
  connectionId: PreviewAutomationConnectionId,
});
export const PreviewRecordingOperationInput = Schema.Struct({
  tabId: PreviewTabId,
  handle: Schema.optionalKey(Handle),
});
export type PreviewRecordingOperationInput = typeof PreviewRecordingOperationInput.Type;

const Operation = Schema.Literals(["recordingStart", "recordingStop"]);
const Timestamp = Schema.String.check(Schema.isMaxLength(128)).check(
  Schema.makeFilter(
    (value: string) => {
      const parts =
        /^(\d{4})-(\d{2})-(\d{2})T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/.exec(
          value,
        );
      if (parts === null || Option.isNone(DateTime.make(value))) return false;
      const year = Number(parts[1]);
      const month = Number(parts[2]);
      const day = Number(parts[3]);
      const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
      const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
      return month >= 1 && month <= 12 && day >= 1 && day <= days[month - 1]!;
    },
    {
      expected: "a valid bounded RFC 3339 date-time",
      toJsonSchema: () => [{ format: "date-time" }, true],
    },
  ),
);

export const PreviewRecordingReportedStart = Schema.Struct({
  tabId: PreviewTabId,
  recording: Schema.Boolean,
  startedAt: Schema.NullOr(Timestamp),
});
export type PreviewRecordingReportedStart = typeof PreviewRecordingReportedStart.Type;

const DispatchEvidence = Schema.Struct({
  basis: Schema.Literal("dispatch_time"),
  runtimeIdentity: Schema.NullOr(PreviewAutomationRuntimeIdentity),
  attachmentGeneration: Schema.NullOr(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
});
const Binding = { handle: Handle, operation: Operation };
const Evidence = { dispatchEvidence: Schema.optional(DispatchEvidence) };
const Metadata = Schema.Union([
  Schema.Struct({ status: Schema.Literal("valid"), reportedStart: PreviewRecordingReportedStart }),
  Schema.Struct({ status: Schema.Literals(["invalid", "not_projected"]) }),
]);
const Settled = Schema.Struct({
  status: Schema.Literal("settled"),
  ...Binding,
  ...Evidence,
  reportedOutcome: Schema.Literals(["success", "failure", "not_started", "unclassifiable"]),
  metadata: Metadata,
  settledAt: Timestamp,
});

export const PreviewRecordingOperationResult = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  tabId: PreviewTabId,
  result: Schema.Union([
    Schema.Struct({
      status: Schema.Literal("pending"),
      ...Binding,
      dispatched: Schema.Literal(false),
    }),
    Schema.Struct({
      status: Schema.Literal("pending"),
      ...Binding,
      dispatched: Schema.Literal(true),
      dispatchEvidence: DispatchEvidence,
    }),
    Schema.Struct({ status: Schema.Literal("unknown"), ...Binding, ...Evidence }),
    Settled,
    Schema.Struct({
      status: Schema.Literal("ambiguous"),
      candidates: Schema.Array(
        Schema.Struct({
          ...Binding,
          status: Schema.Literals(["pending", "unknown", "settled"]),
        }),
      )
        .check(Schema.isMinLength(2))
        .check(Schema.isMaxLength(8)),
      truncated: Schema.Boolean,
    }),
    Schema.Struct({
      status: Schema.Literal("unavailable"),
      reason: Schema.Literal("not_retained"),
    }),
  ]),
});
export type PreviewRecordingOperationResult = typeof PreviewRecordingOperationResult.Type;
