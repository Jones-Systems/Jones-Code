import { EventId, ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as SchemaIssue from "effect/SchemaIssue";

const JsonValue = Schema.String.pipe(
  Schema.decodeTo(Schema.Unknown, {
    decode: SchemaGetter.onSome<unknown, string>((input, options) => {
      try {
        const value: unknown = JSON.parse(input);
        return Effect.succeed(Option.some(value));
      } catch (cause) {
        return Effect.fail(
          new SchemaIssue.InvalidValue({ nativeJsonCause: cause }, input, options),
        );
      }
    }),
    encode: SchemaGetter.forbiddenEncoding,
  }),
);

const BirthTuple = Schema.Tuple([
  Schema.Literal("t3.orchestration-v2.thread-birth/v1"),
  EventId,
  Schema.Int.check(Schema.isGreaterThan(0)),
]);

const BirthTupleJson = BirthTuple.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.onSome<string, typeof BirthTuple.Type>((input, options) => {
      try {
        return Effect.succeed(Option.some(JSON.stringify(input)));
      } catch (cause) {
        return Effect.fail(
          new SchemaIssue.InvalidValue({ nativeJsonCause: cause }, input, options),
        );
      }
    }),
    encode: SchemaGetter.forbiddenEncoding,
  }),
);

export const decodeJson = Schema.decodeEffect(JsonValue);
export const encodeBirthTupleJson = Schema.decodeEffect(BirthTupleJson);

// Stock JSON getters discard the native exception. Preserve its identity at the
// caller's existing defect or domain-error boundary through issue metadata.
export function jsonCause(error: Schema.SchemaError): unknown {
  let issue = error.issue;
  while (issue._tag === "Encoding") issue = issue.issue;
  if (
    issue._tag === "InvalidValue" &&
    issue.annotations !== undefined &&
    Object.hasOwn(issue.annotations, "nativeJsonCause")
  ) {
    return issue.annotations["nativeJsonCause"];
  }
  return error;
}

const jsonProjection = <S extends Schema.Top>(output: S, read: (text: string) => S["Type"]) =>
  Schema.decodeEffect(
    Schema.String.pipe(
      Schema.decodeTo(output, {
        decode: SchemaGetter.onSome<S["Type"], string>((input, options) => {
          try {
            return Effect.succeed(Option.some(read(input)));
          } catch (cause) {
            return Effect.fail(
              new SchemaIssue.InvalidValue({ nativeJsonCause: cause }, input, options),
            );
          }
        }),
        encode: SchemaGetter.forbiddenEncoding,
      }),
    ),
  );

export const decodeOwnerBirth = jsonProjection(
  Schema.Unknown,
  (text): unknown => JSON.parse(text).birth,
);
export const decodeCorrelationEvidence = jsonProjection(
  Schema.Unknown,
  (text): unknown => JSON.parse(text).evidence,
);
export const decodeBindingSha256 = jsonProjection(
  Schema.Unknown,
  (text): unknown => JSON.parse(text).bindingSha256,
);
export const decodeTaskKind = jsonProjection(
  Schema.Unknown,
  (text): unknown => JSON.parse(text).kind,
);
export const decodeLeaseStatus = jsonProjection(
  Schema.Unknown,
  (text): unknown => JSON.parse(text).status,
);
export const decodeActiveProviderThreadId = jsonProjection(
  Schema.Unknown,
  (text): unknown => JSON.parse(text).activeProviderThreadId,
);
export const decodeProviderThreadId = jsonProjection(
  Schema.Unknown,
  (text): unknown => JSON.parse(text).providerThreadId,
);

export const decodeTaskWithKind = jsonProjection(
  Schema.Struct({ value: Schema.Unknown, kind: Schema.Unknown }),
  (text) => {
    const value = JSON.parse(text);
    return { value, kind: value.kind };
  },
);
export const decodeLeaseWithStatus = jsonProjection(
  Schema.Struct({ value: Schema.Unknown, status: Schema.Unknown }),
  (text) => {
    const value = JSON.parse(text);
    return { value, status: value.status };
  },
);
export const decodeCleanupCorrelation = jsonProjection(
  Schema.Struct({ value: Schema.Unknown, evidenceSchema: Schema.Unknown }),
  (text) => {
    const value = JSON.parse(text);
    return { value, evidenceSchema: value.evidence?.schema };
  },
);

export const decodeDeletionInventoryMismatch = (
  text: string,
  expected: {
    readonly threadId: string;
    readonly projectId: string;
    readonly branch: string | null;
    readonly projectRoot: string | null;
    readonly path: string | null;
  },
  resolvePath: (root: string, path: string) => string,
) =>
  jsonProjection(Schema.Boolean, (text) => {
    const deleted = JSON.parse(text);
    return (
      deleted.id !== expected.threadId ||
      deleted.projectId !== expected.projectId ||
      deleted.branch !== expected.branch ||
      (deleted.worktreePath === null || expected.projectRoot === null
        ? null
        : resolvePath(expected.projectRoot, deleted.worktreePath)) !== expected.path
    );
  })(text);

export const decodeCleanupDeletionMismatch = (
  text: string,
  expected: {
    readonly threadId: string;
    readonly projectId: string;
    readonly branch: string | null;
    readonly projectRoot: string | null;
    readonly path: string | null;
  },
  resolvePath: (root: string, path: string) => string,
) =>
  jsonProjection(Schema.Boolean, (text) => {
    const deleted = JSON.parse(text);
    const expectedPath =
      deleted.worktreePath === null || expected.projectRoot === null
        ? null
        : resolvePath(expected.projectRoot, deleted.worktreePath);
    return (
      deleted.id !== expected.threadId ||
      deleted.projectId !== expected.projectId ||
      expectedPath !== expected.path ||
      deleted.branch !== expected.branch
    );
  })(text);

export const decodeSharedWorktreeCandidate = jsonProjection(
  Schema.NullOr(Schema.Struct({ projectId: ProjectId, worktreePath: Schema.String })),
  (text) => {
    const candidate = JSON.parse(text);
    if (candidate.deletedAt !== null || typeof candidate.worktreePath !== "string") return null;
    return {
      projectId: ProjectId.make(candidate.projectId),
      worktreePath: candidate.worktreePath,
    };
  },
);

export const decodeThreadPathChanged = (
  text: string,
  created: boolean,
  projectId: string,
  worktreePath: string | null,
) =>
  jsonProjection(Schema.Boolean, (text) => {
    const previous = JSON.parse(text);
    return (
      created ||
      previous === null ||
      previous.projectId !== projectId ||
      previous.worktreePath !== worktreePath
    );
  })(text);
