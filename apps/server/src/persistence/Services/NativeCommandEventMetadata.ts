import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export interface NativeCommandEventMetadataRow {
  readonly eventId: string;
  readonly commandId: string | null;
  readonly aggregateKind: "thread" | "project";
  readonly aggregateId: string;
  readonly sequence: number;
  readonly type: string;
  readonly occurredAt: string;
  readonly applicationEventVersion: number;
}

export interface NativeCommandReceipt {
  readonly commandId: string;
  readonly aggregateKind: "thread" | "project";
  readonly aggregateId: string;
  readonly commandType: string;
  readonly acceptedAt: string;
  readonly resultSequence: number;
  readonly status: "accepted" | "rejected";
}

export interface NativeCommandSnapshot {
  readonly receipt: Option.Option<NativeCommandReceipt>;
  readonly events: ReadonlyArray<NativeCommandEventMetadataRow>;
}

export class NativeCommandEventMetadataError extends Schema.TaggedError<NativeCommandEventMetadataError>()(
  "NativeCommandEventMetadataError",
  { operation: Schema.Literals(["readMetadataByCommandId", "readSnapshotByCommandId"]) },
) {
  override get message(): string {
    return "Native command evidence could not be read.";
  }
}

export class NativeCommandEventMetadata extends Context.Service<
  NativeCommandEventMetadata,
  {
    readonly readMetadataByCommandId: (
      commandId: string,
    ) => Effect.Effect<
      ReadonlyArray<NativeCommandEventMetadataRow>,
      NativeCommandEventMetadataError
    >;
    // The consumer brackets this snapshot with its selected-store authority checks.
    readonly readSnapshotByCommandId: (
      commandId: string,
    ) => Effect.Effect<NativeCommandSnapshot, NativeCommandEventMetadataError>;
  }
>()("t3/persistence/Services/NativeCommandEventMetadata") {}
