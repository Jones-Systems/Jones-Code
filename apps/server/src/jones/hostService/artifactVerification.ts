import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  cliArchiveFileName,
  cliArchivePlatformKey,
  parseChecksums,
} from "@t3tools/shared/cliRelease";
import { JonesRuntimePolicyError } from "./releasePolicy.ts";

export const JONES_RUNTIME_PROVENANCE_FILE = ".jones-provenance.json";
const sha256 = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/i));
const sourceCommit = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));
const version = Schema.String.check(Schema.isPattern(/^\d+\.\d+\.\d+(?:-[a-zA-Z0-9.-]+)?$/));

export const JonesArtifactMetadata = Schema.Struct({
  schema: Schema.Literal(1),
  repository: Schema.String,
  source: sourceCommit,
  version,
  platform: Schema.String,
  architecture: Schema.String,
  artifact: Schema.String,
  sha256,
});
export type JonesArtifactMetadata = typeof JonesArtifactMetadata.Type;

export const JonesRuntimeProvenance = Schema.Struct({
  ...JonesArtifactMetadata.fields,
  repository: Schema.Literal("Jones-Systems/Jones-Code"),
  entrySha256: sha256,
});
export type JonesRuntimeProvenance = typeof JonesRuntimeProvenance.Type;

const decodeArtifactMetadataJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(JonesArtifactMetadata),
);
const decodeRuntimeProvenanceJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(JonesRuntimeProvenance),
);

export class JonesArtifactVerificationError extends Schema.TaggedError<JonesArtifactVerificationError>()(
  "JonesArtifactVerificationError",
  { reason: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {
  override get message(): string {
    return `Jones artifact verification failed: ${this.reason}`;
  }
}

export const decodeJonesArtifactMetadata = (metadataJson: string) =>
  decodeArtifactMetadataJson(metadataJson).pipe(
    Effect.mapError(
      (cause) => new JonesArtifactVerificationError({ reason: "invalid ARTIFACT.json", cause }),
    ),
  );

export interface VerifyJonesArtifactInput {
  readonly metadataJson: string;
  readonly sourceCommit: string;
  readonly expectSourceCommit: string;
  readonly checksums: string;
  readonly archiveSha256: string;
  readonly platform: NodeJS.Platform;
  readonly arch: string;
}

export const verifyJonesArtifact = Effect.fn("jones.host_service.verify_artifact")(function* (
  input: VerifyJonesArtifactInput,
) {
  const metadata = yield* decodeJonesArtifactMetadata(input.metadataJson);
  const fail = (reason: string) => new JonesArtifactVerificationError({ reason });
  if (metadata.repository !== "Jones-Systems/Jones-Code") {
    return yield* fail("ARTIFACT.json repository is not Jones-Systems/Jones-Code");
  }
  if (
    metadata.source !== input.sourceCommit.trim() ||
    metadata.source !== input.expectSourceCommit
  ) {
    return yield* fail("ARTIFACT.json source, SOURCE_COMMIT and expect-source-commit must agree");
  }
  if (metadata.platform !== input.platform || metadata.architecture !== input.arch) {
    return yield* fail("artifact platform and architecture do not match this host");
  }
  const platformKey = cliArchivePlatformKey(input.platform, input.arch);
  if (
    platformKey === undefined ||
    metadata.artifact !== cliArchiveFileName(metadata.version, platformKey)
  ) {
    return yield* fail("artifact filename does not match its version and host platform");
  }
  const expected = parseChecksums(input.checksums).get(metadata.artifact);
  if (
    expected === undefined ||
    !/^[a-f0-9]{64}$/i.test(input.archiveSha256) ||
    input.archiveSha256.toLowerCase() !== expected ||
    metadata.sha256.toLowerCase() !== expected
  ) {
    return yield* fail("archive SHA-256 must match SHA256SUMS and ARTIFACT.json");
  }
  return { ...metadata, repository: "Jones-Systems/Jones-Code" as const, sha256: expected };
});

export const verifyJonesRuntimeProvenance = Effect.fn("jones.host_service.verify_provenance")(
  function* (input: {
    readonly provenanceJson: string;
    readonly version: string;
    readonly platform: NodeJS.Platform;
    readonly arch: string;
    readonly entrySha256: string;
    readonly expectedProvenance?: JonesArtifactMetadata | undefined;
  }) {
    const fail = (reason: string, cause?: unknown) =>
      new JonesRuntimePolicyError({ reason, cause });
    const provenance = yield* decodeRuntimeProvenanceJson(input.provenanceJson).pipe(
      Effect.mapError((cause) => fail("The existing runtime has invalid Jones provenance.", cause)),
    );
    const platformKey = cliArchivePlatformKey(input.platform, input.arch);
    if (
      provenance.version !== input.version ||
      provenance.platform !== input.platform ||
      provenance.architecture !== input.arch ||
      platformKey === undefined ||
      provenance.artifact !== cliArchiveFileName(input.version, platformKey) ||
      provenance.entrySha256.toLowerCase() !== input.entrySha256.toLowerCase()
    ) {
      return yield* fail("The existing runtime does not match its Jones provenance.");
    }
    const expected = input.expectedProvenance;
    if (
      expected !== undefined &&
      (provenance.repository !== expected.repository ||
        provenance.source !== expected.source ||
        provenance.version !== expected.version ||
        provenance.platform !== expected.platform ||
        provenance.architecture !== expected.architecture ||
        provenance.artifact !== expected.artifact ||
        provenance.sha256.toLowerCase() !== expected.sha256.toLowerCase())
    ) {
      return yield* fail("The existing version directory belongs to a different Jones artifact.");
    }
    return provenance;
  },
);
