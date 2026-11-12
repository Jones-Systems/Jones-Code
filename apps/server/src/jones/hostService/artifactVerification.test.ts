import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  JonesArtifactVerificationError,
  verifyJonesArtifact,
  verifyJonesRuntimeProvenance,
  type VerifyJonesArtifactInput,
  type JonesArtifactMetadata,
} from "./artifactVerification.ts";
import { JonesRuntimePolicyError } from "./releasePolicy.ts";

const encode = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const source = "a".repeat(40);
const sha256 = "b".repeat(64);
const entrySha256 = "c".repeat(64);
const metadata: JonesArtifactMetadata = {
  schema: 1,
  repository: "Jones-Systems/Jones-Code",
  source,
  version: "1.2.3",
  platform: "linux",
  architecture: "x64",
  artifact: "t3-1.2.3-linux-x64.tar.gz",
  sha256,
};
const input: VerifyJonesArtifactInput = {
  metadataJson: encode(metadata),
  sourceCommit: `${source}\n`,
  expectSourceCommit: source,
  checksums: `${sha256}  ${metadata.artifact}\n`,
  archiveSha256: sha256,
  platform: "linux",
  arch: "x64",
};

it.effect("verifies a local artifact against both checksum sources and the required commit", () =>
  Effect.gen(function* () {
    assert.deepEqual(yield* verifyJonesArtifact(input), metadata);
  }),
);

const invalidArtifacts: ReadonlyArray<readonly [string, Partial<VerifyJonesArtifactInput>]> = [
  ["wrong repository", { metadataJson: encode({ ...metadata, repository: "pingdotgg/t3code" }) }],
  ["SOURCE_COMMIT mismatch", { sourceCommit: "d".repeat(40) }],
  ["expected commit mismatch", { expectSourceCommit: "d".repeat(40) }],
  ["missing expected commit", { expectSourceCommit: "" }],
  ["platform mismatch", { platform: "darwin" }],
  ["architecture mismatch", { arch: "arm64" }],
  ["tampered archive", { archiveSha256: "d".repeat(64) }],
  ["metadata checksum mismatch", { metadataJson: encode({ ...metadata, sha256: "d".repeat(64) }) }],
  ["missing checksum entry", { checksums: `${sha256}  some-other-file.tar.gz\n` }],
  ["archive filename traversal", { metadataJson: encode({ ...metadata, artifact: "../outside" }) }],
  ["invalid metadata", { metadataJson: "{}" }],
];
it.effect.each(invalidArtifacts)("refuses %s", ([, change]) =>
  Effect.gen(function* () {
    const error = yield* verifyJonesArtifact({ ...input, ...change }).pipe(Effect.flip);
    assert.instanceOf(error, JonesArtifactVerificationError);
  }),
);

it.effect("verifies cached provenance with the extracted executable hash", () =>
  Effect.gen(function* () {
    assert.deepEqual(
      yield* verifyJonesRuntimeProvenance({
        provenanceJson: encode({ ...metadata, entrySha256 }),
        version: metadata.version,
        platform: "linux",
        arch: "x64",
        entrySha256,
      }),
      { ...metadata, entrySha256 },
    );
  }),
);

const invalidProvenance: ReadonlyArray<
  Partial<Parameters<typeof verifyJonesRuntimeProvenance>[0]>
> = [
  { entrySha256: "d".repeat(64) },
  { version: "1.2.4" },
  { platform: "darwin" },
  { arch: "arm64" },
  { provenanceJson: "{}" },
  { expectedProvenance: { ...metadata, source: "d".repeat(40) } },
  { expectedProvenance: { ...metadata, sha256: "d".repeat(64) } },
];
it.effect.each(invalidProvenance)(
  "refuses swapped or mismatched cached provenance (%s)",
  (change) =>
    Effect.gen(function* () {
      const error = yield* verifyJonesRuntimeProvenance({
        provenanceJson: encode({ ...metadata, entrySha256 }),
        version: metadata.version,
        platform: "linux",
        arch: "x64",
        entrySha256,
        ...change,
      }).pipe(Effect.flip);
      assert.instanceOf(error, JonesRuntimePolicyError);
      assert.include(error.message, "t3 jones host stage-runtime");
    }),
);
