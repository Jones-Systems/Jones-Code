import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import {
  decodeNativeBootstrapSubmission,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";

// Actual public synthetic producer vectors: Voice e35a1974, t3_bootstrap.py prepare_bootstrap.
const pythonVectors = [
  {
    preparation:
      '{"binding":{"account_ref":"fixture-account","backend_instance":"fixture-backend","base_branch":"main","environment_id":"fixture-environment","interaction_mode":"default","project_cwd":"/fixture/project","project_id":"fixture-project","provider_model_selection":{"instanceId":"codex","model":"fixture-model"},"run_setup_script":false,"runtime_mode":"full-access","start_from_origin":false},"binding_digest":"7d9e191731bb4bd988355a45cb3baaf494678f58cc5ef3bd831ebac3340285f9","command":{"bootstrap":{"createThread":{"branch":null,"createdAt":"2026-10-02T12:34:56Z","interactionMode":"default","modelSelection":{"instanceId":"codex","model":"fixture-model"},"projectId":"fixture-project","runtimeMode":"full-access","title":"Synthetic thread","worktreePath":null},"prepareWorktree":{"baseBranch":"main","branch":"t3code/voice-aa4c25ff545a65b500bd7830","projectCwd":"/fixture/project","requireWorktree":true,"startFromOrigin":false},"runSetupScript":false},"commandId":"voice-command-aa4c25ff545a65b500bd783049ea3587e2e7e1e9f2df8b104493d0d95eb20af0","createdAt":"2026-10-02T12:34:56Z","interactionMode":"default","message":{"attachments":[],"messageId":"voice-message-aa4c25ff545a65b500bd783049ea3587e2e7e1e9f2df8b104493d0d95eb20af0","role":"user","text":"Create a test thread"},"runtimeMode":"full-access","threadId":"voice-thread-aa4c25ff545a65b500bd783049ea3587e2e7e1e9f2df8b104493d0d95eb20af0","type":"thread.turn.start"},"command_digest":"1b0f82e7cfe4a3ef0e23c846464b329039a8b85229b4a712b80d63ce85136998","operation_id":"fixture-basic","preparation_id":"voice-bootstrap-aa4c25ff545a65b500bd783049ea3587e2e7e1e9f2df8b104493d0d95eb20af0","prompt_digest":"0cec0521300157dba847e178f58c8e7a1167e09fac7228c2d75888f5879f0569","schema":"voice.t3-bootstrap-preparation/v1"}',
    sha256: "e9c979706cee3d947b5373bb4ea16d1ea61bb49f83f1f12b6453e4fb74b61622",
  },
  {
    preparation:
      '{"binding":{"account_ref":"fixture-account","backend_instance":"fixture-backend","base_branch":"main","environment_id":"fixture-environment","interaction_mode":"default","project_cwd":"/fixture/project","project_id":"fixture-project","provider_model_selection":{"instanceId":"codex","model":"fixture-model","options":[{"id":"z","value":true},{"id":"a","value":"high"}]},"run_setup_script":false,"runtime_mode":"full-access","start_from_origin":false},"binding_digest":"818e2389c69d6cb541faee236eda09393a669741fc590e18de03c89f2e3fb885","command":{"bootstrap":{"createThread":{"branch":null,"createdAt":"2026-10-02T08:34:56.123456-04:00","interactionMode":"default","modelSelection":{"instanceId":"codex","model":"fixture-model","options":[{"id":"z","value":true},{"id":"a","value":"high"}]},"projectId":"fixture-project","runtimeMode":"full-access","title":"Unicode 雪","worktreePath":null},"prepareWorktree":{"baseBranch":"main","branch":"t3code/voice-ed9ca90d0042c1268996a855","projectCwd":"/fixture/project","requireWorktree":true,"startFromOrigin":false},"runSetupScript":false},"commandId":"voice-command-ed9ca90d0042c1268996a8551a5b5029b1a6c27e60c1def1bce176e849c5d11b","createdAt":"2026-10-02T08:34:56.123456-04:00","interactionMode":"default","message":{"attachments":[],"messageId":"voice-message-ed9ca90d0042c1268996a8551a5b5029b1a6c27e60c1def1bce176e849c5d11b","role":"user","text":"雪 🧪 é\\n\\t\\b\\f\\r\\u0000\\u001f "},"runtimeMode":"full-access","threadId":"voice-thread-ed9ca90d0042c1268996a8551a5b5029b1a6c27e60c1def1bce176e849c5d11b","type":"thread.turn.start"},"command_digest":"f1204c723d5f98d3fd47e10a339b0a5fa845cd6e95036fc6332793ae9b955ed2","operation_id":"fixture-unicode","preparation_id":"voice-bootstrap-ed9ca90d0042c1268996a8551a5b5029b1a6c27e60c1def1bce176e849c5d11b","prompt_digest":"aa122aed1fb570b8eb22b9f6215fb3eb776050f196fc70086df9908f7b7e183c","schema":"voice.t3-bootstrap-preparation/v1"}',
    sha256: "fd1334f7c9bb430072d78700d67e63c2f4e15dfed0e55052f6282d0b772ea8a0",
  },
  {
    preparation:
      '{"binding":{"account_ref":"fixture-account","backend_instance":"fixture-backend","base_branch":"main","environment_id":"fixture-environment","interaction_mode":"default","project_cwd":"/fixture/project","project_id":"fixture-project","provider_model_selection":{"instanceId":"codex","model":"fixture-model","options":[{"id":"a","value":"high"},{"id":"z","value":false}]},"run_setup_script":false,"runtime_mode":"full-access","start_from_origin":false},"binding_digest":"7e90236e66b1dbe0f1a1cd47407603105e40c960cf03b7c1258e9a6074fb999a","command":{"bootstrap":{"createThread":{"branch":null,"createdAt":"2026-10-02T12:34:56+00:00","interactionMode":"default","modelSelection":{"instanceId":"codex","model":"fixture-model","options":[{"id":"a","value":"high"},{"id":"z","value":false}]},"projectId":"fixture-project","runtimeMode":"full-access","title":"Sorted object keys","worktreePath":null},"prepareWorktree":{"baseBranch":"main","branch":"t3code/voice-449c049ec4eec031daba9c09","projectCwd":"/fixture/project","requireWorktree":true,"startFromOrigin":false},"runSetupScript":false},"commandId":"voice-command-449c049ec4eec031daba9c0948a3ef09d734f03519e7e88c8d3c788ab39d9089","createdAt":"2026-10-02T12:34:56+00:00","interactionMode":"default","message":{"attachments":[],"messageId":"voice-message-449c049ec4eec031daba9c0948a3ef09d734f03519e7e88c8d3c788ab39d9089","role":"user","text":"Options with reverse IDs"},"runtimeMode":"full-access","threadId":"voice-thread-449c049ec4eec031daba9c0948a3ef09d734f03519e7e88c8d3c788ab39d9089","type":"thread.turn.start"},"command_digest":"de42b4bf7f8a1ccda235f68d410285210d65809721aaa383896fc8c93fcc38fa","operation_id":"fixture-options","preparation_id":"voice-bootstrap-449c049ec4eec031daba9c0948a3ef09d734f03519e7e88c8d3c788ab39d9089","prompt_digest":"09c65720131afabb879f5e190b4e5546012c4ffe5d549a552f52376c04683d0d","schema":"voice.t3-bootstrap-preparation/v1"}',
    sha256: "1c715e5c0f11e4390d2200e04a2f02598e3cde787396e90b79a0b9f0e30f838c",
  },
  {
    preparation:
      '{"binding":{"account_ref":"fixture-account","backend_instance":"fixture-backend","base_branch":"main","environment_id":"fixture-environment","interaction_mode":"default","project_cwd":"/fixture/project","project_id":"fixture-project","provider_model_selection":{"instanceId":"codex","model":"fixture-model"},"run_setup_script":false,"runtime_mode":"full-access","start_from_origin":false},"binding_digest":"7d9e191731bb4bd988355a45cb3baaf494678f58cc5ef3bd831ebac3340285f9","command":{"bootstrap":{"createThread":{"branch":null,"createdAt":"20261002T123456Z","interactionMode":"default","modelSelection":{"instanceId":"codex","model":"fixture-model"},"projectId":"fixture-project","runtimeMode":"full-access","title":"Basic timestamp","worktreePath":null},"prepareWorktree":{"baseBranch":"main","branch":"t3code/voice-f4993fe3682795c161932b71","projectCwd":"/fixture/project","requireWorktree":true,"startFromOrigin":false},"runSetupScript":false},"commandId":"voice-command-f4993fe3682795c161932b71d42efd03b223fbbf21eb4ed3fa06253667245cb2","createdAt":"20261002T123456Z","interactionMode":"default","message":{"attachments":[],"messageId":"voice-message-f4993fe3682795c161932b71d42efd03b223fbbf21eb4ed3fa06253667245cb2","role":"user","text":"Basic time"},"runtimeMode":"full-access","threadId":"voice-thread-f4993fe3682795c161932b71d42efd03b223fbbf21eb4ed3fa06253667245cb2","type":"thread.turn.start"},"command_digest":"c12bd874f9d5d3c5b96eb80b3a4991470969e62e48b8adaae5ca4ec5a0db94cf","operation_id":"fixture-basic-time","preparation_id":"voice-bootstrap-f4993fe3682795c161932b71d42efd03b223fbbf21eb4ed3fa06253667245cb2","prompt_digest":"fb67be97ef4e7258ddf2c39267a42174611b59475efaa647d880d20333e07543","schema":"voice.t3-bootstrap-preparation/v1"}',
    sha256: "0200605eb05346b0dbc24ffc416c2d06ba5386d29dadf6daa8b94395782006ac",
  },
  {
    preparation:
      '{"binding":{"account_ref":"fixture-account","backend_instance":"fixture-backend","base_branch":"main","environment_id":"fixture-environment","interaction_mode":"default","project_cwd":"/fixture/project","project_id":"fixture-project","provider_model_selection":{"instanceId":"codex","model":"fixture-model"},"run_setup_script":false,"runtime_mode":"full-access","start_from_origin":false},"binding_digest":"7d9e191731bb4bd988355a45cb3baaf494678f58cc5ef3bd831ebac3340285f9","command":{"bootstrap":{"createThread":{"branch":null,"createdAt":"2026-W40-5T12:34:56+00:00","interactionMode":"default","modelSelection":{"instanceId":"codex","model":"fixture-model"},"projectId":"fixture-project","runtimeMode":"full-access","title":"Week timestamp","worktreePath":null},"prepareWorktree":{"baseBranch":"main","branch":"t3code/voice-71cf8b0fc216d903bf9cacfc","projectCwd":"/fixture/project","requireWorktree":true,"startFromOrigin":false},"runSetupScript":false},"commandId":"voice-command-71cf8b0fc216d903bf9cacfc071918afddddfbcbf130b735096558b3d7cfaaf8","createdAt":"2026-W40-5T12:34:56+00:00","interactionMode":"default","message":{"attachments":[],"messageId":"voice-message-71cf8b0fc216d903bf9cacfc071918afddddfbcbf130b735096558b3d7cfaaf8","role":"user","text":"Week time"},"runtimeMode":"full-access","threadId":"voice-thread-71cf8b0fc216d903bf9cacfc071918afddddfbcbf130b735096558b3d7cfaaf8","type":"thread.turn.start"},"command_digest":"0c3e84d9661217b0d203dbd4d92271260439b93615e50c08812d7c7e5f01ab19","operation_id":"fixture-week-time","preparation_id":"voice-bootstrap-71cf8b0fc216d903bf9cacfc071918afddddfbcbf130b735096558b3d7cfaaf8","prompt_digest":"50bea8a6176a5b4b97719e49e8af230e27d2f99bebfffbda19cb5cd5d450c841","schema":"voice.t3-bootstrap-preparation/v1"}',
    sha256: "c6f5e6a7bafa85b373449cf0b6f6ad9a1bcac4a7f877bc4bee60eaa3a4d61a3c",
  },
  {
    preparation:
      '{"binding":{"account_ref":"fixture-account","backend_instance":"fixture-backend","base_branch":"main","environment_id":"fixture-environment","interaction_mode":"default","project_cwd":"/fixture/project","project_id":"fixture-project","provider_model_selection":{"instanceId":"codex","model":"fixture-model"},"run_setup_script":false,"runtime_mode":"full-access","start_from_origin":false},"binding_digest":"7d9e191731bb4bd988355a45cb3baaf494678f58cc5ef3bd831ebac3340285f9","command":{"bootstrap":{"createThread":{"branch":null,"createdAt":"2026-10-02T12:34:56+00:00:30.123456","interactionMode":"default","modelSelection":{"instanceId":"codex","model":"fixture-model"},"projectId":"fixture-project","runtimeMode":"full-access","title":"Offset timestamp","worktreePath":null},"prepareWorktree":{"baseBranch":"main","branch":"t3code/voice-0bc8df814f12afbc689cea1e","projectCwd":"/fixture/project","requireWorktree":true,"startFromOrigin":false},"runSetupScript":false},"commandId":"voice-command-0bc8df814f12afbc689cea1ee5069d237500e8e1fb49c3377a83f3a2e5da8e55","createdAt":"2026-10-02T12:34:56+00:00:30.123456","interactionMode":"default","message":{"attachments":[],"messageId":"voice-message-0bc8df814f12afbc689cea1ee5069d237500e8e1fb49c3377a83f3a2e5da8e55","role":"user","text":"Offset seconds"},"runtimeMode":"full-access","threadId":"voice-thread-0bc8df814f12afbc689cea1ee5069d237500e8e1fb49c3377a83f3a2e5da8e55","type":"thread.turn.start"},"command_digest":"569f7a5853737ca0d57e03100278f0f5e28c7d3762e973e4f2d3485c42158ceb","operation_id":"fixture-offset-seconds","preparation_id":"voice-bootstrap-0bc8df814f12afbc689cea1ee5069d237500e8e1fb49c3377a83f3a2e5da8e55","prompt_digest":"b9878d65eae625d2d8b9eeebe78989d409f902267b3fdc775b50ef54e36a11de","schema":"voice.t3-bootstrap-preparation/v1"}',
    sha256: "7e10733900dac8ff24a25107abd89388f967b0f9464d26dd80688f018ebbc69c",
  },
  {
    preparation:
      '{"binding":{"account_ref":"fixture-account","backend_instance":"fixture-backend","base_branch":"main","environment_id":"fixture-environment","interaction_mode":"default","project_cwd":"/fixture/project","project_id":"fixture-project","provider_model_selection":{"instanceId":"codex","model":"fixture-model"},"run_setup_script":false,"runtime_mode":"full-access","start_from_origin":false},"binding_digest":"7d9e191731bb4bd988355a45cb3baaf494678f58cc5ef3bd831ebac3340285f9","command":{"bootstrap":{"createThread":{"branch":null,"createdAt":"2026-10-02T12:34:56,123456+02","interactionMode":"default","modelSelection":{"instanceId":"codex","model":"fixture-model"},"projectId":"fixture-project","runtimeMode":"full-access","title":"Comma timestamp","worktreePath":null},"prepareWorktree":{"baseBranch":"main","branch":"t3code/voice-e17fa5c9bb7169b6b33e59b0","projectCwd":"/fixture/project","requireWorktree":true,"startFromOrigin":false},"runSetupScript":false},"commandId":"voice-command-e17fa5c9bb7169b6b33e59b0b86491e81bf998b82cdca851eb62205f0a7a17ae","createdAt":"2026-10-02T12:34:56,123456+02","interactionMode":"default","message":{"attachments":[],"messageId":"voice-message-e17fa5c9bb7169b6b33e59b0b86491e81bf998b82cdca851eb62205f0a7a17ae","role":"user","text":"Comma fraction"},"runtimeMode":"full-access","threadId":"voice-thread-e17fa5c9bb7169b6b33e59b0b86491e81bf998b82cdca851eb62205f0a7a17ae","type":"thread.turn.start"},"command_digest":"bf84eebafafcf50b8ce70609fa5ea6b129a195441c3e47d106d0560b1759e442","operation_id":"fixture-comma-fraction","preparation_id":"voice-bootstrap-e17fa5c9bb7169b6b33e59b0b86491e81bf998b82cdca851eb62205f0a7a17ae","prompt_digest":"1202a37974b275ef1c79871f1e06447a1f39e59cabb30acbc1aa2eae5ecfdb01","schema":"voice.t3-bootstrap-preparation/v1"}',
    sha256: "a1b7bed97671790457e4d89fe9a5dc7879a223f33c70e1618f62aadb3b4a1a89",
  },
  {
    preparation:
      '{"binding":{"account_ref":"fixture-account","backend_instance":"fixture-backend","base_branch":"main","environment_id":"fixture-environment","interaction_mode":"default","project_cwd":"/fixture/project","project_id":"fixture-project","provider_model_selection":{"instanceId":"codex","model":"fixture-model"},"run_setup_script":false,"runtime_mode":"full-access","start_from_origin":false},"binding_digest":"7d9e191731bb4bd988355a45cb3baaf494678f58cc5ef3bd831ebac3340285f9","command":{"bootstrap":{"createThread":{"branch":null,"createdAt":"2026-10-02T12:34:56Z","interactionMode":"default","modelSelection":{"instanceId":"codex","model":"fixture-model"},"projectId":"fixture-project","runtimeMode":"full-access","title":"﻿Synthetic title﻿","worktreePath":null},"prepareWorktree":{"baseBranch":"main","branch":"t3code/voice-053a14ad96571aa95e4bb95f","projectCwd":"/fixture/project","requireWorktree":true,"startFromOrigin":false},"runSetupScript":false},"commandId":"voice-command-053a14ad96571aa95e4bb95fd0dc37066e1b533c5ae928c4d5e9716a3f7129ac","createdAt":"2026-10-02T12:34:56Z","interactionMode":"default","message":{"attachments":[],"messageId":"voice-message-053a14ad96571aa95e4bb95fd0dc37066e1b533c5ae928c4d5e9716a3f7129ac","role":"user","text":" ﻿ "},"runtimeMode":"full-access","threadId":"voice-thread-053a14ad96571aa95e4bb95fd0dc37066e1b533c5ae928c4d5e9716a3f7129ac","type":"thread.turn.start"},"command_digest":"f1b5260f71c1ced0eefed180ac62d7c91f4d523cf95e697a118ca3fd98d6b087","operation_id":"fixture-python-strip","preparation_id":"voice-bootstrap-053a14ad96571aa95e4bb95fd0dc37066e1b533c5ae928c4d5e9716a3f7129ac","prompt_digest":"6209822005ccdf32080612cf1e33d3727eee0d512c54d59b397027b6c0acf87c","schema":"voice.t3-bootstrap-preparation/v1"}',
    sha256: "6167660aa8190550de51cc8f7292c6599558ecc1db297ad7bab114f6573c37d0",
  },
] as const;

for (const [index, vector] of pythonVectors.entries()) {
  it.effect(`matches actual Python canonical vector ${index}`, () =>
    Effect.gen(function* () {
      const preparation = yield* validateNativeCreationPreparation(
        new TextEncoder().encode(vector.preparation),
      );
      assert.strictEqual(preparation.canonicalText, vector.preparation);
      assert.strictEqual(preparation.preparationSha256, vector.sha256);
      assert.strictEqual(nativeCreationSha256(preparation.canonicalText), vector.sha256);
    }),
  );
}

it.effect("rejects original-byte, shape, digest, identity and binding disagreement", () =>
  Effect.gen(function* () {
    const source = pythonVectors[0].preparation;
    const invalid = [
      ` ${source}`,
      source.replace('"binding":{', '"binding":{"extra":false,'),
      source.replace(
        '"schema":"voice.t3-bootstrap-preparation/v1"',
        '"schema":"voice.t3-bootstrap-preparation/v1","schema":"voice.t3-bootstrap-preparation/v1"',
      ),
      source.replace('"attachments":[]', '"attachments":[{}]'),
      source.replace('"requireWorktree":true', '"requireWorktree":false'),
      source.replace('"role":"user"', '"role":"assistant"'),
      source.replace('"title":"Synthetic thread"', '"title":"Synthetic thread","unknown":true'),
      source.replace('"account_ref":"fixture-account"', '"account_ref":"other-account"'),
      source.replace('"command_digest":"1', '"command_digest":"2'),
      source.replace("voice-command-aa4c", "voice-command-bb4c"),
      source.replace("t3code/voice-aa4c", "t3code/voice-bb4c"),
      source.replace('"branch":null', '"branch":"main"'),
      source.replace(/2026-10-02T12:34:56Z/g, "2026-10-02T12:34:56"),
      source.replace('"text":"Create a test thread"', '"text":"\\ud800"'),
      source.replace('"instanceId":"codex"', '"instanceId":"invalid.provider"'),
    ];
    for (const text of invalid) {
      const result = yield* validateNativeCreationPreparation(new TextEncoder().encode(text)).pipe(
        Effect.flip,
      );
      assert.strictEqual(result.code, "invalid_preparation");
    }
    for (const bytes of [
      new Uint8Array([0xc3, 0x28]),
      new Uint8Array(1_048_577),
      new Uint8Array(),
    ]) {
      const error = yield* validateNativeCreationPreparation(bytes).pipe(Effect.flip);
      assert.strictEqual(error.code, "invalid_preparation");
    }
  }),
);

it.effect("counts Unicode code points and rejects lone surrogates before hashing", () =>
  Effect.gen(function* () {
    const parsed = yield* validateNativeCreationPreparation(
      new TextEncoder().encode(pythonVectors[0].preparation),
    );
    const binding = parsed.binding;
    const prepare = (text: string, timestamp = "2026-10-02T12:34:56Z") => {
      const command = nativePreparationCommand(
        parsed.operationId,
        binding,
        text,
        "Synthetic thread",
        timestamp,
      );
      return nativeCreationCanonicalJson({
        schema: "voice.t3-bootstrap-preparation/v1",
        preparation_id: parsed.preparationId,
        operation_id: parsed.operationId,
        binding,
        binding_digest: parsed.bindingDigest,
        prompt_digest: nativeCreationSha256(text),
        command_digest: nativeCreationSha256(nativeCreationCanonicalJson(command)),
        command,
      });
    };
    const accepted = yield* validateNativeCreationPreparation(
      new TextEncoder().encode(prepare("🧪".repeat(100_000))),
    );
    assert.strictEqual([...accepted.command.message.text].length, 100_000);
    for (const text of ["🧪".repeat(100_001), "\ud800", "   ", "\u0085", "\u001c\u001f"]) {
      assert.strictEqual(
        (yield* validateNativeCreationPreparation(new TextEncoder().encode(prepare(text))).pipe(
          Effect.flip,
        )).code,
        "invalid_preparation",
      );
    }
    for (const timestamp of [
      "2026-02-30T12:00:00Z",
      "2026-13-01T12:00:00Z",
      "0000-01-01T12:00:00Z",
      "2025-W53-1T12:00:00Z",
      "2026-10-02T24:00:00Z",
      "2026-10-02T12:00:60Z",
      "2026-10-02T12:00:00+24:00",
      "2026-10-02T12:00:00",
      "2026-10-02Z12:00:00Z",
    ]) {
      assert.strictEqual(
        (yield* validateNativeCreationPreparation(
          new TextEncoder().encode(prepare("Synthetic prompt", timestamp)),
        ).pipe(Effect.flip)).code,
        "invalid_preparation",
      );
    }
  }),
);

it.effect("decodes only a bounded guarded canonical base64 submission", () =>
  Effect.gen(function* () {
    const input = {
      schema: "t3.native-bootstrap-submission/v1",
      preparationBase64: Buffer.from(pythonVectors[0].preparation).toString("base64"),
      creationGuard: {
        schema: "t3.native-creation-guard/v1",
        grantId: "fixture-grant",
        grantRevision: 1,
      },
    };
    const result = yield* decodeNativeBootstrapSubmission(input);
    assert.strictEqual(result.preparation.preparationSha256, pythonVectors[0].sha256);
    for (const invalid of [
      { ...input, extra: true },
      { ...input, creationGuard: { ...input.creationGuard, grantRevision: 0 } },
      { ...input, preparationBase64: "YQ==" },
      { ...input, preparationBase64: "Yh==" },
      { ...input, preparationBase64: "YQ" },
    ]) {
      assert.strictEqual(
        (yield* decodeNativeBootstrapSubmission(invalid).pipe(Effect.flip)).code,
        "invalid_preparation",
      );
    }
  }),
);
