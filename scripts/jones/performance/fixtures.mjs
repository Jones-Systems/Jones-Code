import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import {
  createOwnedRoot,
  disposeOwnedRoot,
  syntheticFixtureReceiptSha256,
  validateSyntheticFixture,
} from "./guard.mjs";
import { runOwnedChild } from "./lifecycle.mjs";
import { captureFixture, produceFixture } from "./fixtures-historical-worker.mjs";
import { sourceParentEnvironment, syntheticSourceParent, assertCurrentDatabaseSource } from "./sources.mjs";

const requestLimit = 49 * 1024;
const envelopeLimit = 49 * 1024;
const receiptLimit = 24 * 1024;
const stderrReserve = 8 * 1024;
const profileNames = ["health-offline-delete", "benchmark-wal"];
const workerPath = NodeURL.fileURLToPath(new URL("./fixtures-historical-worker.mjs", import.meta.url));

function fail(code, message, evidence) {
  const error = new Error(message);
  error.code = code;
  error.evidence = evidence;
  throw error;
}

function retained(owner, childReceipts, reason) {
  return Object.freeze({
    schema: "jones-performance-cleanup/v1",
    creationReceipt: owner.creationReceipt,
    outcome: "retained",
    absent: false,
    childReceipts,
    reason,
  });
}

function raise(error, evidence) {
  const failure = error instanceof Error ? error : new Error(String(error));
  failure.evidence =
    failure.evidence === undefined ? evidence : { ...evidence, primaryEvidence: failure.evidence };
  throw failure;
}

function boundedPolicy(policy) {
  return {
    ...policy,
    maxFiles: Math.min(policy?.maxFiles ?? 32, 32),
    maxReceiptBytes: Math.min(policy?.maxReceiptBytes ?? receiptLimit, receiptLimit),
  };
}

function outputEnvelope(child) {
  if (
    child.truncated ||
    Buffer.byteLength(child.stdout) > envelopeLimit ||
    !child.stdout.endsWith("\n")
  )
    fail("invalid_transport", "worker output was truncated, oversized or incomplete");
  let envelope;
  try {
    envelope = JSON.parse(child.stdout);
  } catch {
    fail("invalid_transport", "worker output was not one JSON envelope");
  }
  if (`${JSON.stringify(envelope)}\n` !== child.stdout)
    fail("invalid_transport", "worker output contained noncanonical framing or extra output");
  return envelope;
}

export function captureSyntheticFixture(context) {
  return captureFixture(context);
}

export async function withOpenSyntheticFixture(options, use) {
  if (!options || typeof use !== "function") fail("invalid_options", "fixture options and callback required");
  const producer = options.producer ?? "current-v2";
  if (!["current-v2", "historical-v1"].includes(producer)) fail("invalid_producer", "explicit supported producer required");
  const produced = producer === "current-v2"
    ? await (await import("../../../apps/server/scripts/jones/currentFixtures.ts")).produceCurrentFixture(options, use)
    : await produceFixture(options, use);
  const cleanup =
    produced.closeKnown && !produced.retainReason
      ? disposeOwnedRoot(produced.owner)
      : retained(produced.owner, [], produced.retainReason ?? "unknown_resource_close");
  const evidence = {
    creationReceipt: produced.owner.creationReceipt,
    capture: produced.capture,
    receipt: produced.receipt,
    receiptSha256: produced.receiptSha256,
    ...(produced.profile ? { profile: produced.profile } : {}),
    cleanup,
  };
  if (produced.error) raise(produced.error, evidence);
  if (cleanup.outcome !== "complete")
    fail("cleanup_retained", "open fixture cleanup retained its root", evidence);
  return {
    value: produced.value,
    capture: produced.capture,
    receipt: produced.receipt,
    receiptSha256: produced.receiptSha256,
    captureSha256: produced.captureSha256,
    cleanup,
  };
}

export async function withClosedSyntheticFixture(options, use) {
  if (!options || typeof use !== "function")
    fail("invalid_options", "fixture options and callback required");
  if (options.profile !== undefined && !profileNames.includes(options.profile))
    fail("unsupported_profile", "profile must be health-offline-delete or benchmark-wal");
  const producer = options.producer ?? "current-v2";
  if (!["current-v2", "historical-v1"].includes(producer)) fail("invalid_producer", "explicit supported producer required");
  if (producer === "current-v2") {
    assertCurrentDatabaseSource(options.databaseSource);
    if (options.binding?.sourceRevision !== options.databaseSource.sourceRevision || options.binding?.repository !== options.databaseSource.repository)
      fail("invalid_source", "producer binding differs from current candidate");
  }
  const profile = producer === "current-v2" ? (options.profile ?? "health-offline-delete") : options.profile;
  const policy = boundedPolicy(options.policy);
  const owner = createOwnedRoot({ ...options, policy });
  let child;
  let envelope;
  let validation;
  let value;
  let consumerOutcomeAccepted = false;
  let error;
  let retainReason;
  try {
    const request = {
      schema: "jones-performance-fixture-request/v1",
      options: {
        parentPath: owner.creationReceipt.canonicalRootPath,
        childName: "fixture",
        binding: options.binding,
        policy,
        producer,
        databaseSource: options.databaseSource,
        recipe: options.recipe,
        ...(profile ? { profile } : {}),
      },
    };
    const encoded = JSON.stringify(request);
    if (Buffer.byteLength(encoded) > requestLimit)
      fail("request_limit", "fixture request exceeds 49 KiB");
    const args = [workerPath, encoded];
    if (args.reduce((bytes, arg) => bytes + Buffer.byteLength(arg) + 1, 0) >= 64 * 1024)
      fail("request_limit", "fixture argv reaches the aggregate 64 KiB limit");
    child = await runOwnedChild({
      owner,
      executable: process.execPath,
      args,
      env: {
        HOME: policy.homePath,
        LANG: "C.UTF-8",
        TZ: "UTC",
        NODE_NO_WARNINGS: "1",
        ...(producer === "historical-v1" ? { [sourceParentEnvironment]: syntheticSourceParent() } : {}),
        TMPDIR: owner.creationReceipt.canonicalRootPath,
      },
      timeoutMs: options.lifecycle?.timeoutMs ?? 30000,
      terminateGraceMs: options.lifecycle?.terminateGraceMs ?? 2000,
      reapTimeoutMs: options.lifecycle?.reapTimeoutMs ?? 5000,
      maxOutputBytes: envelopeLimit + stderrReserve,
      signal: options.signal,
    });
    if (!child.closed || !child.reaped || child.outcome === "unknown") {
      retainReason = "unknown_child_close";
      fail("unknown_child_close", "fixture child close or reap was not established");
    }
    if (
      child.truncated ||
      Buffer.byteLength(child.stdout) > envelopeLimit ||
      Buffer.byteLength(child.stderr) > stderrReserve
    ) {
      retainReason = "invalid_transport";
      fail("invalid_transport", "fixture child exceeded compact transport limits");
    }
    if (child.outcome !== "success") {
      if (child.stdout) {
        retainReason = "invalid_transport";
        envelope = outputEnvelope(child);
        if (
          envelope.schema !== "jones-performance-fixture-failure/v1" ||
          envelope.closeKnown !== true
        )
          retainReason = "unknown_resource_close";
        else retainReason = envelope.retainReason;
      }
      if (profile)
        retainReason ??= options.signal?.aborted ? "profile_cancelled" : "profile_producer_failed";
      fail(
        envelope?.code ?? child.stopReason ?? "producer_failed",
        "fixture leaf did not complete production",
      );
    }
    retainReason = "invalid_transport";
    envelope = outputEnvelope(child);
    if (
      envelope.schema !== "jones-performance-fixture-envelope/v1" ||
      JSON.stringify(envelope.databaseSource) !== JSON.stringify(options.databaseSource) ||
      JSON.stringify(envelope.capture?.databaseSource) !== JSON.stringify(options.databaseSource) ||
      envelope.capture?.runtime?.profile !== (profile ?? "observed-production-defaults") ||
      (profile &&
        (envelope.capture?.profile?.kind !== profile ||
          envelope.capture?.profile?.stage !== "sealed"))
    )
      fail("invalid_transport", "fixture output source or schema differs from the request");
    const receipt = envelope.receipt;
    if (producer === "current-v2" && (receipt?.schema !== "jones-performance-fixture/v2" || receipt.producer !== producer || JSON.stringify(receipt.databaseSource) !== JSON.stringify(options.databaseSource) || JSON.stringify(receipt.runtime) !== JSON.stringify(envelope.capture.runtime)))
      fail("invalid_transport", "current fixture receipt source, producer or runtime differs");
    if (producer === "historical-v1" && receipt?.schema !== "jones-performance-fixture/v1")
      fail("invalid_transport", "historical fixture receipt schema differs");
    const expectedRoot = NodePath.join(owner.creationReceipt.canonicalRootPath, "fixture");
    if (
      receipt?.creationReceipt?.canonicalRootPath !== expectedRoot ||
      receipt.creationReceipt.canonicalParentPath !== owner.creationReceipt.canonicalRootPath ||
      receipt.producerStep !== options.binding.taskRef
    )
      fail("invalid_location", "fixture receipt does not name the exact nested producer root");
    if (Buffer.byteLength(`${JSON.stringify(receipt)}\n`) > receiptLimit)
      fail("receipt_limit", "complete sealed receipt exceeds 24 KiB");
    // The pin comes from this audited child's captured output after close, never a companion file.
    const receiptSha256 = syntheticFixtureReceiptSha256(receipt);
    const captureSha256 = NodeCrypto.createHash("sha256")
      .update(`${JSON.stringify(envelope.capture)}\n`)
      .digest("hex");
    if (receiptSha256 !== envelope.receiptSha256 || captureSha256 !== envelope.captureSha256)
      fail("invalid_transport", "fixture envelope hashes differ from its captured bytes");
    retainReason = "fixture_validation_failed";
    const custodyReceipt = fixtureCustodyReceipt(receipt);
    validation = await validateSyntheticFixture({
      receipt: custodyReceipt,
      expectedReceiptSha256: syntheticFixtureReceiptSha256(custodyReceipt),
      expectedBinding: options.binding,
      policy,
      signal: options.signal,
    });
    if (producer === "current-v2") assertCurrentDatabaseSource(options.databaseSource);
    retainReason = "consumer_outcome_unproved";
    const outcome = await use(
      Object.freeze({
        fixture: validation,
        receipt,
        receiptSha256,
        capture: envelope.capture,
        captureSha256,
        databaseSource: envelope.databaseSource,
        childReceipt: child,
      }),
    );
    if (
      !outcome ||
      typeof outcome !== "object" ||
      Array.isArray(outcome) ||
      outcome.schema !== "jones-performance-fixture-consumer/v1" ||
      outcome.fixtureReceiptSha256 !== receiptSha256 ||
      (outcome.disposition !== "release" && outcome.disposition !== "retain") ||
      !Object.hasOwn(outcome, "value")
    ) {
      retainReason = "invalid_consumer_outcome";
      fail(
        "invalid_consumer_outcome",
        "closed fixture consumer must return its matching release or retain outcome",
      );
    }
    value = outcome.value;
    consumerOutcomeAccepted = true;
    retainReason = outcome.disposition === "release" ? undefined : "consumer_retained";
  } catch (failure) {
    error = failure;
    if (profile)
      retainReason ??= options.signal?.aborted ? "profile_cancelled" : "profile_failed";
  }
  const childReceipts = child ? [child] : [];
  const cleanup = retainReason
    ? retained(owner, childReceipts, retainReason)
    : disposeOwnedRoot(owner, { childReceipts });
  const evidence = {
    creationReceipt: owner.creationReceipt,
    childReceipt: child,
    receipt: envelope?.receipt,
    receiptSha256: envelope?.receiptSha256,
    capture: envelope?.capture,
    ...(profile
      ? {
          profile:
            envelope?.schema === "jones-performance-fixture-failure/v1" &&
            envelope.profile?.kind === profile
              ? envelope.profile
              : (envelope?.capture?.profile ?? {
                  kind: profile,
                  stage: "producer-transport",
                  productionObservations: [],
                  failure: { code: retainReason ?? "profile_failed" },
                }),
        }
      : {}),
    ...(consumerOutcomeAccepted ? { value } : {}),
    cleanup,
  };
  if (error) raise(error, evidence);
  if (cleanup.outcome !== "complete")
    fail("cleanup_retained", "closed fixture cleanup retained its root", evidence);
  return {
    value,
    receipt: envelope.receipt,
    receiptSha256: envelope.receiptSha256,
    capture: envelope.capture,
    captureSha256: envelope.captureSha256,
    childReceipt: child,
    cleanup,
  };
}

export function fixtureCustodyReceipt(receipt) {
  if (receipt?.schema === "jones-performance-fixture/v1") return receipt;
  if (receipt?.schema !== "jones-performance-fixture/v2" || receipt.producer !== "current-v2")
    fail("invalid_receipt", "unsupported fixture producer receipt");
  const { producer, databaseSource, runtime, ...custody } = receipt;
  return { ...custody, schema: "jones-performance-fixture/v1" };
}
