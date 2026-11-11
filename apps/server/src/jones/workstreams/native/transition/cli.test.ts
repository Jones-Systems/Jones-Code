// @effect-diagnostics nodeBuiltinImport:off -- Synthetic public keys and per-test input files only; each test removes its own directory.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { Command } from "effect/cli";
import * as TestConsole from "effect/testing/TestConsole";
import { command, WorkstreamsTransition, type TransitionSelection } from "./cli.ts";

type Service = WorkstreamsTransition["Service"];
const secretRow = "synthetic-private-row-payload";
const { publicKey } = NodeCrypto.generateKeyPairSync("ed25519");
const pem = publicKey.export({ type: "spki", format: "pem" }).toString();
const header = {
  registry: { registry_instance_id: "10000000-0000-4000-8000-000000000001" },
  owners: [{ owner_id: "synthetic.owner", server_generation: 7, registry_version: 7 }],
};
const fakePlan = {
  import_run_id: "70000000-0000-4000-8000-000000000001",
  snapshot: {
    header,
    rows: [{ class: "workstreams", key: ["w"], row: { payload: secretRow } }],
    classes: [{ class: "workstreams", count: 1, sha256: "c".repeat(64) }],
    snapshot_sha256: "s".repeat(64),
  },
  home_environment_id: "synthetic.home",
  active_key_id: "synthetic-key",
  mappings: [],
  unresolved_sources: ["synthetic.source"],
  plan_sha256: "p".repeat(64),
};
const request = {
  import_run_id: fakePlan.import_run_id,
  stream_key_id: "synthetic-stream-key",
  permit: { key_id: "operator", header_sha256: "h".repeat(64), signature: "AAAA" },
  permit_key_id: "operator",
  home_environment_id: "synthetic.home",
  active_key_id: "synthetic-key",
  mappings: [],
};
const bundle = {
  transfer_id: "transfer:fixed",
  destination_home: "synthetic.home",
  next_generations: [{ owner_id: "synthetic.owner", server_generation: 8 }],
  lineage: { original_snapshot_id: "x", original_snapshot_sha256: "y", import_run_id: "z" },
  snapshot: secretRow,
  extensions: [],
};

/** Records every service call; `bootstrap` counts layer construction so refusals prove no home opened. */
const harness = () => {
  const calls: { readonly name: string; readonly args: readonly unknown[] }[] = [];
  const selections: TransitionSelection[] = [];
  const record =
    (name: string, result: unknown) =>
    (...args: readonly unknown[]) => {
      calls.push({ name, args });
      return Effect.succeed(result);
    };
  const service = {
    plan: record("plan", fakePlan),
    apply: record("apply", { state: "applied", applied: 1 }),
    readback: record("readback", {
      snapshot_sha256: fakePlan.snapshot.snapshot_sha256,
      classes: fakePlan.snapshot.classes,
      rows: fakePlan.snapshot.rows,
      state: "verified",
    }),
    reconcileLegacyOpen: record("reconcileLegacyOpen", {}),
    activate: record("activate", { state: "activated", activation_id: "a", replayed: false }),
    prepareReverse: record("prepareReverse", {
      snapshot: { ...fakePlan.snapshot, header },
      canonical_jsonl: secretRow,
      next_generations: bundle.next_generations,
      lineage: bundle.lineage,
    }),
    reverseExport: record("reverseExport", { encrypted: "ciphertext", snapshot_sha256: "r" }),
    prepareTransfer: record("prepareTransfer", bundle),
    transfer: record("transfer", { encrypted: "ciphertext", bundle_sha256: "b" }),
    readTransfer: record("readTransfer", bundle),
    applyTransferExtensions: record("applyTransferExtensions", { extension_count: 0 }),
    readbackTransfer: record("readbackTransfer", { state: "verified", extension_count: 0 }),
    completeTransfer: record("completeTransfer", { state: "transferred_out" }),
  } as unknown as Service;
  const cli = command((selection) => {
    selections.push(selection);
    return Layer.succeed(WorkstreamsTransition, service);
  });
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "nw-transition-cli-"));
  let files = 0;
  // Each write gets a fresh name so argument lists built together keep their own contents.
  const file = (name: string, text: string) => {
    const target = NodePath.join(dir, String(files++) + "-" + name);
    NodeFS.writeFileSync(target, text, { mode: 0o600 });
    return target;
  };
  const base = (req: unknown = request, key = pem) => [
    "--home",
    "/synthetic/home",
    "--key-provider",
    "server-secret-store",
    "--request",
    JSON.stringify(req),
    "--snapshot",
    file("snapshot.enc", "encrypted-snapshot"),
    "--permit-public-key",
    file("permit.pem", key),
  ];
  const run = (args: readonly string[]) => Command.runWith(cli, { version: "test" })(args);
  return {
    calls,
    selections,
    base,
    file,
    run,
    cleanup: () => NodeFS.rmSync(dir, { recursive: true }),
  };
};
const output = Effect.map(TestConsole.logLines, (lines) => lines.join("\n"));
const provided = <A, E>(effect: Effect.Effect<A, E, NodeServices.NodeServices>) =>
  effect.pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, TestConsole.layer)));

it.effect("plan rebuilds from the encrypted file and public key and prints digests only", () => {
  const h = harness();
  return Effect.gen(function* () {
    yield* h.run(["plan", ...h.base()]);
    expect(h.selections).toEqual([{ home: "/synthetic/home", keyProvider: "server-secret-store" }]);
    const [call] = h.calls;
    expect(call?.name).toBe("plan");
    const input = call?.args[0] as Record<string, unknown>;
    expect(input).toMatchObject({ ...request, encrypted: "encrypted-snapshot" });
    expect((input.permit_public_key as NodeCrypto.KeyObject).type).toBe("public");
    const text = yield* output;
    expect(text).toContain(fakePlan.plan_sha256);
    expect(text).toContain("synthetic.source");
    expect(text).not.toContain(secretRow);
  }).pipe(provided, Effect.ensuring(Effect.sync(h.cleanup)));
});

it.effect("apply, readback and activate pass the rebuilt plan and redact private rows", () => {
  const h = harness();
  const intent = {
    activation_id: "activation:fixed",
    plan_sha256: fakePlan.plan_sha256,
    home_environment_id: "synthetic.home",
    generations: [{ owner_id: "synthetic.owner", server_generation: 7 }],
  };
  return Effect.gen(function* () {
    yield* h.run(["apply", ...h.base(), "--limit", "9"]);
    yield* h.run(["readback", ...h.base()]);
    yield* h.run(["activate", ...h.base(), "--intent", JSON.stringify(intent)]);
    expect(h.calls.map((c) => c.name)).toEqual([
      "plan",
      "apply",
      "plan",
      "readback",
      "plan",
      "activate",
    ]);
    expect(h.calls[1]?.args).toEqual([fakePlan, 9]);
    expect(h.calls[5]?.args).toEqual([fakePlan, intent]);
    const text = yield* output;
    expect(text).toContain('"state":"verified"');
    expect(text).not.toContain(secretRow);
  }).pipe(provided, Effect.ensuring(Effect.sync(h.cleanup)));
});

it.effect("reverse and transfer preparation print signing digests, never plaintext", () => {
  const h = harness();
  const reverse = {
    snapshot_id: "80000000-0000-4000-8000-000000000001",
    exported_at: "2026-10-25T13:00:00.000Z",
    revision: "6c1e515c57822e4333ce4e2b75024ebbe521661c",
    fence_epoch: 3,
    stream_key_id: "synthetic-stream-key",
    representable_principals: [],
  };
  const transfer = { ...reverse, transfer_id: "transfer:fixed", destination_home: "next" };
  const permit = { key_id: "operator", bundle_sha256: "b".repeat(64), signature: "AAAA" };
  return Effect.gen(function* () {
    yield* h.run(["reverse-prepare", ...h.base(), "--reverse", JSON.stringify(reverse)]);
    yield* h.run(["transfer-prepare", ...h.base(), "--transfer", JSON.stringify(transfer)]);
    yield* h.run([
      "transfer",
      ...h.base(),
      "--transfer",
      JSON.stringify(transfer),
      "--transfer-permit",
      JSON.stringify(permit),
      "--transfer-permit-key-id",
      "operator",
      "--transfer-public-key",
      h.file("transfer.pem", pem),
    ]);
    expect(h.calls.filter((c) => c.name !== "plan").map((c) => c.name)).toEqual([
      "prepareReverse",
      "prepareTransfer",
      "transfer",
    ]);
    const transferCall = h.calls.find((c) => c.name === "transfer")!;
    expect(transferCall.args.slice(0, 4)).toEqual([fakePlan, transfer, permit, "operator"]);
    const text = yield* output;
    expect(text).toContain('"header_sha256"');
    expect(text).toContain('"bundle_sha256"');
    expect(text).toContain("ciphertext");
    expect(text).not.toContain(secretRow);
  }).pipe(provided, Effect.ensuring(Effect.sync(h.cleanup)));
});

it.effect("destination extension commands authenticate the bundle before applying it", () => {
  const h = harness();
  const bundleRequest = {
    stream_key_id: "synthetic-stream-key",
    destination_home: "synthetic.home",
    permit: { key_id: "operator", bundle_sha256: "b".repeat(64), signature: "AAAA" },
    permit_key_id: "operator",
  };
  return Effect.gen(function* () {
    for (const name of ["transfer-apply-extensions", "transfer-readback"])
      yield* h.run([
        name,
        ...h.base(),
        "--bundle",
        h.file("bundle.enc", "encrypted-bundle"),
        "--bundle-request",
        JSON.stringify(bundleRequest),
        "--bundle-public-key",
        h.file("bundle.pem", pem),
      ]);
    expect(h.calls.map((c) => c.name)).toEqual([
      "plan",
      "readTransfer",
      "applyTransferExtensions",
      "plan",
      "readTransfer",
      "readbackTransfer",
    ]);
    expect(h.calls[1]?.args[0]).toMatchObject({ ...bundleRequest, encrypted: "encrypted-bundle" });
    expect(h.calls[2]?.args).toEqual([fakePlan, bundle]);
    expect(yield* output).not.toContain(secretRow);
  }).pipe(provided, Effect.ensuring(Effect.sync(h.cleanup)));
});

it.effect(
  "invalid inputs refuse before the selected home opens; reconciliation is not mounted",
  () => {
    const h = harness();
    return Effect.gen(function* () {
      const refused = [
        ["plan", ...h.base({ ...request, extra: true })],
        ["plan", ...h.base({ ...request, mappings: "none" })],
        ["plan", ...h.base(request, "not a key")],
        ["activate", ...h.base(), "--intent", "{}"],
        ["transfer-complete", ...h.base(), "--receipt", '{"transfer_id":"t"}'],
        ["reconcile-legacy-open", ...h.base()],
        ["plan", "--home", "/synthetic/home", "--key-provider", "server-secret-store"],
      ];
      for (const args of refused)
        expect((yield* Effect.result(h.run(args)))._tag, args.join(" ")).toBe("Failure");
      expect(h.selections).toEqual([]);
      expect(h.calls).toEqual([]);
    }).pipe(provided, Effect.ensuring(Effect.sync(h.cleanup)));
  },
);
