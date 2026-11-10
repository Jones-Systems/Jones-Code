// @effect-diagnostics nodeBuiltinImport:off -- Synthetic receipt fixtures pin raw request-byte digests.
import * as NodeCrypto from "node:crypto";
import * as EffectNodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { WorkstreamReceipt } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerSecretStore from "../../../../auth/ServerSecretStore.ts";
import migration from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import rollupAttemptsMigration from "../../../persistence/Migrations/127_JonesWorkstreamsRollupAttempts.ts";
import * as KeyProvider from "../crypto/keyProvider.ts";
import * as Envelope from "../crypto/envelope.ts";
import * as IdentityIndex from "../crypto/identityIndex.ts";
import * as NativeSql from "./sql.ts";
import * as RegistryStore from "./registryStore.ts";
import * as ArchiveStore from "./archiveStore.ts";
import * as ReceiptStore from "./receiptStore.ts";
import * as HistoryStore from "./historyStore.ts";
import * as EffectStore from "./effectStore.ts";
import * as BindingStore from "./bindingStore.ts";
import * as OperationStore from "./operationStore.ts";

export const scope = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: "owner:test",
};
export const now = "2026-01-01T00:00:00.000Z";
export const hash = "a".repeat(64);
export const keyBytes = Uint8Array.from({ length: 32 }, (_, i) => i + 1);
export const secrets = () =>
  new Map([
    ["workstreams-content-key-test-home", keyBytes],
    ["workstreams-content-key-test-local", keyBytes],
    [
      `workstreams-index-key-${scope.registry_instance_id}`,
      Uint8Array.from({ length: 32 }, (_, i) => 255 - i),
    ],
  ]);
const secretLayer = (values: Map<string, Uint8Array>) =>
  Layer.succeed(
    ServerSecretStore.ServerSecretStore,
    ServerSecretStore.ServerSecretStore.of({
      get: (name) => Effect.succeed(Option.fromNullishOr(values.get(name))),
      set: () => Effect.die("Unexpected secret write"),
      create: () => Effect.die("Unexpected secret creation"),
      getOrCreateRandom: () => Effect.die("Unexpected automatic key creation"),
      remove: () => Effect.die("Unexpected key removal"),
    }),
  );
export const testLayer = (
  options: {
    filename?: string;
    keys?: Map<string, Uint8Array>;
    secrets?: ServerSecretStore.ServerSecretStore["Service"];
  } = {},
) => {
  const db = NativeSql.layer.pipe(
    Layer.provideMerge(
      Envelope.layer.pipe(
        Layer.provideMerge(
          KeyProvider.layer.pipe(
            Layer.provideMerge(
              options.secrets
                ? Layer.succeed(ServerSecretStore.ServerSecretStore, options.secrets)
                : secretLayer(options.keys ?? secrets()),
            ),
          ),
        ),
      ),
    ),
  );
  const receipts = ReceiptStore.layer.pipe(Layer.provideMerge(ArchiveStore.layer));
  const effects = EffectStore.layer.pipe(
    Layer.provideMerge(Layer.mergeAll(receipts, HistoryStore.layer)),
  );
  const operations = OperationStore.layer.pipe(Layer.provideMerge(BindingStore.layer));
  return Layer.mergeAll(RegistryStore.layer, effects, operations, IdentityIndex.layer).pipe(
    Layer.provideMerge(db),
    Layer.provideMerge(NodeSqliteClient.layer({ filename: options.filename ?? ":memory:" })),
    Layer.provideMerge(EffectNodeCrypto.layer),
  );
};
export const seed = Effect.gen(function* () {
  yield* migration;
  yield* rollupAttemptsMigration;
  const registry = yield* RegistryStore.RegistryStore;
  yield* registry.initialize({
    ...scope,
    canonical_origin: "https://native.workstreams.invalid",
    home_environment_id: "home:test",
    server_generation: 1,
    registry_version: 0,
    lineage: "native",
    active_key_id: "test-home",
    state: "active",
  });
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO jones_workstreams_keys(key_id,registry_instance_id,scope,state,created_at) VALUES ('test-home',${scope.registry_instance_id},'home','active',${now}),('test-local',${scope.registry_instance_id},'local','active',${now})`;
});
const decodeReceipt = Schema.decodeUnknownSync(WorkstreamReceipt);
export const pending = (
  commandId: string,
  body = '{"synthetic":"request"}',
): ReceiptStore.ReceiptRecord => ({
  origin: "native",
  request_body: body,
  receipt: decodeReceipt({
    command_id: commandId,
    owner_id: scope.owner_id,
    actor: { principal_id: "principal:test" },
    operation: "verify_reference",
    request_sha256: NodeCrypto.createHash("sha256").update(body).digest("hex"),
    server_generation: 1,
    accepted_at: now,
    state: "pending",
    retry_after_seconds: 1,
  }),
});
export const terminal = (
  record: ReceiptStore.ReceiptRecord,
  registryVersion = 0,
): ReceiptStore.ReceiptRecord => ({
  ...record,
  receipt: decodeReceipt({
    ...record.receipt,
    state: "committed",
    completed_at: now,
    registry_version: registryVersion,
    changed: false,
    effects: {
      workstream_versions: [],
      native_reference_id: null,
      membership_ids: [],
      declaration_id: null,
      declaration_revision: null,
      edge_id: null,
      observation: null,
      registration: null,
      lifecycle_declaration: null,
      coordination_disposition: null,
      native_settlement: null,
    },
  }),
});
export const binding = {
  ...scope,
  home_environment_id: "home:test",
  home_generation: 1,
  principal_id: "principal:test",
  binding_version: 1,
  state: "active" as const,
};
