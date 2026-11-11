// @effect-diagnostics nodeBuiltinImport:off -- Operator-selected public verification keys only; no private key is read.
import * as NodeCrypto from "node:crypto";
import * as Console from "effect/Console";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { Command, Flag } from "effect/cli";
import { canonicalWorkstreamsSnapshotJson } from "@t3tools/contracts/jones/workstreamsSnapshot";
import { canonical, digest } from "./canonical.ts";
import * as NativeSql from "../store/sql.ts";
import * as Registry from "../store/registryStore.ts";
import * as Archive from "../store/archiveStore.ts";
import * as Identity from "../crypto/identityIndex.ts";
import * as Keys from "../crypto/keyProvider.ts";
import { plan } from "./plan.ts";
import { apply } from "./apply.ts";
import { readback } from "./readback.ts";
import { reconcileLegacyOpen, LegacySourceLookup } from "./reconcileLegacyOpen.ts";
import { activate } from "./activate.ts";
import { prepareReverse, reverseExport } from "./reverseExport.ts";
import {
  prepareTransfer,
  transfer,
  readTransfer,
  applyTransferExtensions,
  readbackTransfer,
  completeTransfer,
} from "./transfer.ts";
type Bound<F extends (...args: never[]) => Effect.Effect<unknown, unknown, unknown>> = (
  ...args: Parameters<F>
) => Effect.Effect<Effect.Success<ReturnType<F>>, Effect.Error<ReturnType<F>>>;
export class WorkstreamsTransition extends Context.Service<
  WorkstreamsTransition,
  {
    readonly plan: Bound<typeof plan>;
    readonly apply: Bound<typeof apply>;
    readonly readback: Bound<typeof readback>;
    readonly reconcileLegacyOpen: Bound<typeof reconcileLegacyOpen>;
    readonly activate: Bound<typeof activate>;
    readonly prepareReverse: Bound<typeof prepareReverse>;
    readonly reverseExport: Bound<typeof reverseExport>;
    readonly prepareTransfer: Bound<typeof prepareTransfer>;
    readonly transfer: Bound<typeof transfer>;
    readonly readTransfer: Bound<typeof readTransfer>;
    readonly applyTransferExtensions: Bound<typeof applyTransferExtensions>;
    readonly readbackTransfer: Bound<typeof readbackTransfer>;
    readonly completeTransfer: Bound<typeof completeTransfer>;
  }
>()("t3/jones/workstreams/native/transition/cli/WorkstreamsTransition") {}
const make = Effect.gen(function* () {
  const sourceLookup = yield* LegacySourceLookup;
  const sql = yield* SqlClient.SqlClient,
    db = yield* NativeSql.NativeSql,
    registry = yield* Registry.RegistryStore,
    archive = yield* Archive.ArchiveStore,
    identity = yield* Identity.WorkstreamsIdentityIndex,
    keys = yield* Keys.WorkstreamsKeyProvider;
  const provide = <A, E>(
    effect: Effect.Effect<
      A,
      E,
      | SqlClient.SqlClient
      | NativeSql.NativeSql
      | Registry.RegistryStore
      | Archive.ArchiveStore
      | Identity.WorkstreamsIdentityIndex
      | Keys.WorkstreamsKeyProvider
    >,
  ) =>
    effect.pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.provideService(NativeSql.NativeSql, db),
      Effect.provideService(Registry.RegistryStore, registry),
      Effect.provideService(Archive.ArchiveStore, archive),
      Effect.provideService(Identity.WorkstreamsIdentityIndex, identity),
      Effect.provideService(Keys.WorkstreamsKeyProvider, keys),
    );
  return WorkstreamsTransition.of({
    plan: (...args) => provide(plan(...args)),
    apply: (...args) => provide(apply(...args)),
    readback: (...args) => provide(readback(...args)),
    reconcileLegacyOpen: (...args) =>
      provide(reconcileLegacyOpen(...args)).pipe(
        Effect.provideService(LegacySourceLookup, sourceLookup),
      ),
    activate: (...args) => provide(activate(...args)),
    prepareReverse: (...args) => provide(prepareReverse(...args)),
    reverseExport: (...args) => provide(reverseExport(...args)),
    prepareTransfer: (...args) => provide(prepareTransfer(...args)),
    transfer: (...args) => provide(transfer(...args)),
    readTransfer: (...args) => provide(readTransfer(...args)),
    applyTransferExtensions: (...args) => provide(applyTransferExtensions(...args)),
    readbackTransfer: (...args) => provide(readbackTransfer(...args)),
    completeTransfer: (...args) => provide(completeTransfer(...args)),
  });
});
/** @public Operator composition supplies J1 persistence and explicitly provisioned keys. */
export const layer = Layer.effect(WorkstreamsTransition, make);

export class TransitionCliError extends Schema.TaggedError<TransitionCliError>()(
  "TransitionCliError",
  { code: Schema.Literals(["invalid_request", "invalid_public_key"]) },
) {
  override get message(): string {
    return "Workstreams transition command refused (" + this.code + "). No transition ran.";
  }
}
const Text = Schema.String;
const Generation = Schema.Struct({ owner_id: Text, server_generation: Schema.Int });
const SignedPermit = Schema.Struct({ key_id: Text, header_sha256: Text, signature: Text });
const TransferPermit = Schema.Struct({ key_id: Text, bundle_sha256: Text, signature: Text });
/** `plan` inputs other than the encrypted stream and public key, which come from files. */
const ImportRequest = Schema.Struct({
  import_run_id: Text,
  stream_key_id: Text,
  permit: SignedPermit,
  permit_key_id: Text,
  home_environment_id: Text,
  active_key_id: Text,
  mappings: Schema.Array(
    Schema.Struct({
      principal_id: Text,
      environment_id: Text,
      source_instance_id: Text,
      evidence_sha256: Text,
    }),
  ),
});
const ActivationIntent = Schema.Struct({
  reconciliation: Schema.optionalKey(
    Schema.Struct({
      delta_root: Text,
      registry_versions: Schema.Array(
        Schema.Struct({ owner_id: Text, registry_version: Schema.Int }),
      ),
    }),
  ),
  activation_id: Text,
  plan_sha256: Text,
  home_environment_id: Text,
  generations: Schema.Array(Generation),
});
const reverseFields = {
  snapshot_id: Text,
  exported_at: Text,
  revision: Text,
  fence_epoch: Schema.Int,
  stream_key_id: Text,
  representable_principals: Schema.Array(Text),
};
const ReverseInput = Schema.Struct(reverseFields);
const TransferInput = Schema.Struct({
  ...reverseFields,
  transfer_id: Text,
  destination_home: Text,
});
/** `readTransfer` inputs other than the encrypted bundle and public key, which come from files. */
const BundleRequest = Schema.Struct({
  stream_key_id: Text,
  destination_home: Text,
  permit: TransferPermit,
  permit_key_id: Text,
});
const TransferReceipt = Schema.Struct({
  transfer_id: Text,
  destination_home: Text,
  bundle_sha256: Text,
  next_generations: Schema.Array(Generation),
});
const decodeJson =
  <S extends Schema.Top & { readonly DecodingServices: never }>(schema: S) =>
  (text: string) =>
    Schema.decodeEffect(Schema.fromJsonString(schema))(text, {
      onExcessProperty: "error",
    }).pipe(Effect.mapError(() => new TransitionCliError({ code: "invalid_request" })));
const publicKey = (pem: string) =>
  Effect.try({
    try: () => {
      const key = NodeCrypto.createPublicKey(pem);
      if (key.type !== "public") throw new Error("not a public key");
      return key;
    },
    catch: () => new TransitionCliError({ code: "invalid_public_key" }),
  });
const print = (value: unknown) => Console.log(canonicalWorkstreamsSnapshotJson(value));

export interface TransitionSelection {
  readonly home: string;
  readonly keyProvider: string;
}
const importFlags = {
  home: Flag.String("home"),
  keyProvider: Flag.String("key-provider"),
  request: Flag.String("request").pipe(
    Flag.withDescription("JSON plan request; the plan is rebuilt from these inputs on every run."),
  ),
  snapshot: Flag.FileText("snapshot").pipe(
    Flag.withDescription("File holding the encrypted X1 snapshot stream."),
  ),
  permitPublicKey: Flag.FileText("permit-public-key").pipe(
    Flag.withDescription("PEM file holding the selected public permit verification key."),
  ),
};
const bundleFlags = {
  bundle: Flag.FileText("bundle").pipe(
    Flag.withDescription("File holding the exact encrypted transfer bundle text."),
  ),
  bundleRequest: Flag.String("bundle-request"),
  bundlePublicKey: Flag.FileText("bundle-public-key"),
};
interface ImportFlags {
  readonly home: string;
  readonly keyProvider: string;
  readonly request: string;
  readonly snapshot: string;
  readonly permitPublicKey: string;
}

/**
 * Explicit operator commands: inputs decode before the home opens, the plan is rebuilt in
 * memory each run, and output carries only digests, counts, state or ciphertext. Legacy
 * reconciliation is not mounted because no host-owned source lookup exists.
 */
export const command = <E, R>(
  bootstrap: (selection: TransitionSelection) => Layer.Layer<WorkstreamsTransition, E, R>,
) => {
  const decodeImport = decodeJson(ImportRequest);
  const run = Effect.fn("WorkstreamsTransitionCli.run")(function* <A, X>(
    flags: ImportFlags,
    use: (
      service: WorkstreamsTransition["Service"],
      plan: Effect.Success<ReturnType<WorkstreamsTransition["Service"]["plan"]>>,
    ) => Effect.Effect<A, X>,
  ) {
    const request = yield* decodeImport(flags.request);
    const permit_public_key = yield* publicKey(flags.permitPublicKey);
    return yield* Effect.gen(function* () {
      const service = yield* WorkstreamsTransition;
      const plan = yield* service.plan({
        ...request,
        encrypted: flags.snapshot,
        permit_public_key,
      });
      return yield* use(service, plan);
    }).pipe(Effect.provide(bootstrap({ home: flags.home, keyProvider: flags.keyProvider })));
  });
  return Command.make("transition").pipe(
    Command.withDescription(
      "Explicitly import, verify, activate, reverse or transfer a fenced registry into this home.",
    ),
    Command.withSubcommands([
      Command.make("plan", importFlags).pipe(
        Command.withDescription("Validate the encrypted snapshot and print the plan digest."),
        Command.withHandler((flags) =>
          run(flags, (_, p) =>
            print({
              import_run_id: p.import_run_id,
              plan_sha256: p.plan_sha256,
              snapshot_sha256: p.snapshot.snapshot_sha256,
              registry_instance_id: p.snapshot.header.registry.registry_instance_id,
              home_environment_id: p.home_environment_id,
              classes: p.snapshot.classes,
              unresolved_sources: p.unresolved_sources,
            }),
          ),
        ),
      ),
      Command.make("apply", {
        ...importFlags,
        limit: Flag.Int("limit").pipe(Flag.optional),
      }).pipe(
        Command.withDescription("Apply or resume the import under its original run ID."),
        Command.withHandler((flags) =>
          run(flags, (s, p) =>
            Effect.flatMap(
              Option.isSome(flags.limit) ? s.apply(p, flags.limit.value) : s.apply(p),
              print,
            ),
          ),
        ),
      ),
      Command.make("readback", importFlags).pipe(
        Command.withDescription("Verify the applied import; prints digests and counts only."),
        Command.withHandler((flags) =>
          run(flags, (s, p) =>
            Effect.flatMap(s.readback(p), ({ rows: _rows, ...result }) => print(result)),
          ),
        ),
      ),
      Command.make("activate", { ...importFlags, intent: Flag.String("intent") }).pipe(
        Command.withDescription("Activate a verified import with an explicit fixed intent."),
        Command.withHandler((flags) =>
          Effect.flatMap(decodeJson(ActivationIntent)(flags.intent), (intent) =>
            run(flags, (s, p) => Effect.flatMap(s.activate(p, intent), print)),
          ),
        ),
      ),
      Command.make("reverse-prepare", { ...importFlags, reverse: Flag.String("reverse") }).pipe(
        Command.withDescription("Print the fenced reverse-export header for operator signing."),
        Command.withHandler((flags) =>
          Effect.flatMap(decodeJson(ReverseInput)(flags.reverse), (input) =>
            run(flags, (s, p) =>
              Effect.flatMap(s.prepareReverse(p, input), (r) =>
                print({
                  header: r.snapshot.header,
                  header_sha256: digest(canonical(r.snapshot.header)),
                  snapshot_sha256: r.snapshot.snapshot_sha256,
                  next_generations: r.next_generations,
                  lineage: r.lineage,
                }),
              ),
            ),
          ),
        ),
      ),
      Command.make("reverse-export", {
        ...importFlags,
        reverse: Flag.String("reverse"),
        reversePermit: Flag.String("reverse-permit"),
        reversePermitKeyId: Flag.String("reverse-permit-key-id"),
        reversePublicKey: Flag.FileText("reverse-public-key"),
      }).pipe(
        Command.withDescription("Write the signed reverse export as ciphertext JSON to stdout."),
        Command.withHandler((flags) =>
          Effect.gen(function* () {
            const input = yield* decodeJson(ReverseInput)(flags.reverse);
            const permit = yield* decodeJson(SignedPermit)(flags.reversePermit);
            const key = yield* publicKey(flags.reversePublicKey);
            return yield* run(flags, (s, p) =>
              Effect.flatMap(
                s.reverseExport(p, input, permit, flags.reversePermitKeyId, key),
                print,
              ),
            );
          }),
        ),
      ),
      Command.make("transfer-prepare", { ...importFlags, transfer: Flag.String("transfer") }).pipe(
        Command.withDescription("Print the transfer bundle digest for operator signing."),
        Command.withHandler((flags) =>
          Effect.flatMap(decodeJson(TransferInput)(flags.transfer), (input) =>
            run(flags, (s, p) =>
              Effect.flatMap(s.prepareTransfer(p, input), (bundle) =>
                print({
                  transfer_id: bundle.transfer_id,
                  destination_home: bundle.destination_home,
                  bundle_sha256: digest(canonical(bundle)),
                  next_generations: bundle.next_generations,
                  lineage: bundle.lineage,
                  extension_count: bundle.extensions.length,
                }),
              ),
            ),
          ),
        ),
      ),
      Command.make("transfer", {
        ...importFlags,
        transfer: Flag.String("transfer"),
        transferPermit: Flag.String("transfer-permit"),
        transferPermitKeyId: Flag.String("transfer-permit-key-id"),
        transferPublicKey: Flag.FileText("transfer-public-key"),
      }).pipe(
        Command.withDescription(
          "Record the transfer and write the signed bundle as ciphertext JSON to stdout.",
        ),
        Command.withHandler((flags) =>
          Effect.gen(function* () {
            const input = yield* decodeJson(TransferInput)(flags.transfer);
            const permit = yield* decodeJson(TransferPermit)(flags.transferPermit);
            const key = yield* publicKey(flags.transferPublicKey);
            return yield* run(flags, (s, p) =>
              Effect.flatMap(s.transfer(p, input, permit, flags.transferPermitKeyId, key), print),
            );
          }),
        ),
      ),
      ...(["transfer-apply-extensions", "transfer-readback"] as const).map((name) =>
        Command.make(name, { ...importFlags, ...bundleFlags }).pipe(
          Command.withDescription(
            name === "transfer-apply-extensions"
              ? "On the destination, apply the authenticated bundle's Jones extensions."
              : "On the destination, verify the applied transfer extensions.",
          ),
          Command.withHandler((flags) =>
            Effect.gen(function* () {
              const request = yield* decodeJson(BundleRequest)(flags.bundleRequest);
              const key = yield* publicKey(flags.bundlePublicKey);
              return yield* run(flags, (s, p) =>
                Effect.gen(function* () {
                  const bundle = yield* s.readTransfer({
                    ...request,
                    encrypted: flags.bundle,
                    public_key: key,
                  });
                  return yield* print(
                    name === "transfer-apply-extensions"
                      ? yield* s.applyTransferExtensions(p, bundle)
                      : yield* s.readbackTransfer(p, bundle),
                  );
                }),
              );
            }),
          ),
        ),
      ),
      Command.make("transfer-complete", { ...importFlags, receipt: Flag.String("receipt") }).pipe(
        Command.withDescription(
          "On the source, mark the fenced registry transferred out against the matching receipt.",
        ),
        Command.withHandler((flags) =>
          Effect.flatMap(decodeJson(TransferReceipt)(flags.receipt), (receipt) =>
            run(flags, (s, p) => Effect.flatMap(s.completeTransfer(p, receipt), print)),
          ),
        ),
      ),
    ]),
  );
};
