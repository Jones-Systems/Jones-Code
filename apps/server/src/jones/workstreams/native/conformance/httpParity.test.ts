import * as Runner from "../registry/effectRunner.ts";
import * as Receipts from "../store/receiptStore.ts";
import * as NativeRegistry from "../registry/service.ts";
const decodeReceiptJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Wire.WorkstreamReceipt));
import { expect, it } from "@effect/vitest";
import {
  Effect,
  Wire,
  fresh,
  seed,
  harness,
  app,
  http,
  json,
  createOperation,
  registry,
  Entry,
  enrollmentRequest,
  canonicalJson,
  revokeGrant,
  Store,
  scope,
  legacyWorkstreamCommandNoEffect,
} from "./testkit.ts";

it.effect(
  "mounted real bearer HTTP raw submit command read operations and stale registry CAS",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f);
      yield* Effect.gen(function* () {
        const h = yield* harness,
          web = yield* http;
        const command = yield* h.command({
          operation: "create_workstream",
          name: "HTTP raw synthetic",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        });
        const denied = yield* web.request("/commands", { command }, false);
        expect(denied.status).toBe(401);
        expect((yield* h.gateway.readMetadata()).items).toHaveLength(0);
        const response = yield* web.request("/commands", { command });
        expect(response.status).toBe(200);
        expect(response.headers.get("cache-control")).toBe("private, no-store");
        const bytes = yield* Effect.promise(() => response.text());
        const receipt = yield* decodeReceiptJson(bytes);
        expect(receipt.state).toBe("committed");
        const polled = yield* web.request("/commands/" + command.command_id);
        expect(yield* Effect.promise(() => polled.text())).toBe(bytes);

        const request = createOperation("http-operation-ingress-0001");
        const op = yield* web.request("/operations/submit", { request, wait_ms: 3000 });
        expect(op.status).toBe(200);
        expect(yield* json(op)).toMatchObject({
          state: "committed",
          operation_id: request.operation_id,
        });
        expect(
          yield* json(
            yield* web.request("/operations/read", { operation_id: request.operation_id }),
          ),
        ).toMatchObject({ operation: { state: "committed" } });
        expect(yield* json(yield* web.request("?limit=100"))).toMatchObject({
          source: "live",
          stale: false,
          items: expect.any(Array),
        });
        const status = yield* json(yield* web.request("/setup-status"));
        expect(status).toMatchObject({
          registry: { state: "ready", registry_id: registry.registry_instance_id },
          t3_source: { state: "ready" },
        });
        yield* web.auth.revokeSession(web.sessionId);
        expect((yield* web.request("?limit=100")).status).toBe(401);
        expect((yield* h.gateway.readMetadata()).items).toHaveLength(2);
        expect(prepared.provider.calls).toHaveLength(0);
      }).pipe(app, Effect.provide(prepared.services));
    }).pipe(Effect.provide(f.layer));
  },
);
import * as Schema from "effect/Schema";

it.effect("mounted HTTP preserves the stale registry version conflict discriminator", () => {
  const f = fresh();
  return Effect.gen(function* () {
    const prepared = yield* seed(f);
    yield* Effect.gen(function* () {
      const h = yield* harness,
        web = yield* http;
      const command = yield* h.command({
        operation: "create_workstream",
        name: "CAS",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      });
      expect((yield* web.modernRequest("/commands", { command })).status).toBe(200);
      const response = yield* web.modernRequest("/commands", {
        command: { ...command, command_id: "http-stale-registry-0000001" },
      });
      expect(response.status).toBe(409);
      expect(yield* json(response)).toMatchObject({ message: "version_conflict" });
    }).pipe(app, Effect.provide(prepared.services));
  }).pipe(Effect.provide(f.layer));
});

it.effect(
  "mounted HTTP v1 and v2 placements retain issued source evidence and native home provenance",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f);
      yield* Effect.gen(function* () {
        const h = yield* harness,
          web = yield* http,
          id = yield* h.create(),
          reference = yield* h.register();
        yield* h.verify(reference);
        yield* h.attach(id, reference);
        const payload = {
          identities: [
            {
              source_instance_id: registry.home_environment_id,
              native_thread_id: "thread-synthetic",
            },
          ],
        };
        const v2 = yield* web.request("/thread-placements", payload, true, {
          "x-t3-workstreams-placement-contract": "workstreams-t3-placement/2.0.0",
        });
        expect(v2.status).toBe(200);
        expect(v2.headers.get("cache-control")).toBe("private, no-store");
        expect(yield* json(v2)).toMatchObject({
          page: {
            provenance: {
              authority: "native-home",
              registry_instance_id: registry.registry_instance_id,
              home_environment_id: registry.home_environment_id,
              home_generation: 1,
            },
            items: [
              {
                kind: "primary",
                attestation_source: "native",
                evidence_contract: "workstreams-t3-provider/2.0.0",
              },
            ],
          },
        });
        const v1 = yield* web.request("/thread-placements", payload);
        expect(v1.status).toBe(200);
        const decoded = yield* Effect.flatMap(
          json(v1),
          Schema.decodeUnknownEffect(Wire.T3PlacementResult),
        );
        if (decoded.readiness === "ready") expect(decoded.page).not.toHaveProperty("provenance");
        expect(
          (yield* web.request("/thread-placements", payload, true, {
            "x-t3-workstreams-placement-contract": "workstreams-t3-placement/3.0.0",
          })).status,
        ).toBe(400);
        expect(prepared.provider.calls).toHaveLength(0);
      }).pipe(app, Effect.provide(prepared.services));
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect(
  "mounted HTTP terminal same-ID submit replay preserves original bytes after registry advancement",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f);
      yield* Effect.gen(function* () {
        const h = yield* harness,
          web = yield* http;
        const command = yield* h.command({
          operation: "create_workstream",
          name: "Retained replay",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        });
        const first = yield* web.request("/commands", { command });
        expect(first.status).toBe(200);
        const bytes = yield* Effect.promise(() => first.text());
        const get = yield* web.request("/commands/" + command.command_id);
        expect(yield* Effect.promise(() => get.text())).toBe(bytes);
        const replay = yield* web.request("/commands", { command });
        expect(replay.status).toBe(200);
        expect(yield* Effect.promise(() => replay.text())).toBe(bytes);
        expect((yield* h.gateway.readMetadata()).items).toHaveLength(1);
      }).pipe(app, Effect.provide(prepared.services));
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect("mounted HTTP pending receipt polling keeps 202 without executing provider work", () => {
  const f = fresh();
  return Effect.gen(function* () {
    const prepared = yield* seed(f);
    yield* Effect.gen(function* () {
      const h = yield* harness,
        web = yield* http,
        entry = yield* Entry.Entry,
        store = yield* NativeRegistry.Registry;
      const ref = yield* h.register();
      const command = yield* h.command({
        operation: "verify_reference",
        native_reference_id: ref,
        expected_attestation_version: 0,
      });
      yield* entry.run("workstreams:write", "thread-synthetic", () =>
        store.submit(
          {
            principal_id: enrollmentRequest.context.principal_id,
            source_environment_id: registry.home_environment_id,
            binding_version: 1,
            origin: { kind: "app" },
          },
          canonicalJson(command),
        ),
      );
      const response = yield* web.modernRequest("/commands/" + command.command_id);
      expect(yield* json(response)).toMatchObject({
        state: "pending",
        command_id: command.command_id,
      });
      expect(response.headers.get("cache-control")).toBe("private, no-store");
      expect(prepared.provider.calls).toHaveLength(0);
      expect(response.status).toBe(202);
    }).pipe(app, Effect.provide(prepared.services));
  }).pipe(Effect.provide(f.layer));
});

it.effect(
  "mounted owner write cannot request source content through the raw command ingress",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f);
      yield* Effect.gen(function* () {
        const web = yield* http,
          h = yield* harness;
        const command = yield* h.command({
          operation: "create_workstream",
          name: "invalid source request",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        });
        const reference = yield* h.register();
        yield* revokeGrant(["workstreams:write"], "active");
        const before = yield* (yield* Store.RegistryStore).context(scope);
        const readDenied = yield* web.request("/references/" + reference);
        expect(readDenied.status).toBe(500);
        expect(yield* json(readDenied)).toMatchObject({ code: "internal_error" });
        const response = yield* web.request("/commands", {
          command: { ...command, action: { operation: "read_native_source_content" } },
        });
        expect(response.status).toBe(400);
        const bytes = yield* Effect.promise(() => response.text());
        expect(bytes).not.toContain("source_content");
        expect(yield* (yield* Store.RegistryStore).list(scope, "records")).toHaveLength(0);
        expect(yield* (yield* Store.RegistryStore).context(scope)).toEqual(before);
        expect(prepared.provider.calls).toHaveLength(0);
      }).pipe(app, Effect.provide(prepared.services));
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect("mounted invalid provider identity refuses before persistence or provider work", () => {
  const f = fresh();
  return Effect.gen(function* () {
    const prepared = yield* seed(f);
    yield* Effect.gen(function* () {
      const web = yield* http,
        h = yield* harness,
        store = yield* Store.RegistryStore;
      const command = yield* h.command({
        operation: "register_reference",
        identity: {
          provider: "t3",
          source_instance_id: registry.home_environment_id,
          resource_kind: "thread",
          id_kind: "internal",
          native_id: "thread-synthetic",
          account_provenance: { kind: "not_account_scoped" },
        },
        pr_locator: null,
      });
      const before = yield* store.context(scope);
      const response = yield* web.request("/commands", {
        command: {
          ...command,
          action: {
            ...command.action,
            identity: {
              provider: "Invalid/Provider",
              source_instance_id: registry.home_environment_id,
              resource_kind: "thread",
              id_kind: "internal",
              native_id: "thread-synthetic",
              account_provenance: { kind: "not_account_scoped" },
            },
          },
        },
      });
      expect(response.status).toBe(400);
      expect(yield* store.context(scope)).toEqual(before);
      expect(yield* store.list(scope, "references")).toHaveLength(0);
      expect(prepared.provider.calls).toHaveLength(0);
    }).pipe(app, Effect.provide(prepared.services));
  }).pipe(Effect.provide(f.layer));
});

it.effect.each(["pending", "unresolved"] as const)(
  "mounted HTTP polls persisted %s through real source recovery and terminal byte replay",
  (state) => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f, true, { nativeOutcome: "pending" });
      yield* Effect.gen(function* () {
        const h = yield* harness,
          web = yield* http,
          runner = yield* Runner.EffectRunner;
        const ref = yield* h.register();
        yield* h.verify(ref);
        const command = yield* h.command({
          operation: "request_native_t3_settlement",
          native_reference_id: ref,
          expected_attestation_version: 1,
          native_action: "settle",
        });
        const first = yield* web.modernRequest("/commands", { command });
        expect(first.status).toBe(202);
        const pending = yield* decodeReceiptJson(yield* Effect.promise(() => first.text()));
        expect(pending.state).toBe("pending");
        expect(prepared.provider.calls).toHaveLength(1);
        if (state === "unresolved") {
          // Seed the supported recovery state through the real encrypted receipt store.
          // This fixture does not claim the current runner emits unresolved itself.
          const receipts = yield* Receipts.ReceiptStore;
          const saved = yield* receipts.read(scope, command.command_id);
          expect(saved).not.toBeNull();
          if (!saved || saved.receipt.state !== "pending")
            throw new Error("Missing pending receipt");
          yield* receipts.save(
            scope,
            { ...saved, receipt: { ...saved.receipt, state: "unresolved" } },
            null,
          );
        }
        const before = yield* (yield* Store.RegistryStore).context(scope);
        let waitingBytes = "";
        for (let i = 0; i < 2; i++) {
          const response = yield* web.modernRequest("/commands/" + command.command_id);
          expect(response.status).toBe(202);
          expect(response.headers.get("cache-control")).toBe("private, no-store");
          const bytes = yield* Effect.promise(() => response.text());
          expect(yield* decodeReceiptJson(bytes)).toEqual({ ...pending, state });
          if (i === 0) waitingBytes = bytes;
          else expect(bytes).toBe(waitingBytes);
        }
        const retry = yield* web.modernRequest("/commands", { command });
        expect(retry.status).toBe(202);
        expect(yield* Effect.promise(() => retry.text())).toBe(waitingBytes);
        expect(yield* (yield* Store.RegistryStore).context(scope)).toEqual(before);
        expect(prepared.provider.calls).toHaveLength(1);
        prepared.provider.commit(prepared.provider.calls[0]!.commandId);
        yield* runner.recoverOne(command.command_id);
        const terminal = yield* web.modernRequest("/commands/" + command.command_id);
        expect(terminal.status).toBe(200);
        const bytes = yield* Effect.promise(() => terminal.text());
        const receipt = yield* decodeReceiptJson(bytes);
        expect(receipt).toMatchObject({
          state: "committed",
          command_id: command.command_id,
          request_sha256: pending.request_sha256,
          effects: { native_settlement: { outcome: "committed" } },
        });
        yield* h.create("Advance after terminal receipt");
        for (const response of [
          yield* web.modernRequest("/commands/" + command.command_id),
          yield* web.modernRequest("/commands", { command }),
        ]) {
          expect(response.status).toBe(200);
          expect(yield* Effect.promise(() => response.text())).toBe(bytes);
        }
        expect(prepared.provider.calls).toHaveLength(1);
      }).pipe(app, Effect.provide(prepared.services));
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect.each(["POST", "GET"] as const)(
  "advertised typed HTTP client decodes pending %s before terminal recovery",
  (method) => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f, true, { nativeOutcome: "pending" });
      yield* Effect.gen(function* () {
        const h = yield* harness,
          web = yield* http;
        const ref = yield* h.register();
        yield* h.verify(ref);
        const command = yield* h.command({
          operation: "request_native_t3_settlement",
          native_reference_id: ref,
          expected_attestation_version: 1,
          native_action: "settle",
        });
        expect((yield* web.modernClient.setupStatus({ headers: web.headers })).registry.state).toBe(
          "ready",
        );
        if (method === "GET")
          expect((yield* web.modernRequest("/commands", { command })).status).toBe(202);
        const receipt =
          method === "POST"
            ? yield* web.modernClient.submit({ headers: web.headers, payload: { command } })
            : yield* web.modernClient.command({
                headers: web.headers,
                params: { commandId: command.command_id },
              });
        expect(receipt.state).toBe("pending");
        expect(
          (yield* web.modernClient.command({
            headers: web.headers,
            params: { commandId: command.command_id },
          })).state,
        ).toBe("pending");
        expect(prepared.provider.calls).toHaveLength(1);
        prepared.provider.commit(prepared.provider.calls[0]!.commandId);
        yield* (yield* Runner.EffectRunner).recoverOne(command.command_id);
        const terminal = yield* web.modernClient.command({
          headers: web.headers,
          params: { commandId: command.command_id },
        });
        expect(terminal.state).toBe("committed");
        expect(
          yield* web.modernClient.submit({ headers: web.headers, payload: { command } }),
        ).toEqual(terminal);
        expect(prepared.provider.calls).toHaveLength(1);
      }).pipe(app, Effect.provide(prepared.services));
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect("mounted cursor cannot cross authenticated native owner homes or routes", () => {
  const first = fresh(),
    second = fresh();
  const ownerPage = (f: ReturnType<typeof fresh>, ownerId: string, foreign?: string) =>
    Effect.gen(function* () {
      const prepared = yield* seed(f, true, { ownerId });
      return yield* Effect.gen(function* () {
        const h = yield* harness,
          web = yield* http;
        const a = yield* h.create("Owner isolated a"),
          b = yield* h.create("Owner isolated b", 10);
        expect((yield* h.gateway.readSession()).ownerId).toBe(ownerId);
        const response = yield* web.request("?limit=1");
        expect(response.status).toBe(200);
        const page = yield* Effect.flatMap(
          json(response),
          Schema.decodeUnknownEffect(Wire.T3WorkstreamListResult),
        );
        expect(page.binding.ownerId).toBe(ownerId);
        expect(page.nextCursor).not.toBeNull();
        const own = yield* web.request("?limit=1&cursor=" + page.nextCursor);
        expect(own.status).toBe(200);
        expect(yield* json(own)).toMatchObject({
          binding: { ownerId },
          items: [{ workstreamId: b }],
          nextCursor: null,
        });
        if (foreign) {
          const before = yield* h.gateway.readSession();
          const denied = yield* web.request("?limit=1&cursor=" + foreign);
          expect(denied.status).toBe(500);
          expect(yield* json(denied)).toMatchObject({ code: "internal_error" });
          const route = yield* web.request("/" + a + "/history?limit=1&cursor=" + page.nextCursor);
          expect(route.status).toBe(500);
          expect(yield* h.gateway.readSession()).toEqual(before);
          expect((yield* h.gateway.readMetadata({ limit: 100 })).items).toHaveLength(2);
        }
        expect(prepared.provider.calls).toHaveLength(0);
        return page.nextCursor!;
      }).pipe(app, Effect.provide(prepared.services));
    }).pipe(Effect.provide(f.layer));
  return Effect.gen(function* () {
    const cursor = yield* ownerPage(first, "owner:cursor-alpha");
    yield* ownerPage(second, "owner:cursor-beta", cursor);
  });
});

it.effect(
  "mounted account labels cannot self-issue attestation or invent provenance variants",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f);
      yield* Effect.gen(function* () {
        const h = yield* harness,
          web = yield* http;
        const command = yield* h.command({
          operation: "register_reference",
          identity: {
            provider: "t3",
            source_instance_id: registry.home_environment_id,
            resource_kind: "thread",
            id_kind: "internal",
            native_id: "thread-synthetic",
            account_provenance: {
              kind: "account",
              account_id: "self-asserted",
              account_namespace: "synthetic-account",
            },
          },
          pr_locator: null,
        });
        const response = yield* web.request("/commands", { command });
        expect(response.status).toBe(200);
        const receipt = yield* decodeReceiptJson(yield* Effect.promise(() => response.text()));
        if (receipt.state !== "committed" || !receipt.effects.native_reference_id)
          throw new Error("Registration was not retained");
        const reference = receipt.effects.native_reference_id;
        expect(receipt.effects.registration?.state).toBe("quarantined");
        const verified = yield* h.committed({
          operation: "verify_reference",
          native_reference_id: reference,
          expected_attestation_version: 0,
        });
        expect(verified.effects.registration).toMatchObject({
          state: "verification-failed",
          evidence: null,
        });
        const before = yield* (yield* Store.RegistryStore).context(scope);
        const invented = yield* web.request("/commands", {
          command: {
            ...(yield* h.command(command.action)),
            action: {
              ...command.action,
              identity: {
                provider: "t3",
                source_instance_id: registry.home_environment_id,
                resource_kind: "thread",
                id_kind: "internal",
                native_id: "thread-synthetic",
                account_provenance: { kind: "unknown" },
              },
            },
          },
        });
        expect(invented.status).toBe(400);
        expect(yield* (yield* Store.RegistryStore).context(scope)).toEqual(before);
        expect(prepared.provider.calls).toHaveLength(0);
      }).pipe(app, Effect.provide(prepared.services));
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect("mounted replay never replaces original accepted noncanonical request bytes", () => {
  const f = fresh();
  return Effect.gen(function* () {
    const prepared = yield* seed(f);
    yield* Effect.gen(function* () {
      const h = yield* harness,
        web = yield* http;
      const command = yield* h.command({
        operation: "create_workstream",
        name: "Original bytes remain authority",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      });
      const body = " \n" + JSON.stringify(command, null, 2) + "\n";
      const accepted = yield* h.gateway.resubmit(command.command_id, body);
      expect(accepted.kind).toBe("receipt");
      const retained = yield* (yield* Receipts.ReceiptStore).read(scope, command.command_id);
      expect(retained?.request_body).toBe(body);
      const get = yield* web.request("/commands/" + command.command_id);
      expect(get.status).toBe(200);
      const bytes = yield* Effect.promise(() => get.text());
      const before = yield* (yield* Store.RegistryStore).context(scope);
      const conflict = yield* web.request("/commands", { command });
      expect(conflict.status).toBe(500);
      expect(yield* json(conflict)).toMatchObject({ code: "internal_error" });
      expect(yield* (yield* Store.RegistryStore).context(scope)).toEqual(before);
      expect(yield* (yield* Receipts.ReceiptStore).read(scope, command.command_id)).toEqual(
        retained,
      );
      const again = yield* web.request("/commands/" + command.command_id);
      expect(yield* Effect.promise(() => again.text())).toBe(bytes);
      expect((yield* h.gateway.readMetadata()).items).toHaveLength(1);
      expect(prepared.provider.calls).toHaveLength(0);
    }).pipe(app, Effect.provide(prepared.services));
  }).pipe(Effect.provide(f.layer));
});

it.effect.each([
  { method: "POST", state: "pending" },
  { method: "GET", state: "pending" },
  { method: "POST", state: "unresolved" },
  { method: "GET", state: "unresolved" },
] as const)(
  "b9719594 200-only typed $method client decodes mounted $state and terminal recovery",
  ({ method, state }) => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f, true, { nativeOutcome: "pending" });
      yield* Effect.gen(function* () {
        const h = yield* harness,
          web = yield* http;
        const ref = yield* h.register();
        yield* h.verify(ref);
        const command = yield* h.command({
          operation: "request_native_t3_settlement",
          native_reference_id: ref,
          expected_attestation_version: 1,
          native_action: "settle",
        });
        if (method === "GET" || state === "unresolved") {
          const response = yield* web.request("/commands", { command });
          expect(response.status).toBe(200);
          if (state === "unresolved") {
            const store = yield* Receipts.ReceiptStore;
            const saved = yield* store.read(scope, command.command_id);
            if (!saved || saved.receipt.state !== "pending")
              throw new Error("Missing pending receipt");
            yield* store.save(scope, { ...saved, receipt: { ...saved.receipt, state } }, null);
          }
        }
        const receipt =
          method === "POST"
            ? yield* web.oldClient.submit({ headers: web.headers, payload: { command } })
            : yield* web.oldClient.command({
                headers: web.headers,
                params: { commandId: command.command_id },
              });
        expect(receipt.state).toBe(state);
        const waiting = yield* web.request("/commands/" + command.command_id);
        expect(waiting.status).toBe(200);
        const waitingBytes = yield* Effect.promise(() => waiting.text());
        const replay = yield* web.request("/commands", { command });
        expect(replay.status).toBe(200);
        expect(yield* Effect.promise(() => replay.text())).toBe(waitingBytes);
        expect(prepared.provider.calls).toHaveLength(1);
        prepared.provider.commit(prepared.provider.calls[0]!.commandId);
        yield* (yield* Runner.EffectRunner).recoverOne(command.command_id);
        const terminal = yield* web.oldClient.command({
          headers: web.headers,
          params: { commandId: command.command_id },
        });
        expect(terminal.state).toBe("committed");
        yield* h.create("Advance after old-client completion");
        expect(yield* web.oldClient.submit({ headers: web.headers, payload: { command } })).toEqual(
          terminal,
        );
        const terminalGet = yield* web.request("/commands/" + command.command_id);
        const terminalBytes = yield* Effect.promise(() => terminalGet.text());
        const terminalPost = yield* web.request("/commands", { command });
        expect(terminalPost.status).toBe(200);
        expect(yield* Effect.promise(() => terminalPost.text())).toBe(terminalBytes);
        expect(prepared.provider.calls).toHaveLength(1);
      }).pipe(app, Effect.provide(prepared.services));
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect(
  "b9719594 classifier clears a definitive absent stale-ID refusal from the real mounted native home",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f);
      yield* Effect.gen(function* () {
        const h = yield* harness,
          web = yield* http;
        const command = yield* h.command({
          operation: "create_workstream",
          name: "Absent stale command",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        });
        yield* h.create("Advance before the absent command");
        const before = yield* (yield* Store.RegistryStore).context(scope);
        const result = yield* web.oldClient
          .submit({ headers: web.headers, payload: { command } })
          .pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        if (result._tag !== "Failure") throw new Error("Stale command dispatched");
        expect(legacyWorkstreamCommandNoEffect(result.failure)).toBe("stale");
        const modern = yield* web.modernRequest("/commands", { command });
        expect(modern.status).toBe(409);
        expect(yield* json(modern)).toMatchObject({ message: "version_conflict" });
        expect(yield* (yield* Receipts.ReceiptStore).read(scope, command.command_id)).toBeNull();
        expect(yield* (yield* Store.RegistryStore).context(scope)).toEqual(before);
        expect((yield* h.gateway.readMetadata()).items).toHaveLength(1);
        expect(prepared.provider.calls).toHaveLength(0);
      }).pipe(app, Effect.provide(prepared.services));
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect.each([
  "",
  "workstreams-http-response/9.0.0",
  "workstreams-http-response/1.0.0, workstreams-http-response/1.0.0",
  "x".repeat(257),
])("mounted HTTP rejects unsupported response contract %s before command effects", (contract) => {
  const f = fresh();
  return Effect.gen(function* () {
    const prepared = yield* seed(f);
    yield* Effect.gen(function* () {
      const h = yield* harness,
        web = yield* http;
      const command = yield* h.command({
        operation: "create_workstream",
        name: "Never dispatched",
        lifecycle: "active",
        progress: { state: "unknown" },
        sort_order: 0,
      });
      const before = yield* (yield* Store.RegistryStore).context(scope);
      for (const [path, body] of [
        ["/commands", { command }],
        ["/commands/" + command.command_id, undefined],
      ] as const) {
        const response = yield* web.request(path, body, true, {
          "x-t3-workstreams-response-contract": contract,
        });
        expect(response.status).toBe(400);
        expect(yield* Effect.promise(() => response.text())).toBe("");
        expect(response.headers.get("cache-control")).toBe("private, no-store");
      }
      expect(yield* (yield* Store.RegistryStore).context(scope)).toEqual(before);
      expect(yield* (yield* Receipts.ReceiptStore).read(scope, command.command_id)).toBeNull();
      expect((yield* h.gateway.readMetadata()).items).toHaveLength(0);
      expect(prepared.provider.calls).toHaveLength(0);
    }).pipe(app, Effect.provide(prepared.services));
  }).pipe(Effect.provide(f.layer));
});
