import { assert, it } from "@effect/vitest";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import {
  startFailureDiagnosticCode,
  withStartFailureDiagnostic,
} from "./startFailureDiagnostics.ts";

const runtime = (reason: unknown, cause?: unknown) => ({ _tag: "RuntimeUseError", reason, cause });
const known = "runtime_use_prior_outcome_unknown_no_replay";

it("retains exactly the reviewed production reasons as bounded codes", () => {
  const reasons = [
    "runtime_use_app_thread_unbound",
    "runtime_use_attachment_physical_check_inside_transaction",
    "runtime_use_attachment_publication_outside_transaction",
    "runtime_use_attachment_publication_owner_changed",
    "runtime_use_conflicting_deletion",
    "runtime_use_conversation_changed",
    "runtime_use_conversation_missing",
    "runtime_use_conversation_unavailable",
    "runtime_use_default_workspace_mismatch",
    "runtime_use_expected_outbox_claim_changed",
    "runtime_use_identity_changed",
    "runtime_use_initial_producer_thread_mismatch",
    "runtime_use_inventory_changed",
    "runtime_use_open_outside_workspace_exclusion",
    "runtime_use_opened_session_mismatch",
    "runtime_use_original_coverage_changed",
    "runtime_use_original_outbox_reference_changed",
    "runtime_use_original_scope_changed",
    "runtime_use_original_session_replaced",
    "runtime_use_owner_without_actual_database",
    "runtime_use_prior_outcome_unknown_no_replay",
    "runtime_use_producer_session_mismatch",
    "runtime_use_provider_thread_missing",
    "runtime_use_provider_thread_replaced",
    "runtime_use_provider_thread_unbound",
    "runtime_use_requested_workspace_mismatch",
    "runtime_use_response_caller_changed",
    "runtime_use_response_changed_before_dispatch",
    "runtime_use_response_original_intent_changed",
    "runtime_use_response_request_changed",
    "runtime_use_response_request_missing",
    "runtime_use_scope_changed",
    "runtime_use_scope_missing",
    "runtime_use_terminal_cwd_outside_workspace",
    "runtime_use_terminal_scope_changed",
    "runtime_use_terminal_thread_missing",
    "runtime_use_unavailable",
    "runtime_use_uncovered_runtime",
    "runtime_use_workspace_alias",
    "runtime_use_workspace_changed",
    "runtime_use_workspace_not_canonical",
    "runtime_use_workspace_replaced",
  ];
  assert.equal(reasons.length, 42);
  for (const reason of reasons) {
    assert.equal(startFailureDiagnosticCode(runtime(reason)), reason);
    assert.match(reason, /^[a-z_]+$/);
    assert.isBelow(reason.length, 128);
  }
});

it("uses a fixed fallback for unknown or malformed recognized reasons", () => {
  for (const reason of [undefined, null, 42, {}, "", `${known}_PRIVATE`, "PRIVATE".repeat(10000)]) {
    assert.equal(startFailureDiagnosticCode(runtime(reason)), "runtime_use_unclassified");
  }
});

it("accepts only the exact own tag and the outermost recognized error", () => {
  for (const input of [
    null,
    undefined,
    42,
    "RuntimeUseError",
    { _tag: "RuntimeUseErrorExtra", reason: known },
    Object.create(runtime(known)),
  ]) {
    assert.isUndefined(startFailureDiagnosticCode(input));
  }
  assert.equal(startFailureDiagnosticCode({ cause: { cause: runtime(known) } }), known);
  assert.equal(
    startFailureDiagnosticCode(runtime("unknown", runtime(known))),
    "runtime_use_unclassified",
  );
  assert.equal(
    startFailureDiagnosticCode(runtime(known, runtime("runtime_use_unavailable"))),
    known,
  );
});

it("stops on cycles and after sixteen visited objects", () => {
  const cycle: { cause?: unknown } = {};
  cycle.cause = cycle;
  assert.isUndefined(startFailureDiagnosticCode(cycle));
  let chain: unknown = runtime(known);
  for (let index = 0; index < 15; index += 1) chain = { cause: chain };
  assert.equal(startFailureDiagnosticCode(chain), known);
  assert.isUndefined(startFailureDiagnosticCode({ cause: chain }));
});

it("never invokes accessors, proxy traps, or renderers", () => {
  let calls = 0;
  const trap = () => {
    calls += 1;
    throw new Error("PRIVATE");
  };
  const proxy = new Proxy({}, { get: trap, getOwnPropertyDescriptor: trap, getPrototypeOf: trap });
  const revoked = Proxy.revocable({}, {});
  revoked.revoke();
  for (const input of [
    proxy,
    revoked.proxy,
    { cause: proxy },
    Object.defineProperty({}, "_tag", { get: trap }),
    Object.defineProperty({}, "cause", { get: trap }),
  ]) {
    assert.isUndefined(startFailureDiagnosticCode(input));
  }
  assert.equal(
    startFailureDiagnosticCode(
      Object.defineProperty({ _tag: "RuntimeUseError" }, "reason", { get: trap }),
    ),
    "runtime_use_unclassified",
  );
  assert.equal(
    startFailureDiagnosticCode(runtime({ toString: trap, toJSON: trap })),
    "runtime_use_unclassified",
  );
  assert.equal(calls, 0);
});

it("fills only a null code and preserves the existing failure fields", () => {
  const failure = makeProviderFailure({ class: "provider_error", message: "session open failed" });
  const result = withStartFailureDiagnostic(failure, runtime(known));
  assert.deepEqual(result, { ...failure, code: known });
  assert.equal(failure.code, null);
  assert.strictEqual(withStartFailureDiagnostic(failure, {}), failure);
  for (const code of ["explicit", "provider_runtime_binding_unclassified", ""]) {
    const existing = { ...failure, code };
    assert.strictEqual(withStartFailureDiagnostic(existing, runtime(known)), existing);
  }
});
