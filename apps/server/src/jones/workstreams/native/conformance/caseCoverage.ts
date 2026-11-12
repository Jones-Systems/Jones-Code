export interface CaseCoverage {
  /** The catalogue has no ID field; its exact frozen name is the identity. */
  readonly id: string;
  readonly ordinal: number;
  readonly status: "ingress-assertion" | "existing-proof" | "excluded" | "missing";
  readonly file: string | null;
  readonly test: string | null;
  readonly detail: string;
}
export const semanticCatalogue = {
  path: "packages/contracts/src/workstreams-fixtures/semantic-cases.json.fixture",
  sha256: "1d939c25a2f9b9aca0191b390ba8739fcd8f6d2e40bb0f1e6ae389c2eb05a6fa",
  oracleRevision: "8c882eedbcf50c68a4dc83d7b5ae5cff1f25e930",
  oraclePath: "contracts/workstreams/v1/fixtures/semantic-cases.json",
} as const;

// These are assertion locations, not a claim that a run passed. Execution results
// and remaining ingress limits belong to the revision-bound qualification receipt.
export const caseCoverage: readonly CaseCoverage[] = [
  {
    ordinal: 0,
    id: "account grant union is not owner authority",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress current owner grant read and write ceilings refuse without registry or provider changes",
    detail:
      "Current persisted native grant, not account-grant union, authorizes reads/writes; refused writes create no receipt.",
  },
  {
    ordinal: 1,
    id: "owner read is not owner write",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress current owner grant read and write ceilings refuse without registry or provider changes",
    detail:
      "Current persisted native grant, not account-grant union, authorizes reads/writes; refused writes create no receipt.",
  },
  {
    ordinal: 2,
    id: "owner write is not source content authority",
    status: "missing",
    file: null,
    test: null,
    detail:
      "Mounted write-only owner grant cannot read reference metadata or request unknown source-content raw action: HTTP500/400, no content or business changes/provider calls. This validates current read ceilings and the closed command surface; independent source-content authorization has no native content route and remains unproved.",
  },
  {
    ordinal: 3,
    id: "same native ID different account is distinct",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress account scoped IDs remain distinct and PR locator mismatch refuses before verification",
    detail:
      "Register through authenticated native gateway; distinct account identity rows and locator rejection.",
  },
  {
    ordinal: 4,
    id: "unknown account provenance cannot be a fake account",
    status: "missing",
    file: null,
    test: null,
    detail:
      "Mounted self-asserted account labels stay quarantined then verification-failed with no attestation or provider calls; unknown provenance variants are refused by the HTTP decoder. The frozen vector requires independent evidence that a resource is account-specific, which this native composition does not supply. Its not_account_scoped refusal remains unproved; no policy or provenance is invented.",
  },
  {
    ordinal: 5,
    id: "PR tuple must match locator",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress account scoped IDs remain distinct and PR locator mismatch refuses before verification",
    detail:
      "Register through authenticated native gateway; distinct account identity rows and locator rejection.",
  },
  {
    ordinal: 6,
    id: "second active primary is rejected",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress primary secondary move rollback reattach and removal never settle source",
    detail:
      "Real SQLite versions and membership episodes, secondary preservation, rollback and zero synthetic source-settlement calls.",
  },
  {
    ordinal: 7,
    id: "explicit secondary association coexists with primary",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress primary secondary move rollback reattach and removal never settle source",
    detail:
      "Real SQLite versions and membership episodes, secondary preservation, rollback and zero synthetic source-settlement calls.",
  },
  {
    ordinal: 8,
    id: "remove and reattach primary creates a new episode without settlement",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress primary secondary move rollback reattach and removal never settle source",
    detail:
      "Real SQLite versions and membership episodes, secondary preservation, rollback and zero synthetic source-settlement calls.",
  },
  {
    ordinal: 9,
    id: "move primary preserves secondary associations",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress primary secondary move rollback reattach and removal never settle source",
    detail:
      "Real SQLite versions and membership episodes, secondary preservation, rollback and zero synthetic source-settlement calls.",
  },
  {
    ordinal: 10,
    id: "move version failure is atomic",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress primary secondary move rollback reattach and removal never settle source",
    detail:
      "Real SQLite versions and membership episodes, secondary preservation, rollback and zero synthetic source-settlement calls.",
  },
  {
    ordinal: 11,
    id: "cross-relation cycles fail",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress declarations are inert revisions and relation cycles serialize",
    detail:
      "Declaration revision/withdrawal and real serialized reciprocal/cross-relation edges; no instruction interpreter or approval port.",
  },
  {
    ordinal: 12,
    id: "concurrent reciprocal edges serialize",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress declarations are inert revisions and relation cycles serialize",
    detail:
      "Declaration revision/withdrawal and real serialized reciprocal/cross-relation edges; no instruction interpreter or approval port.",
  },
  {
    ordinal: 13,
    id: "self edge fails",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress declarations are inert revisions and relation cycles serialize",
    detail:
      "Declaration revision/withdrawal and real serialized reciprocal/cross-relation edges; no instruction interpreter or approval port.",
  },
  {
    ordinal: 14,
    id: "declaration cannot authorize operations",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress declarations are inert revisions and relation cycles serialize",
    detail:
      "Declaration revision/withdrawal and real serialized reciprocal/cross-relation edges; no instruction interpreter or approval port.",
  },
  {
    ordinal: 15,
    id: "terminal replay returns original exact bytes",
    status: "ingress-assertion",
    file: "httpParity.test.ts",
    test: "mounted HTTP terminal same-ID submit replay preserves original bytes after registry advancement",
    detail:
      "Mounted wire-byte assertions compare initial terminal POST, GET and same-ID POST replay after registry advancement. Current authorization and original command digest remain required; execution outcome is recorded separately.",
  },
  {
    ordinal: 16,
    id: "changed command bytes conflict",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress exact byte replay lost reply same identity and changed bytes conflict",
    detail:
      "Retained receipt adoption, unchanged business state, different exact request bytes conflict and current grant recheck.",
  },
  {
    ordinal: 17,
    id: "revoked owner grant blocks terminal replay",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress exact byte replay lost reply same identity and changed bytes conflict",
    detail:
      "Retained receipt adoption, unchanged business state, different exact request bytes conflict and current grant recheck.",
  },
  {
    ordinal: 18,
    id: "lost response reconciles same identity",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress exact byte replay lost reply same identity and changed bytes conflict",
    detail:
      "Retained receipt adoption, unchanged business state, different exact request bytes conflict and current grant recheck.",
  },
  {
    ordinal: 19,
    id: "missing receipt does not prove no effect",
    status: "existing-proof",
    file: "semanticTransport.test.ts",
    test: "lost reply is adopted from the real home receipt and a destination mismatch never becomes absence",
    detail:
      "Native home lookup distinguishes proven_absent from wrong-destination unknown; bare gateway absent is not by itself dispatch authority.",
  },
  {
    ordinal: 20,
    id: "generation change prevents blind retry",
    status: "existing-proof",
    file: "semanticTransport.test.ts",
    test: "grants, exact binding, and generation fence both reads and dispatch",
    detail:
      "Actual native session/transport generation and binding checks; synthetic caller fixture, not authenticated process restart.",
  },
  {
    ordinal: 21,
    id: "cursor cannot cross owner or route",
    status: "ingress-assertion",
    file: "httpParity.test.ts",
    test: "mounted cursor cannot cross authenticated native owner homes or routes",
    detail:
      "Real issued/admitted native homes for two owners accept their own continuation and refuse the other owner's cursor and a same-home route substitution, without mutation/provider calls. Homes have separate process-local cursor signing keys; this is ingress isolation, not a shared-key owner-discriminator unit proof. Owner/account AAD remains separately counted once at row31.",
  },
  {
    ordinal: 22,
    id: "mutation between pages invalidates cursor",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress history complete pages 1 7 100 share versions high water and cursor scope",
    detail:
      "Global shared-version rejected history, high-water continuation, scoped route refusal and stale registry cursor. The mounted cross-owner/route ingress proof is recorded at row21; AAD isolation remains separate.",
  },
  {
    ordinal: 23,
    id: "audit no-op pagination holds high-water",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress history complete pages 1 7 100 share versions high water and cursor scope",
    detail:
      "Global shared-version rejected history, high-water continuation, scoped route refusal and stale registry cursor. The mounted cross-owner/route ingress proof is recorded at row21; AAD isolation remains separate.",
  },
  {
    ordinal: 24,
    id: "unlinked PR cannot refresh",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress real GitHub observer preserves inaccessible snapshots bounds rates refuses redirects and never infers completion",
    detail:
      "Production GitHub observer, resolver, runner and native local transport over a disclosed synthetic HTTP/source port: closed-episode refusal, uncached 304, retained 404/rate/redirect/calendar-invalid snapshots, bounded retry and merged observation without lifecycle/membership changes. This is not mounted startup GitHub configuration or process network proof.",
  },
  {
    ordinal: 25,
    id: "404 preserves last success",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress real GitHub observer preserves inaccessible snapshots bounds rates refuses redirects and never infers completion",
    detail:
      "Production GitHub observer, resolver, runner and native local transport over a disclosed synthetic HTTP/source port: closed-episode refusal, uncached 304, retained 404/rate/redirect/calendar-invalid snapshots, bounded retry and merged observation without lifecycle/membership changes. This is not mounted startup GitHub configuration or process network proof.",
  },
  {
    ordinal: 26,
    id: "304 requires prior success",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress real GitHub observer preserves inaccessible snapshots bounds rates refuses redirects and never infers completion",
    detail:
      "Production GitHub observer, resolver, runner and native local transport over a disclosed synthetic HTTP/source port: closed-episode refusal, uncached 304, retained 404/rate/redirect/calendar-invalid snapshots, bounded retry and merged observation without lifecycle/membership changes. This is not mounted startup GitHub configuration or process network proof.",
  },
  {
    ordinal: 27,
    id: "rate limit is bounded and preserves snapshot",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress real GitHub observer preserves inaccessible snapshots bounds rates refuses redirects and never infers completion",
    detail:
      "Production GitHub observer, resolver, runner and native local transport over a disclosed synthetic HTTP/source port: closed-episode refusal, uncached 304, retained 404/rate/redirect/calendar-invalid snapshots, bounded retry and merged observation without lifecycle/membership changes. This is not mounted startup GitHub configuration or process network proof.",
  },
  {
    ordinal: 28,
    id: "provider redirect is not followed",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress real GitHub observer preserves inaccessible snapshots bounds rates refuses redirects and never infers completion",
    detail:
      "Production GitHub observer, resolver, runner and native local transport over a disclosed synthetic HTTP/source port: closed-episode refusal, uncached 304, retained 404/rate/redirect/calendar-invalid snapshots, bounded retry and merged observation without lifecycle/membership changes. This is not mounted startup GitHub configuration or process network proof.",
  },
  {
    ordinal: 29,
    id: "membership removed during refresh fences completion",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress membership removal during real PR refresh fences completion without an observation",
    detail:
      "Deferred gates the real observer while a real native command removes membership. Completion fails version_conflict, retains no observation and preserves the closed episode. No sleeps or polling.",
  },
  {
    ordinal: 30,
    id: "PR merged does not complete workstream",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress real GitHub observer preserves inaccessible snapshots bounds rates refuses redirects and never infers completion",
    detail:
      "Production GitHub observer, resolver, runner and native local transport over a disclosed synthetic HTTP/source port: closed-episode refusal, uncached 304, retained 404/rate/redirect/calendar-invalid snapshots, bounded retry and merged observation without lifecycle/membership changes. This is not mounted startup GitHub configuration or process network proof.",
  },
  {
    ordinal: 31,
    id: "owner AAD cannot decrypt another owner or account domain",
    status: "existing-proof",
    file: "../crypto/envelope.test.ts",
    test: "AES-GCM authenticates every AAD field and rejects ciphertext, nonce, tag and key-ID substitution",
    detail:
      "Real encryption/store layer rejects owner, registry and account-purpose substitution using synthetic keys. This row is counted once, with no ingress claim.",
  },
  {
    ordinal: 32,
    id: "no summary needed for organization",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress metadata lifecycle progress ordering appearance and independent identity",
    detail:
      "Native metadata has no rollup dependency, distinct same-name IDs, immutable lifecycle reopening and independent progress/appearance.",
  },
  {
    ordinal: 33,
    id: "schema-valid invalid calendar timestamp rejected",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress real GitHub observer preserves inaccessible snapshots bounds rates refuses redirects and never infers completion",
    detail:
      "Production GitHub observer, resolver, runner and native local transport over a disclosed synthetic HTTP/source port: closed-episode refusal, uncached 304, retained 404/rate/redirect/calendar-invalid snapshots, bounded retry and merged observation without lifecycle/membership changes. This is not mounted startup GitHub configuration or process network proof.",
  },
  {
    ordinal: 34,
    id: "name is metadata not inferred identity",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress metadata lifecycle progress ordering appearance and independent identity",
    detail:
      "Native metadata has no rollup dependency, distinct same-name IDs, immutable lifecycle reopening and independent progress/appearance.",
  },
  {
    ordinal: 35,
    id: "account grants cannot bootstrap owner CSRF",
    status: "excluded",
    file: null,
    test: null,
    detail:
      "CP owner-session CSRF bootstrap is a CP trusted-owner browser endpoint. Native bearer/source admission does not mint or accept its CSRF.",
  },
  {
    ordinal: 36,
    id: "pending receipt polling preserves terminal business bytes",
    status: "ingress-assertion",
    file: "httpParity.test.ts",
    test: "mounted HTTP polls persisted %s through real source recovery and terminal byte replay",
    detail:
      "Mounted raw POST/GET retains unnegotiated pending/unresolved200 and explicitly opted-in202 receipts, then real source attempt lookup/recovery completes once and terminal200 GET/POST bytes remain exact after advancement. Unresolved is a disclosed encrypted-store fixture of an authentic pending receipt, not claimed runner emission. Pinned b9719594 typed clients decode legacy receipts and definitive stale refusals; current-client response-header/schema/export hooks are parent-owned and require composed qualification.",
  },
  {
    ordinal: 37,
    id: "quarantined registration cannot become membership or refresh",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress quarantined reference refuses membership without provider work",
    detail:
      "Actual issued native source enrollment, quarantine-to-attested evidence, membership refusal before verification and after attestation expiry. Changed-authority variants additionally use admission.test.ts.",
  },
  {
    ordinal: 38,
    id: "attestation gates exact verified identity",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress quarantined reference refuses membership without provider work",
    detail:
      "Actual issued native source enrollment, quarantine-to-attested evidence, membership refusal before verification and after attestation expiry. Changed-authority variants additionally use admission.test.ts.",
  },
  {
    ordinal: 39,
    id: "expired or changed native authority requires re-attestation",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress quarantined reference refuses membership without provider work",
    detail:
      "Actual issued native source enrollment, quarantine-to-attested evidence, membership refusal before verification and after attestation expiry. Changed-authority variants additionally use admission.test.ts.",
  },
  {
    ordinal: 40,
    id: "coordination disposition and native settlement have separate partial outcomes",
    status: "ingress-assertion",
    file: "operationsParity.test.ts",
    test: "mounted source settlement %s remains distinct from coordination and exact replay dispatches once",
    detail:
      "Table executes production source admission/provider algorithm and real SQL attempt persistence for committed and denied synthetic engine/evidence outcomes. Coordination closes its episode independently; native denied result remains a separate committed command with changed=false, exact replay dispatches once. Synthetic provider metadata is a disclosed port, not the real orchestration event ledger.",
  },
  {
    ordinal: 41,
    id: "membership removal never settles native T3 state",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress primary secondary move rollback reattach and removal never settle source",
    detail:
      "Real SQLite versions and membership episodes, secondary preservation, rollback and zero synthetic source-settlement calls.",
  },
  {
    ordinal: 42,
    id: "browser mutation missing Origin rejects before admission",
    status: "excluded",
    file: null,
    test: null,
    detail:
      "Frozen CP trustedOwnerIngress requires Origin even with CSRF; that CP endpoint is not mounted by native environment HTTP.",
  },
  {
    ordinal: 43,
    id: "browser mutation forged Origin rejects before admission",
    status: "excluded",
    file: null,
    test: null,
    detail:
      "Frozen exact-configured CP browser Origin validation is not the native bearer ingress. Native HTTP authentication negatives are exercised separately.",
  },
  {
    ordinal: 44,
    id: "browser mutation missing or forged CSRF rejects before admission",
    status: "excluded",
    file: null,
    test: null,
    detail:
      "Frozen CP CSRF variants (missing, forged, expired, cross-session) are not native environment bearer/session credentials.",
  },
  {
    ordinal: 45,
    id: "browser supplied trusted identity header is ignored and rejected",
    status: "excluded",
    file: null,
    test: null,
    detail:
      "Frozen CP trusted-proxy Tailscale-User-Login identity injection is absent. Native authenticated HTTP ignores that authority model and requires its own session.",
  },
  {
    ordinal: 46,
    id: "terminal lifecycle reopening appends a later immutable declaration",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress metadata lifecycle progress ordering appearance and independent identity",
    detail:
      "Native metadata has no rollup dependency, distinct same-name IDs, immutable lifecycle reopening and independent progress/appearance.",
  },
  {
    ordinal: 47,
    id: "closed coordination taxonomies remain independent",
    status: "ingress-assertion",
    file: "gatewayParity.test.ts",
    test: "ingress metadata lifecycle progress ordering appearance and independent identity",
    detail:
      "Native metadata has no rollup dependency, distinct same-name IDs, immutable lifecycle reopening and independent progress/appearance.",
  },
];
