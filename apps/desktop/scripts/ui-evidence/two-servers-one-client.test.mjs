import test from "node:test";
import assert from "node:assert/strict";
import { canonical, selectedSnapshot, rowSelector, assertPinSnapshot, inspectEncryptedCatalog }
  from "./scenarios/two-servers-one-client.mjs";

const fixture = { projectId: "p", threadId: "t", controlThreadId: "c", liveThreadId: "live" };
const snapshot = () => ({ schemaVersion: 2, snapshotSequence: 4,
  projects: [{ id: "p", title: "colliding project", workspaceRoot: "/synthetic" }],
  threads: ["t", "c", "live"].map((id) => ({ id, projectId: "p", title: id,
    updatedAt: "2026-10-22T00:00:00Z", pinnedAt: null, pinOrderKey: null })), archivedThreads: [] });

test("identical native IDs have disjoint exact environment selectors on both shelves", () => {
  assert.equal(rowSelector("env-b", "t"), 'li[data-thread-item="env-b:t"]');
  for (const pinned of [true, false, undefined]) {
    assert.notEqual(rowSelector("env-a", "t", pinned), rowSelector("env-b", "t", pinned));
    assert.equal(rowSelector("env-b", "t", pinned).includes('$="'), false);
  }
  assert.throws(() => rowSelector('env"]', "t"), /row-identity-not-environment-scoped/);
});

test("strict canonical comparison ignores object key order but no data or sequence changes", () => {
  assert.equal(canonical({ b: 2, a: { z: 0, c: 1 } }), canonical({ a: { c: 1, z: 0 }, b: 2 }));
  const before = snapshot();
  for (const mutate of [
    (value) => value.snapshotSequence++,
    (value) => { value.threads[0].updatedAt = "changed"; },
    (value) => { value.projects[0].workspaceRoot = "/other"; },
    (value) => { value.threads[0].pinnedAt = "2026-10-22T01:00:00Z"; },
  ]) {
    const after = structuredClone(before); mutate(after);
    assert.notEqual(canonical(before), canonical(after));
  }
});

test("selected invariance permits the deliberate live-thread change and still catches collisions", () => {
  const before = snapshot(), after = snapshot();
  after.threads[2].title = "A live updated";
  after.snapshotSequence++;
  assert.equal(selectedSnapshot(after, fixture), selectedSnapshot(before, fixture));
  for (const mutate of [
    (value) => { value.threads[0].title = "wrong server"; },
    (value) => { value.threads[1].pinnedAt = "wrong pin"; },
    (value) => { value.projects[0].title = "wrong project"; },
    (value) => { value.threads.push({ id: "unexpected" }); },
    (value) => { value.archivedThreads.push({ id: "unexpected" }); },
  ]) {
    const changed = structuredClone(after); mutate(changed);
    assert.notEqual(selectedSnapshot(changed, fixture), selectedSnapshot(before, fixture));
  }
  after.threads.push({ ...after.threads[0] });
  assert.throws(() => selectedSnapshot(after, fixture), /missing-or-duplicated/);
});

test("pin oracle is false before the action and rejects collateral control pinning", () => {
  const value = snapshot();
  assertPinSnapshot(value, fixture, false);
  assert.throws(() => assertPinSnapshot(value, fixture, true), /backend-pin-mismatch/);
  Object.assign(value.threads[0], { pinnedAt: "2026-10-22T01:00:00Z", pinOrderKey: "a0" });
  assertPinSnapshot(value, fixture, true);
  assert.throws(() => assertPinSnapshot(value, fixture, false), /backend-pin-mismatch/);
  value.threads[1].pinnedAt = "2026-10-22T01:00:00Z";
  assert.throws(() => assertPinSnapshot(value, fixture, true), /control-pin-changed/);
});

test("encrypted wrapper must be exact, bounded, canonical and contain no B address or bearer marker", () => {
  const encoded = (encryptedCatalog, extra = {}) => Buffer.from(JSON.stringify({ version: 1, encryptedCatalog, ...extra }));
  const cipher = Buffer.from([0, 1, 2, 255]).toString("base64");
  assert.equal(inspectEncryptedCatalog(encoded(cipher)).encrypted, true);
  for (const bytes of [encoded(cipher, { plaintext: "unexpected" }), encoded(`${cipher}\n`),
    encoded(""), Buffer.alloc(1024 * 1024 + 1), Buffer.from("invalid")])
    assert.throws(() => inspectEncryptedCatalog(bytes), /catalog-/);
  for (const secret of ["127.0.0.1:43774", "bearer:env:origin"])
    assert.throws(() => inspectEncryptedCatalog(encoded(Buffer.from(secret).toString("base64"))), /plaintext-present/);
});
