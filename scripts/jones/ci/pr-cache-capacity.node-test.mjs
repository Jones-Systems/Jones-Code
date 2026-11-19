import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import { managePrCaches } from "./pr-cache-capacity.mjs";

const repo = "repos/Jones-Systems/Jones-Code/actions/caches";
const a = {
  id: 1,
  ref: "refs/pull/1/merge",
  size_in_bytes: 20,
  last_accessed_at: "2026-11-18T10:00:00Z",
};
const b = {
  ...a,
  id: 2,
  ref: "refs/pull/2/merge",
  last_accessed_at: "2026-11-18T09:00:00Z",
};
const main = {
  ...b,
  id: 3,
  ref: "refs/heads/main",
  size_in_bytes: 90,
  last_accessed_at: "2026-11-18T05:00:00Z",
};
const other = { ...main, id: 4, ref: "refs/heads/topic", size_in_bytes: 5 };

function fixture(...pages) {
  const total_count = pages.flat().length;
  const calls = [];
  return {
    calls,
    api(args) {
      calls.push(args);
      if (args[0] === "--paginate") {
        return JSON.stringify(pages.map((actions_caches) => ({ total_count, actions_caches })));
      }
    },
  };
}

NodeTest.test("keeps all caches when within the budget", () => {
  const { api, calls } = fixture([main], [a]);
  NodeAssert.deepEqual(managePrCaches(api, { apply: true, limit: 110 }).evict, []);
  NodeAssert.equal(calls.length, 1);
  NodeAssert.ok(calls[0].includes(`${repo}?per_page=100`));
});

NodeTest.test("counts all cache scopes and evicts the least recently used PR cache first", () => {
  const { api, calls } = fixture([other, a], [main, b]);
  NodeAssert.deepEqual(managePrCaches(api, { apply: true, limit: 115 }), {
    applied: true,
    limit: 115,
    usedBytes: 135,
    remainingBytes: 115,
    evict: [{ id: 2, ref: b.ref, bytes: 20 }],
  });
  NodeAssert.deepEqual(calls.slice(1), [["--method", "DELETE", `${repo}/2`]]);
});

NodeTest.test(
  "reports a plan by default and never evicts a branch cache even if it cannot reach the target",
  () => {
    const { api, calls } = fixture([a, b, main, other]);
    const report = managePrCaches(api, { limit: 80 });
    NodeAssert.equal(report.applied, false);
    NodeAssert.equal(report.remainingBytes, 95);
    NodeAssert.deepEqual(
      report.evict.map(({ id }) => id),
      [2, 1],
    );
    NodeAssert.equal(calls.length, 1);
  },
);

NodeTest.test("refuses to act on duplicate, incomplete or malformed list results", () => {
  for (const pages of [
    [{ total_count: 3, actions_caches: [a, a, main] }],
    [{ total_count: 3, actions_caches: [a, main] }],
    [{ total_count: 2, actions_caches: [a, { ...main, size_in_bytes: "90" }] }],
    [{ total_count: 2, actions_caches: [a, { ...b, last_accessed_at: null }] }],
  ]) {
    NodeAssert.throws(() =>
      managePrCaches(
        (args) => {
          NodeAssert.equal(args[0], "--paginate");
          return JSON.stringify(pages);
        },
        { apply: true, limit: 20 },
      ),
    );
  }
});
