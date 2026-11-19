import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";

const cacheEndpoint = "repos/Jones-Systems/Jones-Code/actions/caches";
const targetBytes = 9 * 1024 ** 3;

function planEviction(pages, limit) {
  const caches = pages.flatMap(({ actions_caches }) => actions_caches);
  const ids = new Set(caches.map(({ id }) => id));

  if (
    pages.length === 0 ||
    pages.some(({ total_count }) => total_count !== caches.length) ||
    ids.size !== caches.length ||
    caches.some(
      (cache) =>
        !Number.isSafeInteger(cache.id) ||
        cache.id <= 0 ||
        !Number.isSafeInteger(cache.size_in_bytes) ||
        cache.size_in_bytes < 0 ||
        typeof cache.ref !== "string",
    )
  ) {
    throw new Error("Cache inventory changed or was incomplete. Recheck before deleting.");
  }

  const usedBytes = caches.reduce((sum, cache) => sum + cache.size_in_bytes, 0);
  let remainingBytes = usedBytes;
  const evict = [];
  if (remainingBytes > limit) {
    const candidates = caches
      .filter(
        ({ ref, size_in_bytes }) => /^refs\/pull\/[1-9]\d*\/merge$/.test(ref) && size_in_bytes > 0,
      )
      .map((cache) => {
        const lastAccess = Date.parse(cache.last_accessed_at);
        if (!Number.isFinite(lastAccess)) {
          throw new Error(`Missing last-access timestamp for cache ${cache.id}.`);
        }
        return { ...cache, lastAccess };
      })
      .sort((a, b) => a.lastAccess - b.lastAccess || a.id - b.id);

    for (const { id, ref, size_in_bytes } of candidates) {
      evict.push({ id, ref, bytes: size_in_bytes });
      remainingBytes -= size_in_bytes;
      if (remainingBytes <= limit) break;
    }
  }
  return { usedBytes, remainingBytes, evict };
}

export function managePrCaches(api, { apply = false, limit = targetBytes } = {}) {
  const pages = JSON.parse(
    api([
      "--paginate",
      "--slurp",
      "-H",
      "Cache-Control: no-cache",
      `${cacheEndpoint}?per_page=100`,
    ]),
  );
  const plan = planEviction(pages, limit);
  if (apply) {
    for (const { id } of plan.evict) {
      api(["--method", "DELETE", `${cacheEndpoint}/${id}`]);
    }
  }
  return { applied: apply, limit, ...plan };
}

if (process.argv[1] && import.meta.url === NodeURL.pathToFileURL(process.argv[1]).href) {
  const args = process.argv.slice(2);
  if (args.length > 1 || (args.length === 1 && args[0] !== "--apply")) {
    throw new Error("Usage: node scripts/jones/ci/pr-cache-capacity.mjs [--apply]");
  }
  const report = managePrCaches(
    (apiArgs) =>
      NodeChildProcess.execFileSync("gh", ["api", ...apiArgs], {
        encoding: "utf8",
        timeout: 10_000,
      }),
    { apply: args[0] === "--apply" },
  );
  console.log(JSON.stringify(report, null, 2));
  if (report.remainingBytes > report.limit) {
    console.log("Shared caches alone exceed the target. They remain eligible for GitHub eviction.");
  }
}
