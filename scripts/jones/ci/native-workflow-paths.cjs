const fs = require("node:fs");
const { execFileSync } = require("node:child_process");

// This is the labelled native workflow's single path allowlist. Keep dependency
// and harness roots here so the anonymous Git-object consumer uses the same list.
const roots = [
  /^\.github\/workflows\/jones-native-workflows\.yml$/,
  /^apps\/desktop\//,
  /^apps\/web\/src\/components\/(Sidebar[^/]*(?:\/.*)?|ChatView[^/]*(?:\/.*)?|sidebar\/.*|chat\/.*)$/,
  /^apps\/web\/src\/jones\/(workstreams|threadConfiguration|pendingSends)\//,
  /^apps\/web\/src\/jones\/threadMonitor\/(actions|planFollowUp|MonitorController)\.[^/]+$/,
  /^apps\/web\/src\/(state\/|lib\/api|composerDraftStore[^/]*$)/,
  /^apps\/server\/src\/jones\/(workstreams|provider|threadConfiguration)\//,
  /^apps\/server\/src\/(orchestration-v2\/|provider\/|auth\/|ws|persistence\/)/,
  /^packages\/(client-runtime|contracts|shared|effect-codex-app-server|provider-[^/]+)\//,
  /^scripts\/jones\/e2e\//,
  /^scripts\/jones\/ci\/native-workflow[^/]*\.(cjs|mjs)$/,
  /^scripts\/lib\//,
  /^native\/browser-secret\//,
  /^apps\/(server|web)\/(package\.json|vite\.config\.ts)$/,
  /^(package\.json|pnpm-lock\.yaml|pnpm-workspace\.yaml|vite\.config\.ts|patches(?:\/.*)?)$/,
];

function parseDiff(bytes) {
  if (!Buffer.isBuffer(bytes)) throw new Error("Unavailable native diff");
  if (bytes.length === 0) return [];
  const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  if (!text.endsWith("\0")) throw new Error("Truncated native diff");
  const fields = text.slice(0, -1).split("\0");
  const paths = [];
  for (let index = 0; index < fields.length;) {
    const status = fields[index++];
    if (!/^(?:[ACDMRTUXB]|[RC]\d{1,3})$/.test(status))
      throw new Error("Invalid native diff status");
    const count = /^[RC]/.test(status) ? 2 : 1;
    for (let n = 0; n < count; n++) {
      const path = fields[index++];
      if (!path || path.startsWith("/") || path.split("/").some((p) => p === "." || p === ".."))
        throw new Error("Invalid native diff path");
      paths.push(path);
    }
  }
  return paths;
}

function selectNative({ eventName, labelled, diff }) {
  if (eventName !== "pull_request") return true;
  if (labelled !== true) return false;
  try {
    return parseDiff(diff).some((path) => roots.some((pattern) => pattern.test(path)));
  } catch {
    return true;
  }
}

function changedPaths(base, head, execute = execFileSync) {
  if (![base, head].every((value) => /^[a-f0-9]{40}$/.test(value ?? ""))) return null;
  try {
    return execute(
      "git",
      ["diff", "--name-status", "-z", "--find-renames", `${base}...${head}`, "--"],
      { maxBuffer: 64 * 1024 * 1024, stdio: ["ignore", "pipe", "pipe"] },
    );
  } catch {
    return null;
  }
}

module.exports = { parseDiff, selectNative, changedPaths };
if (require.main === module) {
  const changed = selectNative({
    eventName: process.env.EVENT_NAME,
    labelled: process.env.NATIVE_LABELLED === "true",
    diff: changedPaths(process.env.BASE_SHA, process.env.HEAD_SHA),
  });
  fs.appendFileSync(process.env.GITHUB_OUTPUT, `changed=${changed}\n`);
}
