"use strict";

const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const os = require("node:os");
const { readHostProcess } = require("./host-process.mjs");

function insist(condition, message) {
  if (!condition) throw new Error(message);
}
const sha256 = (bytes) => crypto.createHash("sha256").update(bytes).digest("hex");
function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object")
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`)
      .join(",")}}`;
  return JSON.stringify(value);
}
const digestObject = (value) => sha256(canonical(value));
const isDigest = (value) => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
function absolute(value) {
  insist(
    typeof value === "string" &&
      path.isAbsolute(value) &&
      path.normalize(value) === value &&
      !/[\0\r\n]/.test(value),
    `Expected canonical absolute path: ${value}`,
  );
  return value;
}
function relative(value) {
  insist(
    typeof value === "string" &&
      value !== "" &&
      !path.isAbsolute(value) &&
      path.normalize(value) === value &&
      value !== ".." &&
      !value.startsWith("../") &&
      !/[\0\r\n]/.test(value),
    "Invalid relative state or payload path",
  );
  return value;
}
function regular(file) {
  const stat = fs.lstatSync(file);
  insist(
    stat.isFile() && !stat.isSymbolicLink() && fs.realpathSync(file) === file,
    `File is not a canonical regular file: ${file}`,
  );
  return stat;
}
function hashFile(file) {
  regular(file);
  const hash = crypto.createHash("sha256");
  const fd = fs.openSync(file, "r");
  const buffer = Buffer.allocUnsafe(1024 * 1024);
  try {
    let size;
    while ((size = fs.readSync(fd, buffer, 0, buffer.length, null)) !== 0)
      hash.update(buffer.subarray(0, size));
  } finally {
    fs.closeSync(fd);
  }
  return hash.digest("hex");
}
function verifyArtifact(artifact) {
  absolute(artifact.path);
  insist(isDigest(artifact.sha256), "Expected SHA256");
  insist(hashFile(artifact.path) === artifact.sha256, `Artifact digest mismatch: ${artifact.path}`);
}
function checked(artifact) {
  verifyArtifact(artifact);
  insist(regular(artifact.path).size <= 16 * 1024 * 1024, "Structured artifact exceeds 16 MiB");
  const bytes = fs.readFileSync(artifact.path);
  insist(sha256(bytes) === artifact.sha256, `Artifact digest mismatch: ${artifact.path}`);
  return bytes;
}
function syncDirectory(directory) {
  const fd = fs.openSync(directory, "r");
  try {
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
}
function atomic(file, bytes, mode = 0o600) {
  const temporary = `${file}.swap-${crypto.randomUUID()}`;
  const fd = fs.openSync(temporary, "wx", mode);
  try {
    fs.writeFileSync(fd, bytes);
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  fs.renameSync(temporary, file);
  syncDirectory(path.dirname(file));
}
function writeJson(file, value) {
  atomic(file, `${JSON.stringify(value, null, 2)}\n`);
}
function readJson(file) {
  regular(file);
  return JSON.parse(fs.readFileSync(file, "utf8"));
}
const STATE_FILES = [
  "userdata/state.sqlite",
  "userdata/state.sqlite-wal",
  "userdata/state.sqlite-shm",
  "userdata/statev2.sqlite",
  "userdata/statev2.sqlite-wal",
  "userdata/statev2.sqlite-shm",
  "userdata/settings.json",
  "userdata/keybindings.json",
  "userdata/server-runtime.json",
  "userdata/environment-id",
  "userdata/anonymous-id",
  "runtime/service-state.json",
];
function bindingDigest(plan) {
  const {
    schema,
    host,
    uid,
    platform,
    arch,
    home,
    stateDir,
    environmentId,
    service,
    source,
    target,
    descriptorUrl,
    stateFiles,
    runtimeState = null,
  } = plan;
  return digestObject({
    schema,
    host,
    uid,
    platform,
    arch,
    home,
    stateDir,
    environmentId,
    service,
    source,
    target,
    descriptorUrl,
    stateFiles,
    runtimeState,
  });
}
function validatePlan(input) {
  const p = structuredClone(input);
  insist(p.schema === "server-swap-plan/1", "Unsupported plan schema");
  insist(
    typeof p.id === "string" && /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/.test(p.id),
    "Invalid transaction ID",
  );
  insist(
    typeof p.host === "string" && p.host.length > 0 && Number.isInteger(p.uid) && p.uid >= 0,
    "Host and uid required",
  );
  insist(
    ["linux", "darwin"].includes(p.platform) && ["arm64", "x64"].includes(p.arch),
    "Unsupported host platform",
  );
  absolute(p.home);
  insist(
    typeof p.environmentId === "string" && p.environmentId.length > 0,
    "Environment ID required",
  );
  insist(
    p.service && ["systemd-user", "systemd-system", "launchd"].includes(p.service.manager),
    "Unsupported service manager",
  );
  insist(/^[a-zA-Z0-9][a-zA-Z0-9_.@-]*$/.test(p.service.name), "Invalid service name");
  absolute(p.service.configPath);
  insist(
    p.service.elevation === undefined ||
      (p.service.manager === "systemd-system" && p.service.elevation === "sudo-noninteractive"),
    "Elevation requires explicit systemd-system sudo-noninteractive selection",
  );
  p.service.dropIns ??= [];
  insist(Array.isArray(p.service.dropIns), "dropIns must be an explicit artifact list");
  for (const artifact of p.service.dropIns) {
    absolute(artifact.path);
    insist(isDigest(artifact.sha256), "Drop-in digest required");
  }
  insist(
    new Set(p.service.dropIns.map((a) => a.path)).size === p.service.dropIns.length,
    "Duplicate drop-in",
  );
  const dropInNames = p.service.dropIns.map((artifact) => path.basename(artifact.path));
  insist(
    new Set(dropInNames).size === dropInNames.length &&
      canonical(dropInNames) === canonical([...dropInNames].sort()),
    "Drop-ins must have unique names in systemd lexicographic order",
  );
  if (p.service.fragment) {
    insist(p.service.manager !== "launchd", "A fragment binding applies only to systemd");
    absolute(p.service.fragment.path);
    insist(
      isDigest(p.service.fragment.sha256) && p.service.fragment.path !== p.service.configPath,
      "A selected drop-in requires a distinct exact base fragment",
    );
    insist(
      p.service.configPath.endsWith(".conf") &&
        p.service.dropIns.some((entry) => entry.path === p.service.configPath),
      "Selected config must be an inventoried .conf drop-in",
    );
  } else
    insist(
      !p.service.dropIns.some((entry) => entry.path === p.service.configPath),
      "Config path cannot also be a drop-in without fragment binding",
    );
  if (p.service.manager === "launchd")
    insist(
      p.platform === "darwin" && p.service.domain === `gui/${p.uid}`,
      "Only the exact user launchd GUI domain is supported",
    );
  else
    insist(
      p.platform === "linux" && p.service.name.endsWith(".service"),
      "systemd requires a Linux .service unit",
    );
  for (const side of ["source", "target"]) {
    const release = p[side];
    insist(
      release &&
        ["jones", "t3"].includes(release.distribution) &&
        typeof release.version === "string" &&
        /^\d+\.\d+\.\d+(?:[-+][a-zA-Z0-9.+-]+)?$/.test(release.version),
      `Exact ${side} distribution/version required`,
    );
    for (const artifact of [release.config, release.executable]) {
      insist(artifact && isDigest(artifact.sha256), `Invalid ${side} artifact`);
      absolute(artifact.path);
    }
    insist(
      release.config.path !== p.service.configPath,
      "Original and target configs must be separate staged artifacts",
    );
    insist(
      Array.isArray(release.argv) &&
        release.argv.length > 0 &&
        release.argv.every((v) => typeof v === "string" && !/[\0\r\n]/.test(v)),
      "Exact service argv required",
    );
    absolute(release.argv[0]);
    absolute(release.payload?.root);
    insist(
      Array.isArray(release.payload.files) && release.payload.files.length > 0,
      "Complete runtime payload inventory required",
    );
    const names = new Set();
    for (const entry of release.payload.files) {
      relative(entry.path);
      insist(
        !names.has(entry.path) && isDigest(entry.sha256),
        "Invalid or duplicate payload entry",
      );
      names.add(entry.path);
    }
    for (const executable of [release.argv[0], release.executable.path])
      insist(
        names.has(path.relative(release.payload.root, executable)),
        "Service and server executables must be inside the inventoried payload",
      );
    const entry = release.payload.files.find(
      (file) => path.join(release.payload.root, file.path) === release.executable.path,
    );
    insist(
      entry.sha256 === release.executable.sha256,
      "Executable digest differs from payload inventory",
    );
  }
  if (p.service.fragment)
    insist(
      p.service.dropIns.find((entry) => entry.path === p.service.configPath).sha256 ===
        p.source.config.sha256,
      "Selected drop-in original hash must equal source config artifact",
    );
  const targetLauncher = p.target.argv.some(
    (arg) => arg === "__service-launcher" || /service[-]?Launcher|service-launcher/.test(arg),
  );
  insist(
    !targetLauncher || p.runtimeState,
    "Native launcher target requires a qualified service-state.json transition",
  );
  if (p.runtimeState) {
    for (const side of ["source", "target"]) {
      const artifact = p.runtimeState[side];
      insist(
        artifact && isDigest(artifact.sha256),
        "Both staged runtime-state artifacts are required",
      );
      absolute(artifact.path);
      insist(
        artifact.path !== path.join(p.home, "runtime/service-state.json"),
        "Runtime-state artifacts must be separately staged",
      );
      insist(
        p[side].executable.path === path.join(p.home, "runtime/versions", p[side].version, "t3"),
        "Native metadata transition requires the exact pinned version executable",
      );
    }
  }
  insist(
    p.source.payload.root !== p.target.payload.root &&
      !p.source.payload.root.startsWith(`${p.target.payload.root}/`) &&
      !p.target.payload.root.startsWith(`${p.source.payload.root}/`),
    "Source and target immutable runtime roots must be disjoint",
  );
  insist(
    p.qualification && isDigest(p.qualification.sha256),
    "Compatibility qualification artifact required",
  );
  absolute(p.qualification.path);
  const url = new URL(p.descriptorUrl);
  insist(
    url.protocol === "http:" &&
      ["127.0.0.1", "[::1]"].includes(url.hostname) &&
      !url.username &&
      !url.password &&
      !url.search &&
      !url.hash &&
      url.pathname === "/.well-known/t3/environment",
    "Descriptor must be the fixed unauthenticated loopback endpoint",
  );
  insist(
    Array.isArray(p.stateFiles) && STATE_FILES.every((name) => p.stateFiles.includes(name)),
    "Plan must explicitly include the standard paired state files",
  );
  insist(new Set(p.stateFiles).size === p.stateFiles.length, "Duplicate state file");
  for (const name of p.stateFiles) {
    relative(name);
    insist(!name.startsWith(".server-swap/"), "Control state cannot be a backup input");
  }
  p.timeoutMs ??= 60000;
  insist(
    Number.isInteger(p.timeoutMs) && p.timeoutMs >= 1000 && p.timeoutMs <= 300000,
    "timeoutMs must be between 1000 and 300000",
  );
  absolute(p.stateDir);
  for (const root of [p.home, p.source.payload.root, p.target.payload.root])
    insist(
      p.stateDir !== root &&
        !p.stateDir.startsWith(`${root}/`) &&
        !root.startsWith(`${p.stateDir}/`),
      "stateDir must be outside state home and runtime payloads",
    );
  p.controlDir = p.stateDir;
  p.transactionDir = path.join(p.controlDir, p.id);
  return p;
}
function loadPlan(file) {
  absolute(file);
  regular(file);
  const bytes = fs.readFileSync(file);
  return { ...validatePlan(JSON.parse(bytes)), digest: sha256(bytes), planPath: file };
}
function verifyHost(p) {
  const runtime = readHostProcess();
  insist(
    os.hostname() === p.host &&
      process.getuid?.() === p.uid &&
      runtime.platform === p.platform &&
      runtime.arch === p.arch,
    "Plan does not match this exact host, uid, platform and architecture",
  );
  insist(
    fs.realpathSync(p.home) === p.home && fs.statSync(p.home).uid === p.uid,
    "State home must be canonical and owned by the operator",
  );
  insist(
    fs.readFileSync(path.join(p.home, "userdata/environment-id"), "utf8").trim() ===
      p.environmentId,
    "State environment ID mismatch",
  );
}
function verifyPayload(release) {
  insist(
    fs.realpathSync(release.payload.root) === release.payload.root,
    "Runtime root is not canonical",
  );
  const actual = [];
  function walk(directory, prefix = "") {
    for (const name of fs.readdirSync(directory)) {
      const file = path.join(directory, name);
      const stat = fs.lstatSync(file);
      const local = path.join(prefix, name);
      if (stat.isDirectory()) walk(file, local);
      else {
        insist(
          stat.isFile() && !stat.isSymbolicLink(),
          "Runtime payload contains a symlink or special file",
        );
        actual.push(local);
      }
    }
  }
  walk(release.payload.root);
  insist(
    canonical(actual.sort()) === canonical(release.payload.files.map((file) => file.path).sort()),
    "Runtime inventory differs from staged payload",
  );
  for (const entry of release.payload.files)
    verifyArtifact({ path: path.join(release.payload.root, entry.path), sha256: entry.sha256 });
  verifyArtifact(release.executable);
  insist(
    (regular(release.executable.path).mode & 0o111) !== 0,
    "Runtime executable is not executable",
  );
}
function verifyQualification(p) {
  const q = JSON.parse(checked(p.qualification));
  insist(
    q.schema === "server-swap-qualification/1" && q.bindingDigest === bindingDigest(p),
    "Compatibility qualification is not bound to this exact pair and host state",
  );
  for (const group of ["migrations", "events", "settings", "clients"]) {
    insist(
      q[group]?.status === "passed" &&
        typeof q[group].summary === "string" &&
        q[group].summary.trim() !== "",
      `Missing ${group} compatibility qualification`,
    );
    checked(q[group].evidence);
  }
  return q;
}
function verifyRuntimeState(p) {
  if (!p.runtimeState) return;
  for (const side of ["source", "target"]) {
    const state = JSON.parse(checked(p.runtimeState[side]));
    insist(
      [3, 4].includes(state.protocol) && state.activeVersion === p[side].version,
      "Runtime-state protocol or activeVersion differs",
    );
    if (side === "target")
      insist(state.update === undefined, "Target runtime-state must not contain an update");
    else
      insist(
        state.update === undefined ||
          ["committed", "rolled-back", "failed"].includes(state.update?.status),
        "Source native update is pending or unknown",
      );
  }
}
module.exports = {
  insist,
  sha256,
  canonical,
  digestObject,
  absolute,
  relative,
  regular,
  hashFile,
  verifyArtifact,
  checked,
  atomic,
  writeJson,
  readJson,
  syncDirectory,
  STATE_FILES,
  bindingDigest,
  validatePlan,
  loadPlan,
  verifyHost,
  verifyPayload,
  verifyQualification,
  verifyRuntimeState,
};
