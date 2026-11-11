import { spawn, execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

export const candidate = "https://mirrors.edge.kernel.org/ubuntu/";
export const workspace = "/var/lib/jones-ci-apt";
const pockets = ["noble", "noble-updates", "noble-backports", "noble-security"];
const components = ["main", "restricted", "universe", "multiverse"];
const keyring = "/usr/share/keyrings/ubuntu-archive-keyring.gpg";
const mirrorLines = [
  "http://azure.archive.ubuntu.com/ubuntu/ priority:1",
  "https://archive.ubuntu.com/ubuntu/ priority:2",
  "https://security.ubuntu.com/ubuntu/ priority:3",
];
export const isolation = [
  `Dir::Etc::sourcelist=${workspace}/ubuntu.sources`,
  "Dir::Etc::sourceparts=-",
  `Dir::State::lists=${workspace}/lists`,
  `Dir::Cache::archives=${workspace}/archives`,
  "Acquire::AllowInsecureRepositories=false",
  "Acquire::AllowDowngradeToInsecureRepositories=false",
  "Acquire::AllowWeakRepositories=false",
  "APT::Get::AllowUnauthenticated=false",
].flatMap((option) => ["-o", option]);

const fail = (label, message) => Object.assign(new Error(`${label}: ${message}`), { label });
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const sameSet = (values, expected) =>
  values.length === expected.length && new Set(values).size === values.length &&
  expected.every((value) => values.includes(value));

export function parseArguments(args, route = "") {
  if (!["", "auto", "candidate"].includes(route)) throw fail("config", "invalid apt route");
  const noRecommends = args[0] === "--no-install-recommends";
  const packages = args.slice(noRecommends ? 1 : 0);
  if (!packages.length || packages.some((value) => !/^[a-z0-9][a-z0-9+.-]*$/.test(value))) {
    throw fail("config", "expected package names and optional leading --no-install-recommends");
  }
  return { route: route || "auto", packages, noRecommends };
}

function records(text) {
  return text.trim().split(/\n\s*\n/).filter(Boolean).map((stanza) => {
    const result = {};
    for (const line of stanza.split("\n")) {
      if (/^\s*#/.test(line)) continue;
      const match = /^([\w-]+):[ \t]*(.*?)[ \t]*$/.exec(line);
      if (!match || Object.hasOwn(result, match[1])) throw fail("config", "unsupported deb822 record");
      result[match[1]] = match[2];
    }
    return result;
  }).filter((record) => Object.keys(record).length > 0);
}

export function validateSources({ osRelease, architecture, mirrors, sources }) {
  if (!/^VERSION_CODENAME=(?:noble|"noble")$/m.test(osRelease) || architecture.trim() !== "amd64") {
    throw fail("config", "candidate requires noble amd64");
  }
  const actualMirrors = mirrors.trim().split("\n").map((line) => line.trim().replace(/\s+/g, " "));
  if (JSON.stringify(actualMirrors) !== JSON.stringify(mirrorLines)) throw fail("config", "unexpected mirror list");
  const stanzas = records(sources);
  if (stanzas.length !== 2) throw fail("config", "expected two Ubuntu source stanzas");
  const suites = [];
  for (const stanza of stanzas) {
    if (!sameSet(Object.keys(stanza), ["Types", "URIs", "Suites", "Components", "Signed-By"]) ||
        stanza.Types !== "deb" || stanza.URIs !== "mirror+file:/etc/apt/apt-mirrors.txt" ||
        stanza["Signed-By"] !== keyring || !sameSet(stanza.Components.split(/\s+/), components)) {
      throw fail("config", "unsupported Ubuntu source fields or trust configuration");
    }
    suites.push(...stanza.Suites.split(/\s+/));
  }
  if (!sameSet(suites, pockets)) throw fail("config", "unexpected Ubuntu suites");
  return sources.replace(/^URIs:.*$/gm, `URIs: ${candidate}`);
}

export function validateTargets(text) {
  const targets = records(text);
  const coverage = [];
  if (!targets.length) throw fail("config", "no isolated index targets");
  for (const target of targets) {
    if (target["Repo-URI"] !== candidate || !target.Filename ||
        !path.posix.resolve(target.Filename).startsWith(`${workspace}/lists/`)) {
      throw fail("config", "index target escaped candidate or isolated lists");
    }
    if (target.Identifier !== "Packages") continue;
    if (!["amd64", "all"].includes(target.Architecture)) throw fail("config", "foreign Packages architecture");
    const match = target.URI?.startsWith(`${candidate}dists/`) &&
      /^([^/]+)\/([^/]+)\/binary-(amd64|all)\/Packages$/.exec(target.URI.slice(`${candidate}dists/`.length));
    if (!match || match[2] !== target.Component || match[3] !== target.Architecture ||
        !pockets.includes(match[1]) || !components.includes(match[2])) {
      throw fail("config", "unexpected Packages target");
    }
    if (target.Architecture === "amd64") coverage.push(`${match[1]}/${match[2]}`);
  }
  if (!sameSet(coverage, pockets.flatMap((suite) => components.map((component) => `${suite}/${component}`)))) {
    throw fail("config", "incomplete isolated pocket/component coverage");
  }
}

export function classifyFailure(result) {
  if (result.cancelled) return "cancelled";
  if (!result.quiescent) return "cleanup-unresolved";
  const text = `${result.stdout}\n${result.stderr}`;
  if (/GPG error|NO_PUBKEY|BADSIG|EXPKEYSIG|REVKEYSIG|not signed|[Hh]ash [Ss]um mismatch|[Ss]ize mismatch|File has unexpected size|[Cc]learsigned file.*(?:invalid|not valid|isn.t valid)|invalid clearsign/i.test(text)) return "signature";
  if (/Release file .* (?:expired|not valid yet)|Release file .*not yet valid/i.test(text)) return "freshness";
  if (/Malformed|Type .* is not known|Conflicting values|could not be read|option .*not (?:understood|recognized)|Command line option|Could not (?:get|open) lock|Unable to (?:acquire|lock)|Permission denied|are you root|sudo:|parse (?:error|failure)/i.test(text)) return "config";
  const lines = text.split("\n");
  const diagnostics = [];
  for (let i = 0; i < lines.length; i++) {
    if (/^[EW]:/.test(lines[i])) diagnostics.push(lines[i]);
    if (/^Err:/.test(lines[i])) {
      diagnostics.push(/^\s+\S/.test(lines[i + 1] ?? "") ? lines[++i].trim() : lines[i]);
    }
  }
  const transport = /(?:Temporary failure resolving|Could not resolve|Could not connect|Unable to connect|Connection (?:timed out|timeout|reset|failed)|[Cc]onnection refused|Network is unreachable|[Tt]imeout was reached|[Cc]onnecting to .*timed out|\b(?:502 Bad Gateway|503 Service Unavailable|504 Gateway Time-out|504 Gateway Timeout)\b)/;
  const summary = /^[EW]: (?:Some index files failed to download\. They have been ignored, or old ones used instead\.|Unable to fetch some archives, maybe run apt-get update or try with --fix-missing\??)$/;
  if (diagnostics.every((line) => transport.test(line) || summary.test(line)) &&
      (result.timedOut || (result.code !== 0 && diagnostics.some((line) => transport.test(line))))) return "network";
  return "unclassified";
}

export function parseProcStat(raw) {
  const fields = raw.slice(raw.lastIndexOf(")") + 2).trim().split(/\s+/);
  if (!/^\d+$/.test(fields[2] ?? "") || !/^\d+$/.test(fields[3] ?? "")) throw fail("cleanup-unresolved", "invalid proc identity");
  return { state: fields[0], group: Number(fields[2]), session: Number(fields[3]) };
}

export async function groupMembers(group) {
  const members = [];
  for (const pid of await fs.readdir("/proc")) {
    if (!/^\d+$/.test(pid)) continue;
    let stat;
    try { stat = parseProcStat(await fs.readFile(`/proc/${pid}/stat`, "utf8")); }
    catch (error) { if (error.code === "ENOENT" || error.code === "ESRCH") continue; throw error; }
    if (stat.state !== "Z" && (stat.group === group || stat.session === group)) members.push(Number(pid));
  }
  return members;
}

// The shell stays alive at the gate until /proc confirms its group and session.
// Descendants that create a new session, and an uncatchable helper kill, escape this guard.
const gate = 'printf "jones-apt-group %s\\n" "$$"; IFS= read -r ack && [ "$ack" = jones-apt-go ] || exit 125; LC_ALL=C; export LC_ALL; umask 022; exec "$@"';
export function createCommandRunner({
  launch = spawn,
  readStat = async (group) => parseProcStat(await fs.readFile(`/proc/${group}/stat`, "utf8")),
  members = groupMembers,
  privilege = ["/usr/bin/sudo", "-n"],
  report = console.log,
  now = () => performance.now(),
  pause = sleep,
  markerMs = 5_000,
  graceMs = 10_000,
  pollMs = 100,
} = {}) {
  const signalGroup = async (group, signal) => {
    report(`apt group ${group}: send ${signal}`);
    await new Promise((resolve, reject) => {
      const argv = ["/usr/bin/kill", "-s", signal, "--", `-${group}`];
      const child = launch(privilege[0] ?? argv[0], privilege.length ? [...privilege.slice(1), ...argv] : argv.slice(1), { stdio: ["ignore", "ignore", "pipe"], env: { PATH: "/usr/sbin:/usr/bin:/sbin:/bin", LC_ALL: "C" } });
      let stderr = "";
      child.stderr.on("data", (data) => { stderr += data; });
      child.once("error", reject);
      child.once("exit", (code) => code === 0 ? resolve() : reject(fail("cleanup-unresolved", `group signal failed: ${stderr}`)));
    });
  };
  const waitQuiet = async (group, ms, stopSignal) => {
    const end = now() + ms;
    do {
      if (!(await members(group)).length) { report(`apt group ${group}: quiescence proven`); return true; }
      if (stopSignal?.aborted || now() >= end) return false;
      await pause(pollMs);
    } while (true);
  };
  return async (argv, { timeoutMs = 60_000, input = "", signal } = {}) => {
    if (signal?.aborted) throw fail("cancelled", "no command started");
    const command = ["/usr/bin/setsid", "--wait", "/bin/sh", "-c", gate, "jones-apt", ...argv];
    report(`apt command: ${argv.join(" ")}`);
    const child = launch(privilege[0] ?? command[0], privilege.length ? [...privilege.slice(1), ...command] : command.slice(1), { stdio: ["pipe", "pipe", "pipe"], env: { PATH: "/usr/sbin:/usr/bin:/sbin:/bin", LC_ALL: "C" } });
    let stdout = "", stderr = "", prefix = "", marker, exited = false, closed = false, code, spawnError;
    let wake;
    const changed = () => { wake?.(); wake = undefined; };
    const waitChange = (ms) => new Promise((resolve) => {
      const timer = setTimeout(() => { wake = undefined; resolve(); }, ms);
      wake = () => { clearTimeout(timer); resolve(); };
    });
    child.stdin.on("error", () => {});
    child.stdout.on("data", (data) => {
      const text = data.toString();
      if (marker === undefined) {
        prefix += text;
        const newline = prefix.indexOf("\n");
        if (newline < 0) { changed(); return; }
        const match = /^jones-apt-group ([1-9]\d*)$/.exec(prefix.slice(0, newline));
        marker = match ? Number(match[1]) : null;
        const rest = prefix.slice(newline + 1);
        stdout += rest;
        if (rest) report(rest.trimEnd());
      } else { stdout += text; report(text.trimEnd()); }
      changed();
    });
    child.stderr.on("data", (data) => { stderr += data; report(data.toString().trimEnd()); changed(); });
    child.once("error", (error) => { spawnError = error; exited = true; changed(); });
    child.once("exit", (value) => { code = value; exited = true; changed(); });
    child.once("close", () => { closed = true; changed(); });
    const cancel = () => changed();
    signal?.addEventListener("abort", cancel);
    const start = now();
    let group;
    try {
      while (marker === undefined && !exited && now() - start < markerMs) await waitChange(Math.max(1, markerMs - (now() - start)));
      if (!marker) {
        child.stdin.destroy();
        throw fail(exited ? "config" : "cleanup-unresolved", `missing group marker${spawnError ? `: ${spawnError.message}` : ""}; ${stderr}`);
      }
      if (!Number.isSafeInteger(marker) || marker <= 1) throw fail("cleanup-unresolved", "invalid group marker");
      const identity = await readStat(marker);
      if (identity.group !== marker || identity.session !== marker) throw fail("cleanup-unresolved", "group/session identity mismatch");
      group = marker;
      report(`apt group ${group}: captured and confirmed`);
      if (!signal?.aborted) child.stdin.end(`jones-apt-go\n${input}`);
      let timedOut = false;
      while (!exited && !signal?.aborted) {
        const remaining = timeoutMs - (now() - start);
        if (remaining <= 0) { timedOut = true; report(`apt group ${group}: deadline hit`); break; }
        await waitChange(remaining);
      }
      const terminateGroup = async () => {
        if (!(await members(group)).length) {
          report(`apt group ${group}: quiescence proven`);
          return true;
        }
        // The monitored leader may have exited; the captured group still owns its survivors.
        try { await signalGroup(group, "TERM"); }
        catch (error) { if ((await members(group)).length) throw error; }
        if (await waitQuiet(group, graceMs)) return true;
        try { await signalGroup(group, "KILL"); }
        catch (error) { if ((await members(group)).length) throw error; }
        return waitQuiet(group, graceMs);
      };
      let quiescent;
      if (timedOut || signal?.aborted || code !== 0) quiescent = await terminateGroup();
      else {
        quiescent = await waitQuiet(group, graceMs, signal);
        if (!quiescent && signal?.aborted) quiescent = await terminateGroup();
      }
      if (!quiescent && (timedOut || signal?.aborted || code !== 0)) throw fail("cleanup-unresolved", `group ${group} still has members`);
      if (quiescent) {
        // Exit alone does not drain diagnostics; classify only after the pipes close.
        const drainEnd = now() + markerMs;
        while (!closed && now() < drainEnd) await waitChange(Math.max(1, drainEnd - now()));
        if (!closed) throw fail("cleanup-unresolved", `group ${group} quiet but command pipes did not close`);
      } else {
        report(`apt group ${group}: successful leader left members; same route only, retain workspace`);
        child.unref?.();
        child.stdout.unref?.();
        child.stderr.unref?.();
      }
      return { code, stdout, stderr, timedOut, cancelled: signal?.aborted === true, quiescent };
    } catch (error) {
      child.stdin.destroy();
      // Without confirmed identity no numeric signal is safe. Preserve unknown effects.
      child.unref?.();
      child.stdout.unref?.();
      child.stderr.unref?.();
      throw error.label ? error : fail("cleanup-unresolved", error.message);
    } finally { signal?.removeEventListener("abort", cancel); }
  };
}

export async function setupApt({
  args,
  route = "",
  run = createCommandRunner(),
  readFile = (file) => fs.readFile(file, "utf8"),
  now = () => Date.now(),
  signal,
  report = console.log,
}) {
  const options = parseArguments(args, route);
  const installArgs = ["install", "-y", ...(options.noRecommends ? ["--no-install-recommends"] : []), ...options.packages];
  let created = false, allQuiet = true, cleanupSafe = true, fallback = false;
  const command = async (argv, settings = {}) => {
    if (signal?.aborted) throw fail("cancelled", "no further command started");
    let result;
    try { result = await run(argv, { ...settings, signal }); }
    catch (error) { if (error.label === "cleanup-unresolved") cleanupSafe = false; throw error; }
    allQuiet &&= result.quiescent;
    if (result.cancelled) throw fail("cancelled", "apt setup interrupted");
    return result;
  };
  const requireSuccess = (result) => {
    if (result.cancelled || result.code !== 0 || result.timedOut) throw fail(classifyFailure(result), "command failed");
    return result;
  };
  const apt = async (extra, action, timeoutMs) => {
    const result = await command(["/usr/bin/apt-get", ...extra, ...action], { timeoutMs });
    const label = classifyFailure({ ...result, quiescent: true });
    if (["config", "signature", "freshness"].includes(label)) throw fail(label, "terminal apt diagnostic");
    return result;
  };
  try {
    if (options.route === "auto") {
      report("apt route: normal");
      for (const [action, deadline] of [[ ["update", "--error-on=any"], 60_000 ], [ [...installArgs, "--download-only"], 60_000 ]]) {
        const result = await apt([], action, deadline);
        if (result.code === 0 && !result.timedOut) continue;
        const label = classifyFailure(result);
        if (label !== "network" || !allQuiet) throw fail(label === "network" ? "cleanup-unresolved" : label, "normal route stopped");
        fallback = true;
        report("apt normal transport failure; quiescence proven before candidate");
        break;
      }
      // Both network steps succeeded only when the last command downloaded packages.
      if (!fallback) {
        requireSuccess(await apt([], [...installArgs, "--no-download", "-o", "DPkg::Lock::Timeout=30"], 120_000));
        return;
      }
    }
    report("apt route: candidate");
    const configSuccess = (result) => {
      if (result.code !== 0 || result.timedOut) throw fail("config", "candidate preflight command failed");
      return result;
    };
    const architecture = configSuccess(await command(["/usr/bin/dpkg", "--print-architecture"])).stdout;
    let generated, sourceConfig;
    try {
      sourceConfig = {
        architecture,
        osRelease: await readFile("/etc/os-release"),
        mirrors: await readFile("/etc/apt/apt-mirrors.txt"),
        sources: await readFile("/etc/apt/sources.list.d/ubuntu.sources"),
      };
      generated = validateSources(sourceConfig);
    } catch (error) { throw fail("config", error.message); }
    const verified = configSuccess(await command(["/usr/bin/dpkg", "--verify", "ubuntu-keyring"]));
    if (verified.stdout.trim() || verified.stderr.trim()) throw fail("config", "ubuntu-keyring verification is not clean");
    report(`apt preflight accepted noble amd64, signed suites/components and mirrors\nMirrors:\n${sourceConfig.mirrors}Ubuntu sources:\n${sourceConfig.sources}`);
    const made = await command(["/usr/bin/mkdir", "--", workspace]);
    if (made.code !== 0 || made.timedOut) throw fail("config", "workspace mkdir failed; preexisting or unknown workspace retained");
    created = true;
    requireSuccess(await command(["/usr/bin/mkdir", "--", `${workspace}/lists`, `${workspace}/archives`]));
    requireSuccess(await command(["/usr/bin/tee", "--", `${workspace}/ubuntu.sources`], { input: generated }));
    const targets = configSuccess(await apt(isolation, ["indextargets", "--no-release-info"], 60_000));
    report(`apt isolated index targets\n${targets.stdout}`);
    validateTargets(targets.stdout);
    requireSuccess(await apt(isolation, ["update", "--error-on=any"], 120_000));
    for (const pocket of pockets) {
      let release;
      try { release = await readFile(`${workspace}/lists/mirrors.edge.kernel.org_ubuntu_dists_${pocket}_InRelease`); }
      catch { throw fail("freshness", `missing acquired InRelease: ${pocket}`); }
      if (!["noble-updates", "noble-security"].includes(pocket)) continue;
      const dates = [...release.matchAll(/^Date:\s*(.+)$/gm)];
      const date = dates.length === 1 ? Date.parse(dates[0][1]) : NaN;
      const age = now() - date;
      if (!Number.isFinite(age) || age < 0 || age > 7 * 86_400_000) throw fail("freshness", `invalid or stale acquired Date: ${pocket}`);
      report(`apt accepted ${pocket} Date: ${dates[0][1]}`);
    }
    requireSuccess(await apt(isolation, [...installArgs, "--download-only"], 60_000));
    requireSuccess(await apt(isolation, [...installArgs, "--no-download", "-o", "DPkg::Lock::Timeout=30"], 120_000));
  } finally {
    if (created && cleanupSafe && allQuiet) {
      const removed = await run(["/usr/bin/rm", "-r", "--", workspace], { timeoutMs: 60_000, signal: signal?.aborted ? undefined : signal });
      requireSuccess(removed);
      if (!removed.quiescent) throw fail("cleanup-unresolved", "workspace removal group still live");
    } else if (created) report(`apt retain owned workspace ${workspace}: process quiescence unproved`);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const controller = new AbortController();
  const cancel = () => controller.abort();
  process.on("SIGINT", cancel);
  process.on("SIGTERM", cancel);
  try {
    if (process.platform !== "linux" || process.env.GITHUB_ACTIONS !== "true") throw fail("config", "Ubuntu CI helper requires a disposable Linux Actions runner");
    const checkout = execFileSync("/usr/bin/git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim();
    console.log(`apt setup checkout SHA: ${checkout}`);
    if (process.env.GITHUB_SHA && checkout !== process.env.GITHUB_SHA) throw fail("config", "checkout differs from GITHUB_SHA");
    await setupApt({ args: process.argv.slice(2), route: process.env.JONES_CI_APT_ROUTE ?? "", signal: controller.signal });
    if (controller.signal.aborted) throw fail("cancelled", "apt setup interrupted");
  } catch (error) { console.error(error.message); process.exitCode = 1; }
  finally { process.off("SIGINT", cancel); process.off("SIGTERM", cancel); }
}
