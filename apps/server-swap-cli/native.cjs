"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { execFile } = require("node:child_process");
const { promisify } = require("node:util");
const {
  insist,
  checked,
  canonical,
  regular,
  verifyPayload,
  verifyArtifact,
  syncDirectory,
  readJson,
  hashFile,
} = require("./files.cjs");
const exec = promisify(execFile);

async function command(file, args, allowed = [0]) {
  try {
    const { stdout, stderr } = await exec(file, args, {
      encoding: "utf8",
      timeout: 120000,
      maxBuffer: 4 * 1024 * 1024,
      env: {
        PATH: "/usr/bin:/bin:/usr/sbin:/sbin",
        HOME: process.env.HOME,
        XDG_RUNTIME_DIR: process.env.XDG_RUNTIME_DIR,
        DBUS_SESSION_BUS_ADDRESS: process.env.DBUS_SESSION_BUS_ADDRESS,
      },
    });
    return { stdout, stderr, code: 0 };
  } catch (error) {
    if (Number.isInteger(error.code) && allowed.includes(error.code))
      return { stdout: error.stdout, stderr: error.stderr, code: error.code };
    throw new Error(`Native command failed or outcome is unknown: ${file} ${args[0] || ""}`, {
      cause: error,
    });
  }
}
function systemdWords(text) {
  const words = [];
  let word = "",
    quote = null,
    started = false;
  for (let index = 0; index < text.length; index++) {
    const char = text[index];
    if (char === "\\") {
      const next = text[++index];
      insist(
        next === "\\" || next === '"' || next === "'" || next === " ",
        "Unsupported systemd escape",
      );
      word += next;
      started = true;
    } else if (char === quote) {
      quote = null;
      started = true;
    } else if (!quote && (char === '"' || char === "'")) {
      quote = char;
      started = true;
    } else if (!quote && /\s/.test(char)) {
      if (started) {
        words.push(word);
        word = "";
        started = false;
      }
    } else {
      word += char;
      started = true;
    }
  }
  insist(!quote, "Unclosed systemd quote");
  if (started) words.push(word);
  return words;
}
function systemdFields(contents) {
  const fields = new Map();
  const arrays = new Set([
    "ExecStart",
    "ExecStartPre",
    "ExecStartPost",
    "ExecStop",
    "ExecStopPost",
    "ExecReload",
    "Environment",
    "EnvironmentFile",
    "PassEnvironment",
    "UnsetEnvironment",
    "BindPaths",
    "BindReadOnlyPaths",
  ]);
  let section = "";
  for (const raw of contents.split("\n")) {
    const line = raw.trim();
    if (!line || line.startsWith("#") || line.startsWith(";")) continue;
    if (/^\[.*\]$/.test(line)) {
      section = line;
      continue;
    }
    insist(!line.endsWith("\\"), "Config line continuations are unsupported");
    const match = /^([^=]+)=(.*)$/.exec(line);
    insist(match, "Malformed service configuration");
    if (section !== "[Service]") continue;
    const [, key, value] = match;
    if (arrays.has(key)) fields.set(key, value === "" ? [] : [...(fields.get(key) || []), value]);
    else fields.set(key, [value]);
  }
  return fields;
}
function validateSystemdConfig(contents, p, release) {
  const fields = systemdFields(contents);
  const one = (key) => {
    const values = fields.get(key) || [];
    insist(values.length === 1, `Exactly one ${key} required`);
    return values[0];
  };
  insist(
    canonical(systemdWords(one("ExecStart"))) === canonical(release.argv),
    "Service ExecStart does not match the reviewed runtime argv",
  );
  const env = new Map();
  for (const assignment of (fields.get("Environment") || []).flatMap(systemdWords)) {
    const match = /^([a-zA-Z_][a-zA-Z0-9_]*)=(.*)$/.exec(assignment);
    insist(match, "Unsupported environment assignment");
    env.set(match[1], match[2]);
  }
  insist(env.get("T3CODE_HOME") === p.home, "Service home does not match plan");
  for (const key of [
    "EnvironmentFile",
    "PassEnvironment",
    "UnsetEnvironment",
    "ExecStartPre",
    "ExecStartPost",
    "ExecStop",
    "ExecStopPost",
    "ExecReload",
    "RootDirectory",
    "RootImage",
    "BindPaths",
    "BindReadOnlyPaths",
  ])
    insist(!(fields.get(key) || []).some(Boolean), `Unsupported service override: ${key}`);
  insist(
    ["control-group", "mixed"].includes(fields.has("KillMode") ? one("KillMode") : "control-group"),
    "Service must stop its complete process group",
  );
  let user;
  if (p.service.manager === "systemd-system") {
    user = one("User");
    insist(
      /^\d+$/.test(user) ? Number(user) === p.uid : /^[a-zA-Z_][a-zA-Z0-9_-]*[$]?$/.test(user),
      "System service User must identify the exact operator uid",
    );
  } else insist(!fields.has("User"), "User service must not override User");
  insist(!release.argv.some((arg) => /[%$]/.test(arg)), "Service argv expansion is unsupported");
  return { user };
}
function combinedConfig(p, release) {
  const chunks = [
    p.service.fragment
      ? checked(p.service.fragment).toString("utf8")
      : checked(release.config).toString("utf8"),
  ];
  for (const artifact of p.service.dropIns)
    chunks.push(
      checked(artifact.path === p.service.configPath ? release.config : artifact).toString("utf8"),
    );
  return chunks.join("\n");
}
async function resolveServiceUser(user, uid, run = command) {
  if (/^\d+$/.test(user)) {
    insist(Number(user) === uid, "Loaded service uid differs from operator");
    return;
  }
  insist(/^[a-zA-Z_][a-zA-Z0-9_-]*[$]?$/.test(user), "Invalid named system service User");
  const result = await run("/usr/bin/id", ["-u", user]);
  insist(
    /^\s*\d+\s*$/.test(result.stdout) && Number(result.stdout.trim()) === uid,
    "Named service User resolves to a different uid",
  );
}
function verifyLiveConfigIdentity(p) {
  if (!p.service.fragment) return;
  checked(p.service.fragment);
  let expected = [p.source.config.sha256];
  const file = path.join(p.transactionDir, "journal.json");
  if (fs.existsSync(file)) {
    const journal = readJson(file);
    insist(
      journal.id === p.id && journal.planDigest === p.digest,
      "Config observation journal belongs to another plan",
    );
    if (journal.pending === "install-config")
      expected = [p.source.config.sha256, p.target.config.sha256];
    else if (["configured", "loaded", "target-started", "committed"].includes(journal.phase))
      expected = [p.target.config.sha256];
  }
  insist(
    expected.includes(hashFile(p.service.configPath)),
    "Selected live drop-in digest differs from its journal phase",
  );
}
async function validateConfig(p, release, run = command) {
  if (p.service.manager !== "launchd") {
    const { user } = validateSystemdConfig(combinedConfig(p, release), p, release);
    if (user !== undefined) {
      await resolveServiceUser(user, p.uid, run);
      insist(
        systemdFields(combinedConfig(p, p.source)).get("User")?.[0] === user,
        "Target config must preserve the source service User",
      );
    }
    verifyLiveConfigIdentity(p);
    return;
  }
  checked(release.config);
  insist(p.service.dropIns.length === 0, "launchd does not support systemd drop-ins");
  const result = await run("/usr/bin/plutil", ["-convert", "json", "-o", "-", release.config.path]);
  const plist = JSON.parse(result.stdout);
  insist(
    plist.Label === p.service.name && canonical(plist.ProgramArguments) === canonical(release.argv),
    "launchd config does not match label and runtime argv",
  );
  insist(
    plist.EnvironmentVariables?.T3CODE_HOME === p.home &&
      !plist.Program &&
      !plist.UserName &&
      !plist.RootDirectory &&
      plist.AbandonProcessGroup !== true,
    "Unsupported launchd home, program, user or process group override",
  );
}
const parseProperties = (text) =>
  Object.fromEntries(
    text
      .trim()
      .split("\n")
      .map((line) => {
        const at = line.indexOf("=");
        return [line.slice(0, at), line.slice(at + 1)];
      }),
  );
function serviceAdapter(p, run = command) {
  const s = p.service;
  const args = s.manager === "systemd-user" ? ["--user"] : [];
  const systemctl = (...rest) =>
    s.elevation === "sudo-noninteractive"
      ? run("/usr/bin/sudo", ["-n", "/usr/bin/systemctl", ...args, ...rest])
      : run("/usr/bin/systemctl", [...args, ...rest]);
  const target = `${s.domain}/${s.name}`;
  return {
    async installConfig() {
      insist(
        s.elevation === "sudo-noninteractive",
        "Elevated config installation was not selected",
      );
      const st = regular(s.configPath);
      insist(st.uid === 0 && st.nlink === 1, "Elevated config must be a single root-owned file");
      checked(p.target.config);
      const temporary = `${s.configPath}.server-swap-${p.id}`;
      insist(
        !fs.existsSync(temporary),
        "Config staging effect already exists; reconcile before any retry",
      );
      await run("/usr/bin/sudo", [
        "-n",
        "/usr/bin/install",
        `--mode=${(st.mode & 0o777).toString(8)}`,
        `--owner=${st.uid}`,
        `--group=${st.gid}`,
        "--",
        p.target.config.path,
        temporary,
      ]);
      verifyArtifact({ path: temporary, sha256: p.target.config.sha256 });
      const fd = fs.openSync(temporary, "r");
      try {
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
      await run("/usr/bin/sudo", ["-n", "/bin/mv", "-T", "--", temporary, s.configPath]);
      syncDirectory(path.dirname(s.configPath));
    },
    async observe() {
      if (s.manager !== "launchd") {
        const { stdout } = await systemctl(
          "show",
          s.name,
          "--no-pager",
          "--property=LoadState,ActiveState,SubState,MainPID,ControlPID,ControlGroup,FragmentPath,DropInPaths,NeedDaemonReload,KillMode,ExecStart,Environment,User",
        );
        const props = parseProperties(stdout);
        const dropIns = props.DropInPaths ? systemdWords(props.DropInPaths) : [];
        insist(
          props.LoadState === "loaded" &&
            props.FragmentPath === (s.fragment?.path || s.configPath) &&
            canonical(dropIns) === canonical(s.dropIns.map((entry) => entry.path)),
          "Service fragment differs or has unaccounted drop-ins",
        );
        verifyLiveConfigIdentity(p);
        for (const artifact of s.dropIns) if (artifact.path !== s.configPath) checked(artifact);
        if (s.manager === "systemd-system") await resolveServiceUser(props.User, p.uid, run);
        insist(
          ["mixed", "control-group"].includes(props.KillMode),
          "Loaded service does not stop the complete process group",
        );
        const pid = Number(props.MainPID);
        insist(
          Number.isInteger(pid) && pid >= 0 && /^\d+$/.test(props.ControlPID),
          "Manager process identity unavailable",
        );
        return {
          active: props.ActiveState === "active",
          stopped:
            ["inactive", "failed"].includes(props.ActiveState) &&
            pid === 0 &&
            Number(props.ControlPID) === 0,
          pid,
          cgroup: props.ControlGroup,
          reloadNeeded: props.NeedDaemonReload !== "no",
          properties: props,
        };
      }
      const result = await run("/bin/launchctl", ["print", target], [0, 113]);
      if (result.code === 113 && /Could not find service/.test(result.stderr))
        return { active: false, stopped: true, pid: 0 };
      insist(result.code === 0, "launchd service state is unknown");
      const config = /^\s*path = (.+)$/m.exec(result.stdout)?.[1];
      insist(config === s.configPath, "launchd loaded plist path differs");
      const pid = Number(/^\s*pid = (\d+)$/m.exec(result.stdout)?.[1] || 0);
      return {
        active: pid > 0 && /^\s*state = running$/m.test(result.stdout),
        stopped: false,
        pid,
        detail: result.stdout,
      };
    },
    async detached(observed) {
      if (s.manager !== "launchd") {
        const own = fs.readFileSync("/proc/self/cgroup", "utf8");
        insist(
          observed.cgroup &&
            !own.split("\n").some((line) => {
              const group = line.split(":").slice(2).join(":");
              return group === observed.cgroup || group.startsWith(`${observed.cgroup}/`);
            }),
          "Swap worker must run outside the managed service cgroup",
        );
      } else {
        insist(
          !(await descendsFrom(process.pid, observed.pid)),
          "Swap worker must run outside the managed launchd process tree",
        );
      }
    },
    async stop() {
      if (s.manager === "launchd") await run("/bin/launchctl", ["bootout", target]);
      else await systemctl("stop", s.name);
    },
    async reload() {
      if (s.manager !== "launchd") await systemctl("daemon-reload");
    },
    async start() {
      if (s.manager === "launchd")
        await run("/bin/launchctl", ["bootstrap", s.domain, s.configPath]);
      else await systemctl("start", s.name);
    },
  };
}
async function descendsFrom(pid, ancestor) {
  if (!(ancestor > 0)) return false;
  const seen = new Set();
  for (let count = 0; count < 100 && pid > 1; count++) {
    if (pid === ancestor) return true;
    insist(!seen.has(pid), "Cyclic process ancestry");
    seen.add(pid);
    const { stdout } = await command("/bin/ps", ["-p", String(pid), "-o", "ppid="]);
    insist(/^\s*\d+\s*$/.test(stdout), "Process ancestry unavailable");
    pid = Number(stdout.trim());
  }
  return false;
}
function inventory(p) {
  const userdata = path.join(p.home, "userdata");
  insist(fs.realpathSync(userdata) === userdata, "State directory must be canonical");
  for (const name of fs.readdirSync(userdata)) {
    if (/\.(sqlite\d*|db)(-(wal|shm|journal))?$/.test(name))
      insist(
        p.stateFiles.includes(`userdata/${name}`),
        "Unlisted database or journal file; update paired state inventory and qualification",
      );
  }
  insist(
    ["state.sqlite", "statev2.sqlite"].some((name) => fs.existsSync(path.join(userdata, name))),
    "No recognized state database exists",
  );
  return p.stateFiles.map((name) => {
    const file = path.join(p.home, name);
    try {
      regular(file);
      const st = fs.statSync(file, { bigint: true });
      insist(st.uid === BigInt(p.uid), "State file has a different owner");
      return {
        name,
        path: file,
        device: String(st.dev),
        inode: String(st.ino),
        identity: `${st.dev}:${st.ino}:${st.size}:${st.mtimeNs}:${st.ctimeNs}`,
        mode: Number(st.mode & 0o777n),
      };
    } catch (error) {
      if (error.code === "ENOENT" && name !== "userdata/environment-id")
        return { name, path: file, absent: true };
      throw error;
    }
  });
}
function processBirth(stat) {
  const value = stat.slice(stat.lastIndexOf(")") + 2).split(/\s+/)[19];
  insist(/^\d+$/.test(value), "Process birth identity unavailable");
  return value;
}
function deviceText(value) {
  const dev = BigInt(value);
  return `${(((dev >> 8n) & 0xfffn) | ((dev >> 32n) & 0xfffff000n)).toString(16)}:${((dev & 0xffn) | ((dev >> 12n) & 0xffffff00n)).toString(16)}`;
}
async function writerScan(p, files) {
  const existing = files.filter((file) => !file.absent);
  if (p.platform === "darwin") {
    const result = await command(
      "/usr/sbin/lsof",
      ["-nP", "-a", "-u", String(p.uid), "-F", "pfan", "--", ...existing.map((f) => f.path)],
      [0, 1],
    );
    insist(
      !result.stderr && (result.code === 0 || result.stdout === ""),
      "Writer scan is unavailable",
    );
    let pid, access, descriptor;
    for (const line of result.stdout.split("\n").filter(Boolean)) {
      const value = line.slice(1);
      if (line[0] === "p") {
        insist(/^\d+$/.test(value), "Invalid writer PID");
        pid = Number(value);
        access = undefined;
        descriptor = undefined;
      } else if (line[0] === "f") {
        descriptor = value;
        access = undefined;
      } else if (line[0] === "a") access = value;
      else if (line[0] === "n")
        insist(
          pid && descriptor && existing.some((f) => f.path === value) && access === "r",
          "State writer is active or access is unknown",
        );
      else throw new Error("Unknown writer scan output");
    }
    return;
  }
  const started = Date.now();
  let descriptors = 0;
  const bound = () =>
    insist(
      Date.now() - started < 10000 && descriptors < 100000,
      "Writer scan exceeded proof bounds",
    );
  const names = fs.readdirSync("/proc").filter((name) => /^\d+$/.test(name));
  insist(names.length < 8192, "Process inventory exceeds proof bounds");
  for (const name of names) {
    bound();
    const directory = `/proc/${name}`;
    try {
      if (fs.statSync(directory).uid !== p.uid) continue;
      const birth = processBirth(fs.readFileSync(`${directory}/stat`, "utf8"));
      for (const fd of fs.readdirSync(`${directory}/fd`)) {
        bound();
        descriptors++;
        try {
          const stat = fs.statSync(`${directory}/fd/${fd}`, { bigint: true });
          if (!existing.some((f) => f.device === String(stat.dev) && f.inode === String(stat.ino)))
            continue;
          const flags = /^flags:\s+([0-7]+)$/m.exec(
            fs.readFileSync(`${directory}/fdinfo/${fd}`, "utf8"),
          )?.[1];
          insist(
            flags && (parseInt(flags, 8) & 3) === 0,
            "State writer is active or access flags are unavailable",
          );
        } catch (error) {
          if (error.code !== "ENOENT" || fs.existsSync(`${directory}/fd/${fd}`)) throw error;
        }
      }
      for (const line of fs.readFileSync(`${directory}/maps`, "utf8").split("\n").filter(Boolean)) {
        const fields = line.split(/\s+/);
        insist(fields.length >= 5, "Memory mapping proof unavailable");
        if (fields[1].includes("w") && fields[1].includes("s"))
          insist(
            !existing.some(
              (f) =>
                f.inode === fields[4] &&
                deviceText(f.device) ===
                  fields[3]
                    .split(":")
                    .map((v) => BigInt(`0x${v}`).toString(16))
                    .join(":"),
            ),
            "Writable shared state mapping is active",
          );
      }
      insist(
        processBirth(fs.readFileSync(`${directory}/stat`, "utf8")) === birth,
        "Process identity changed during writer scan",
      );
    } catch (error) {
      if (error.code !== "ENOENT" || fs.existsSync(directory)) throw error;
    }
  }
  for (const name of fs
    .readdirSync("/proc")
    .filter((name) => /^\d+$/.test(name) && !names.includes(name))) {
    try {
      insist(
        fs.statSync(`/proc/${name}`).uid !== p.uid,
        "A process was born during writer proof; retry only before effects",
      );
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  }
  bound();
}
async function quiescence(p, service) {
  insist((await service.observe()).stopped, "Service is not proved stopped");
  const before = inventory(p);
  await writerScan(p, before);
  insist(
    canonical(inventory(p)) === canonical(before),
    "State identity changed during writer proof",
  );
  return before;
}
function proveDarwinImage(output, expected) {
  let record = {};
  const records = [];
  for (const line of output.split("\n").filter(Boolean)) {
    const tag = line[0],
      value = line.slice(1);
    if (tag === "f") {
      records.push(record);
      record = { descriptor: value };
    } else if (["i", "D", "n"].includes(tag)) record[tag] = value;
    else insist(tag === "p", "Unexpected executable image observation");
  }
  records.push(record);
  insist(
    records.some(
      (entry) =>
        entry.descriptor === "txt" &&
        entry.n === expected.path &&
        /^\d+$/.test(entry.i) &&
        /^0x[a-fA-F0-9]+$/.test(entry.D) &&
        BigInt(entry.i) === BigInt(expected.inode) &&
        BigInt(entry.D) === BigInt(expected.device),
    ),
    "Running executable inode/device differs from the staged runtime",
  );
}
async function runtimeIdentity(p, release, service) {
  const observed = await service.observe();
  insist(
    observed.active && observed.pid > 0 && !observed.reloadNeeded,
    "Service is not active with loaded current config",
  );
  const runtimePath = path.join(p.home, "userdata/server-runtime.json");
  regular(runtimePath);
  const runtime = JSON.parse(fs.readFileSync(runtimePath, "utf8"));
  insist(
    Number.isInteger(runtime.pid) && runtime.pid > 1,
    "Live service runtime PID is unavailable",
  );
  insist(
    await descendsFrom(runtime.pid, observed.pid),
    "Runtime PID is not in the exact service process tree",
  );
  let executable;
  if (p.platform === "linux") executable = fs.readlinkSync(`/proc/${runtime.pid}/exe`);
  else
    executable = (
      await command("/bin/ps", ["-p", String(runtime.pid), "-o", "comm="])
    ).stdout.trim();
  insist(
    executable === release.executable.path,
    "Running executable path does not match selected runtime",
  );
  verifyArtifact(release.executable);
  if (p.platform === "linux") {
    const disk = fs.statSync(executable),
      live = fs.statSync(`/proc/${runtime.pid}/exe`);
    insist(
      disk.dev === live.dev && disk.ino === live.ino,
      "Running executable inode differs from staged runtime",
    );
  } else {
    const result = await command("/usr/sbin/lsof", [
      "-nP",
      "-a",
      "-p",
      String(runtime.pid),
      "-d",
      "txt",
      "-F",
      "finD",
    ]);
    insist(!result.stderr, "Executable image observation is unavailable");
    const disk = fs.statSync(executable, { bigint: true });
    proveDarwinImage(result.stdout, { path: executable, inode: disk.ino, device: disk.dev });
  }
  const url = new URL(p.descriptorUrl);
  insist(runtime.port === Number(url.port || 80), "Runtime descriptor port differs from plan");
  const lsof = p.platform === "darwin" ? "/usr/sbin/lsof" : "/usr/bin/lsof";
  const listeners = await command(lsof, [
    "-nP",
    "-a",
    "-p",
    String(runtime.pid),
    "-iTCP:" + runtime.port,
    "-sTCP:LISTEN",
    "-Fp",
  ]);
  insist(
    listeners.stdout.split("\n").includes(`p${runtime.pid}`) && !listeners.stderr,
    "Descriptor listener is not owned by runtime PID",
  );
  const response = await fetch(p.descriptorUrl, {
    redirect: "error",
    signal: AbortSignal.timeout(5000),
  });
  insist(
    response.ok && Number(response.headers.get("content-length") || 0) < 1024 * 1024,
    "Descriptor response failed",
  );
  const descriptor = await response.json();
  insist(
    descriptor.environmentId === p.environmentId && descriptor.serverVersion === release.version,
    "Descriptor environment or exact version mismatch",
  );
  insist(
    (await service.observe()).pid === observed.pid &&
      JSON.parse(fs.readFileSync(runtimePath, "utf8")).pid === runtime.pid,
    "Runtime generation changed during identity proof",
  );
  verifyPayload(release);
  return {
    managerPid: observed.pid,
    runtimePid: runtime.pid,
    executable,
    executableSha256: release.executable.sha256,
    environmentId: descriptor.environmentId,
    serverVersion: descriptor.serverVersion,
  };
}
module.exports = {
  command,
  systemdWords,
  systemdFields,
  combinedConfig,
  resolveServiceUser,
  verifyLiveConfigIdentity,
  validateSystemdConfig,
  validateConfig,
  parseProperties,
  serviceAdapter,
  inventory,
  writerScan,
  quiescence,
  proveDarwinImage,
  runtimeIdentity,
};
