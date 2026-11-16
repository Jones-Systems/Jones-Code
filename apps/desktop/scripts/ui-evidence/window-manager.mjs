import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeUtil from "node:util";
import * as NodeTimersPromises from "node:timers/promises";
const exec = NodeUtil.promisify(NodeChildProcess.execFile);
const digest = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const target = "/harness/support/window-manager";
const maxBytes = 200 * 1024 * 1024;
const contained = (base, file) => file.startsWith(base + NodePath.sep);
async function regular(file, base) {
  const resolved = await NodeFSP.realpath(file);
  if (!contained(base, resolved) || !(await NodeFSP.stat(resolved)).isFile())
    throw new Error("Window manager input escaped its declared prefix");
  const bytes = await NodeFSP.readFile(resolved);
  return {
    source: resolved,
    sha256: digest(bytes),
    size: bytes.length,
  };
}

// The caller supplies an already provenance-checked, task-owned Debian extraction.
// Archive hashes are rechecked; this does not replace APT repository authentication.
export async function prepareWindowManager(receiptPath) {
  if (receiptPath === undefined) return null;
  if (!NodePath.isAbsolute(receiptPath)) throw new Error("Window manager receipt must be absolute");
  const receiptBytes = await NodeFSP.readFile(receiptPath);
  const receipt = JSON.parse(receiptBytes);
  if (
    receipt.schema !== "jones-code.wm-prerequisite-result/v1" ||
    receipt.status !== "prepared" ||
    receipt.host?.architecture !== "amd64" ||
    receipt.runtime?.version !== "Openbox 3.6.1" ||
    !Array.isArray(receipt.packages?.items) ||
    receipt.packages.items.length < 1 ||
    receipt.packages.items.length > 64
  )
    throw new Error("Unsupported prepared window manager receipt");
  const base = await NodeFSP.realpath(receipt.write_cone);
  if ((await NodeFSP.stat(base)).uid !== process.getuid())
    throw new Error("Window manager extraction must belong to this user");
  const prefix = await NodeFSP.realpath(NodePath.join(base, "extract/root"));
  if (!contained(base, prefix)) throw new Error("Window manager prefix escaped its receipt");
  let archiveBytes = 0;
  for (const item of receipt.packages.items) {
    const archive = await regular(item.local_file, base);
    archiveBytes += archive.size;
    if (
      item.verified !== true ||
      archive.sha256 !== item.sha256 ||
      archive.size !== item.size_bytes ||
      archiveBytes > maxBytes
    )
      throw new Error("Window manager archive does not match its preparation receipt");
  }
  const files = [];
  const add = async (source, destination) => {
    const file = await regular(source, prefix);
    files.push({
      ...file,
      destination,
    });
    if (files.reduce((total, entry) => total + entry.size, 0) > maxBytes)
      throw new Error("Window manager staged closure exceeds size bound");
  };
  const executable = NodePath.join(prefix, "usr/bin/openbox");
  if ((await NodeFSP.realpath(receipt.runtime.executable)) !== (await NodeFSP.realpath(executable)))
    throw new Error("Window manager executable mismatch");
  await add(executable, "bin/openbox");
  if (!((await NodeFSP.stat(executable)).mode & 0o111))
    throw new Error("Prepared window manager is not executable");
  const libraries = receipt.runtime.staged_libraries;
  if (!Array.isArray(libraries) || libraries.length < 1 || libraries.length > 64)
    throw new Error("Invalid window manager library closure");
  for (const name of libraries) {
    if (!/^lib[A-Za-z0-9._+-]+\.so(?:\.\d+)*$/.test(name))
      throw new Error("Invalid window manager library name");
    let source;
    for (const relative of ["usr/lib/x86_64-linux-gnu", "usr/lib"]) {
      const candidate = NodePath.join(prefix, relative, name);
      try {
        await NodeFSP.stat(candidate);
        source = candidate;
        break;
      } catch (error) {
        if (error.code !== "ENOENT") throw error;
      }
    }
    if (!source) throw new Error("Prepared window manager library is absent");
    await add(source, "lib/" + name);
  }
  // One official theme is enough; no sessions, autostart scripts or menu commands.
  const themeRoot = NodePath.join(prefix, "usr/share/themes/Clearlooks/openbox-3");
  async function theme(directory) {
    for (const entry of await NodeFSP.readdir(directory, {
      withFileTypes: true,
    })) {
      const file = NodePath.join(directory, entry.name);
      if (entry.isDirectory()) await theme(file);
      else if (entry.isFile())
        await add(file, "share/themes/Clearlooks/openbox-3/" + NodePath.relative(themeRoot, file));
      else throw new Error("Special file in window manager theme");
    }
  }
  await theme(themeRoot);
  return {
    schema: "jones-private-window-manager-stage/v1",
    receipt: {
      path: receiptPath,
      sha256: digest(receiptBytes),
    },
    files,
    configuration: {
      managedWindowManager: {
        schema: "jones-private-window-manager/v1",
        executable: target + "/bin/openbox",
        sha256: files[0].sha256,
        libraryPath: target + "/lib",
        dataPath: target + "/share:/usr/share",
        configPath: target + "/rc.xml",
        receiptSha256: digest(receiptBytes),
      },
    },
  };
}
export async function stageWindowManager(prepared, stage) {
  if (prepared === null) return;
  const support = NodePath.join(stage, "harness/support");
  await NodeFSP.mkdir(support, {
    recursive: true,
    mode: 0o700,
  });
  if ((await NodeFSP.realpath(support)) !== support)
    throw new Error("Window manager stage must be a canonical owned directory");
  const root = NodePath.join(support, "window-manager");
  await NodeFSP.mkdir(root, {
    mode: 0o700,
  });
  for (const entry of prepared.files) {
    const bytes = await NodeFSP.readFile(entry.source);
    if (digest(bytes) !== entry.sha256)
      throw new Error("Window manager input changed before staging");
    const destination = NodePath.join(root, entry.destination);
    if (!contained(root, destination)) throw new Error("Window manager stage escaped");
    await NodeFSP.mkdir(NodePath.dirname(destination), {
      recursive: true,
      mode: 0o700,
    });
    await NodeFSP.writeFile(destination, bytes, {
      flag: "wx",
      mode: entry.destination === "bin/openbox" ? 0o700 : 0o600,
    });
  }
  await NodeFSP.writeFile(
    NodePath.join(root, "rc.xml"),
    '<?xml version="1.0"?>\n<openbox_config xmlns="http://openbox.org/3.4/rc">\n' +
      "<theme><name>Clearlooks</name></theme>\n" +
      "<focus><focusNew>yes</focusNew><followMouse>no</followMouse></focus>\n" +
      "<desktops><number>1</number></desktops>\n</openbox_config>\n",
    {
      flag: "wx",
      mode: 0o600,
    },
  );
}

// Openbox 3.6.1 does not publish _NET_WM_PID on its supporting window.
// The caller creates a fresh private Xvfb and starts exactly this owned manager.
export function readWindowManagerReady(rootText, peerText, childPid) {
  const id = rootText.match(/window id # (0x[0-9a-f]+)/i)?.[1];
  const self = peerText.match(/_NET_SUPPORTING_WM_CHECK.*window id # (0x[0-9a-f]+)/i)?.[1];
  const pid = peerText.match(/_NET_WM_PID.*=\s*(\d+)/)?.[1];
  if (
    !id ||
    id === "0x0" ||
    self !== id ||
    !peerText.includes('"Openbox"') ||
    (pid !== undefined && Number(pid) !== childPid)
  )
    return null;
  return {
    supportingWindow: id,
    pidEvidence:
      pid === undefined
        ? "sole task-owned manager on fresh private display"
        : "EWMH PID matches owned process",
  };
}
export async function startWindowManager(configuration, display, register) {
  if (!configuration) return null;
  if (
    configuration.schema !== "jones-private-window-manager/v1" ||
    configuration.executable !== target + "/bin/openbox" ||
    configuration.libraryPath !== target + "/lib" ||
    configuration.dataPath !== target + "/share:/usr/share" ||
    configuration.configPath !== target + "/rc.xml" ||
    !/^:\d+$/.test(display)
  )
    throw new Error("Invalid private window manager configuration");
  if (digest(await NodeFSP.readFile(configuration.executable)) !== configuration.sha256)
    throw new Error("Staged window manager executable changed");
  const env = {
    ...process.env,
    DISPLAY: display,
    LD_LIBRARY_PATH: configuration.libraryPath,
    XDG_DATA_DIRS: configuration.dataPath,
  };
  const child = NodeChildProcess.spawn(
    configuration.executable,
    ["--sm-disable", "--config-file", configuration.configPath],
    {
      stdio: ["ignore", "ignore", "pipe"],
      env,
    },
  );
  register(child); // Capture ownership before any readiness wait or failure.
  let failure = null;
  child.once("error", (error) => {
    failure = error;
  });
  const stderr = [];
  child.stderr.on("data", (bytes) => {
    if (stderr.join("").length < 8192) stderr.push(bytes.toString().slice(0, 8192));
  });
  let lastProbe = null;
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    if (failure) throw failure;
    if (child.exitCode !== null || child.signalCode !== null)
      throw new Error("Private window manager exited before EWMH readiness");
    const root = await exec("/usr/bin/xprop", ["-root", "_NET_SUPPORTING_WM_CHECK"], {
      env: {
        ...process.env,
        DISPLAY: display,
      },
      timeout: 2000,
    }).catch(() => null);
    const id = root?.stdout.match(/window id # (0x[0-9a-f]+)/i)?.[1];
    if (id && id !== "0x0") {
      const peer = await exec(
        "/usr/bin/xprop",
        ["-id", id, "_NET_SUPPORTING_WM_CHECK", "_NET_WM_NAME", "_NET_WM_PID"],
        {
          env: {
            ...process.env,
            DISPLAY: display,
          },
          timeout: 2000,
        },
      );
      lastProbe = {
        root: root.stdout,
        peer: peer.stdout,
        childPid: child.pid,
      };
      const ready = readWindowManagerReady(root.stdout, peer.stdout, child.pid);
      if (ready)
        return {
          kind: "Openbox",
          childPid: child.pid,
          display,
          ...ready,
          ewmh: peer.stdout.trim(),
          receiptSha256: configuration.receiptSha256,
          executableSha256: configuration.sha256,
        };
    }
    await NodeTimersPromises.setTimeout(50);
  }
  throw new Error(
    "Private window manager EWMH readiness timed out: " +
      stderr.join("").slice(0, 1000) +
      JSON.stringify(lastProbe),
  );
}
