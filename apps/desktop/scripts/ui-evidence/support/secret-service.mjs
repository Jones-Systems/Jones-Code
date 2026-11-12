import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";

const nativeRoot = "/harness/support/native";
const digest = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");

async function identity(pid) {
  try {
    const text = await NodeFSP.readFile(`/proc/${pid}/stat`, "utf8");
    return text.slice(text.lastIndexOf(")") + 2).split(" ")[19];
  } catch (error) {
    if (["ENOENT", "ESRCH"].includes(error.code)) return null;
    throw error;
  }
}

export async function verifySecretServiceInputs(config, fs = NodeFSP) {
  if (!config || config.schema !== "jones-private-secret-service/v1")
    throw new Error("Invalid managed Secret Service configuration");
  if (!Array.isArray(config.libraries) || !Array.isArray(config.libraryDirectories))
    throw new Error("Explicit native library closure required");
  const files = [config.daemon, config.bus, config.client, ...config.libraries];
  if (config.bus?.path !== "/usr/bin/dbus-daemon" || config.client?.path !== "/usr/bin/gdbus")
    throw new Error("Explicit public bus tools required");
  for (const entry of [config.daemon, ...config.libraries]) {
    if (!entry?.path?.startsWith(`${nativeRoot}/`) || NodePath.normalize(entry.path) !== entry.path)
      throw new Error("Native input escaped its staged closure");
  }
  for (const entry of files) {
    if (!/^[a-f0-9]{64}$/.test(entry?.sha256 ?? "")) throw new Error("Native digest required");
    if (!(await fs.lstat(entry.path)).isFile() || (await fs.realpath(entry.path)) !== entry.path)
      throw new Error("Native input must be a canonical regular file");
    if (digest(await fs.readFile(entry.path)) !== entry.sha256)
      throw new Error("Native input digest mismatch");
  }
  for (const directory of config.libraryDirectories) {
    if (
      !directory.startsWith(`${nativeRoot}/`) ||
      NodePath.normalize(directory) !== directory ||
      (await fs.realpath(directory)) !== directory ||
      !(await fs.lstat(directory)).isDirectory()
    )
      throw new Error("Invalid staged native library directory");
    const entries = await fs.readdir(directory);
    for (const name of entries) {
      if (!config.libraries.some((entry) => entry.path === NodePath.join(directory, name)))
        throw new Error("Unverified file in native library directory");
    }
  }
  return files.map(({ path, sha256 }) => ({ path, sha256 }));
}

export function privateBusConfiguration(address, uid) {
  if (
    !/^unix:path=\/scratch\/tmp\/secret-service-[a-zA-Z0-9]+\/bus$/.test(address) ||
    !Number.isSafeInteger(uid) ||
    uid < 0
  )
    throw new Error("Invalid private bus identity");
  return `<busconfig><type>session</type><listen>${address}</listen><auth>EXTERNAL</auth>
<policy context="default"><deny user="*"/><allow user="${uid}"/>
<allow own="*"/><allow send_destination="*"/><allow receive_sender="*"/></policy>
</busconfig>`;
}

export function assertSecretStorage(value) {
  if (value?.backend !== "gnome_libsecret" || value?.available !== true)
    throw new Error("Managed Secret Service requires available gnome_libsecret; fallback refused");
  return value;
}

export function createManagedSecretService(
  config,
  {
    fs = NodeFSP,
    spawn = NodeChildProcess.spawn,
    processIdentity = identity,
    env = process.env,
    uid = process.getuid?.(),
    signals = process,
    verify = verifySecretServiceInputs,
    pause = NodeTimersPromises.setTimeout,
    now = Date.now,
  } = {},
) {
  const handles = [];
  const state = { status: "created", services: [], generations: [], cancelled: null };
  let root,
    bus,
    keyring,
    closed = false,
    closing,
    ready = false;
  let rejectCancellation;
  const cancelled = new Promise((_, reject) => {
    rejectCancellation = reject;
  });
  cancelled.catch(() => {});
  const cancel = (signal) => {
    state.cancelled ??= signal;
    rejectCancellation(new Error(`Managed lifecycle cancelled: ${signal}`));
  };
  const interrupt = () => cancel("SIGINT"),
    terminate = () => cancel("SIGTERM");
  signals.on("SIGINT", interrupt);
  signals.on("SIGTERM", terminate);
  const check = () => {
    if (closed || state.cancelled) throw new Error("Managed lifecycle is closed or cancelled");
    for (const handle of [bus, keyring].filter(Boolean))
      if (handle.error || handle.exited) throw new Error("Owned Secret Service process exited");
  };
  const wait = (promise) => Promise.race([promise, cancelled]);
  function own(command, args, childEnv, input) {
    check();
    const child = spawn(command, args, { env: childEnv, stdio: ["pipe", "pipe", "pipe"] });
    const record = {
      pid: child.pid ?? null,
      startTicks: null,
      exited: false,
      closed: false,
      exitCode: null,
      signal: null,
      error: false,
      stdoutBytes: 0,
      stderrBytes: 0,
    };
    const handle = { child, record, exited: false, error: false };
    handles.push(handle);
    handle.done = new Promise((resolve) =>
      child.once("close", () => {
        handle.exited = true;
        record.exited = true;
        record.closed = true;
        resolve();
      }),
    );
    child.on("error", () => {
      handle.error = true;
      record.error = true;
    });
    child.once("exit", (code, signal) => {
      handle.exited = true;
      record.exited = true;
      record.exitCode = code;
      record.signal = signal;
    });
    child.stdout.on("data", (bytes) => {
      record.stdoutBytes += bytes.length;
    });
    child.stderr.on("data", (bytes) => {
      record.stderrBytes += bytes.length;
    });
    child.stdin.on("error", () => {});
    child.stdin.end(input);
    return handle;
  }
  async function captureIdentity(handle) {
    handle.record.startTicks = await processIdentity(handle.child.pid);
    if (!handle.record.startTicks) throw new Error("Owned process identity unavailable");
  }
  async function stop(handle) {
    if (handle.record.closed) return;
    if (!handle.exited) handle.child.kill("SIGTERM");
    let timer;
    const escalated = new Promise((resolve) => {
      timer = setTimeout(() => {
        if (!handle.exited) handle.child.kill("SIGKILL");
        resolve();
      }, 1000);
    });
    await Promise.race([handle.done, escalated]);
    clearTimeout(timer);
    if (!handle.record.closed) {
      let deadline;
      await Promise.race([
        handle.done,
        new Promise((_, reject) => {
          deadline = setTimeout(() => reject(new Error("Owned service cleanup unproved")), 1000);
        }),
      ]).finally(() => clearTimeout(deadline));
    }
  }
  const serviceEnv = () => ({
    ...env,
    DBUS_SESSION_BUS_ADDRESS: `unix:path=${root}/bus`,
    GNOME_KEYRING_CONTROL: `${root}/control`,
    XDG_RUNTIME_DIR: root,
    XDG_DATA_HOME: `${root}/data`,
    LD_LIBRARY_PATH: config.libraryDirectories.join(":"),
  });
  async function call(destination, path, method, ...args) {
    const handle = own(
      config.client.path,
      [
        "call",
        "--address",
        `unix:path=${root}/bus`,
        "--dest",
        destination,
        "--object-path",
        path,
        "--method",
        method,
        "--timeout",
        "1",
        ...args,
      ],
      serviceEnv(),
    );
    let output = Buffer.alloc(0),
      overflow = false;
    handle.child.stdout.on("data", (bytes) => {
      if (output.length + bytes.length > 4096) overflow = true;
      else output = Buffer.concat([output, bytes]);
    });
    let timer;
    try {
      await wait(
        Promise.race([
          handle.done,
          new Promise((_, reject) => {
            timer = setTimeout(() => reject(new Error("Private bus call timed out")), 1500);
          }),
        ]),
      );
      if (handle.error || handle.record.exitCode !== 0 || overflow)
        throw new Error("Private bus call unavailable");
      return output.toString().trim();
    } finally {
      clearTimeout(timer);
      await stop(handle);
      handles.splice(handles.indexOf(handle), 1);
    }
  }
  const busCall = (method, ...args) =>
    call(
      "org.freedesktop.DBus",
      "/org/freedesktop/DBus",
      `org.freedesktop.DBus.${method}`,
      ...args,
    );
  async function readiness() {
    check();
    const owner = (await busCall("GetNameOwner", "org.freedesktop.secrets")).match(
      /^\('(:[0-9]+\.[0-9]+)',\)$/,
    )?.[1];
    if (!owner) throw new Error("Secret Service owner unavailable");
    const pid = (await busCall("GetConnectionUnixProcessID", owner)).match(
      /^\(uint32 ([0-9]+),\)$/,
    )?.[1];
    const user = (await busCall("GetConnectionUnixUser", owner)).match(
      /^\(uint32 ([0-9]+),\)$/,
    )?.[1];
    if (
      Number(pid) !== keyring.child.pid ||
      Number(user) !== uid ||
      (await processIdentity(keyring.child.pid)) !== keyring.record.startTicks
    )
      throw new Error("Secret Service is not the owned same-UID daemon");
    const collection = (
      await call(
        owner,
        "/org/freedesktop/secrets",
        "org.freedesktop.Secret.Service.ReadAlias",
        "default",
      )
    ).match(/^\(objectpath '(\/org\/freedesktop\/secrets\/collection\/[a-zA-Z0-9_]+)',\)$/)?.[1];
    if (!collection) throw new Error("Default synthetic collection unavailable");
    const locked = await call(
      owner,
      collection,
      "org.freedesktop.DBus.Properties.Get",
      "org.freedesktop.Secret.Collection",
      "Locked",
    );
    if (locked !== "(<false>,)") throw new Error("Synthetic collection is locked");
    state.owner = { pid: Number(pid), uid: Number(user), collection, locked: false };
  }
  async function poll(operation) {
    const deadline = now() + 10000;
    while (true) {
      check();
      try {
        await operation();
        return;
      } catch (error) {
        check();
        if (now() >= deadline) throw error;
        await wait(pause(50));
      }
    }
  }
  return {
    wait,
    async sleep(milliseconds) {
      let timer;
      try {
        await wait(
          new Promise((resolve) => {
            timer = setTimeout(resolve, milliseconds);
          }),
        );
      } finally {
        clearTimeout(timer);
      }
    },
    async start() {
      check();
      if (state.status !== "created") throw new Error("Managed service already started");
      state.status = "starting";
      if (
        env.HOME !== "/scratch/home" ||
        env.XDG_DATA_HOME !== "/scratch/xdg-data" ||
        Object.keys(env).some((key) => /^(DBUS_|GNOME_KEYRING_|LD_)/.test(key))
      )
        throw new Error("Ambient bus, loader or home environment refused");
      state.inputs = await verify(config, fs);
      check();
      root = await fs.mkdtemp("/scratch/tmp/secret-service-");
      for (const name of ["control", "data"]) await fs.mkdir(`${root}/${name}`, { mode: 0o700 });
      const address = `unix:path=${root}/bus`;
      await fs.writeFile(`${root}/bus.conf`, privateBusConfiguration(address, uid), {
        mode: 0o600,
        flag: "wx",
      });
      bus = own(
        config.bus.path,
        ["--nofork", "--nopidfile", "--nosyslog", `--config-file=${root}/bus.conf`],
        serviceEnv(),
      );
      state.services.push(bus.record);
      await captureIdentity(bus);
      await poll(() => busCall("GetId"));
      const unlock = NodeCrypto.randomBytes(32).toString("hex");
      keyring = own(
        config.daemon.path,
        ["--foreground", "--unlock", "--components=secrets", `--control-directory=${root}/control`],
        serviceEnv(),
        unlock,
      );
      state.services.push(keyring.record);
      await captureIdentity(keyring);
      await poll(readiness);
      ready = true;
      state.status = "ready";
    },
    async beforeLaunch() {
      check();
      if (!ready) throw new Error("Secret Service has not become ready");
      await readiness();
      return {
        DBUS_SESSION_BUS_ADDRESS: `unix:path=${root}/bus`,
        GNOME_KEYRING_CONTROL: `${root}/control`,
      };
    },
    async assertBackend(app) {
      const value = await wait(
        app.evaluate(({ safeStorage }) => ({
          backend: safeStorage.getSelectedStorageBackend(),
          available: safeStorage.isEncryptionAvailable(),
        })),
      );
      state.generations.push(assertSecretStorage(value));
    },
    snapshot() {
      return structuredClone(state);
    },
    async close() {
      if (closing) return closing;
      closed = true;
      closing = (async () => {
        const errors = [];
        for (const handle of handles.toReversed()) {
          try {
            await stop(handle);
          } catch (error) {
            errors.push(error);
          }
        }
        signals.off("SIGINT", interrupt);
        signals.off("SIGTERM", terminate);
        state.status = errors.length ? "cleanup-unproved" : "closed";
        if (errors.length) throw new AggregateError(errors, "Managed service cleanup unproved");
      })();
      return closing;
    },
  };
}

export async function preserveAndCloseSecretService(service, preserve) {
  let evidenceError;
  try {
    await preserve(service.snapshot());
  } catch (error) {
    evidenceError = error;
  }
  try {
    await service.close();
  } catch (error) {
    if (evidenceError)
      throw new AggregateError([evidenceError, error], "Evidence and managed cleanup failed", {
        cause: error,
      });
    throw error;
  }
  if (evidenceError) throw evidenceError;
}

export async function launchManagedElectron({ service, launch, register, close, pending }) {
  const env = service ? await service.beforeLaunch() : {};
  pending(true);
  const operation = Promise.resolve()
    .then(() => launch(env))
    .then(
      async (app) => {
        pending(false);
        const generation = register(app);
        if (service?.snapshot().cancelled) {
          await close(generation);
          throw new Error("Cancelled Electron generation closed after launch");
        }
        return generation;
      },
      (error) => {
        pending(false);
        throw error;
      },
    );
  return service ? service.wait(operation) : operation;
}
