import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import * as NodeEvents from "node:events";
import * as NodeStream from "node:stream";
import * as NodeCrypto from "node:crypto";
import {
  createManagedSecretService,
  verifySecretServiceInputs,
  privateBusConfiguration,
  assertSecretStorage,
  preserveAndCloseSecretService,
  launchManagedElectron,
} from "./support/secret-service.mjs";

const bytes = Buffer.from("synthetic executable fixture");
const sha256 = NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const config = {
  schema: "jones-private-secret-service/v1",
  daemon: { path: "/harness/support/native/bin/gnome-keyring-daemon", sha256 },
  bus: { path: "/usr/bin/dbus-daemon", sha256 },
  client: { path: "/usr/bin/gdbus", sha256 },
  libraries: [{ path: "/harness/support/native/lib/libfixture.so", sha256 }],
  libraryDirectories: ["/harness/support/native/lib"],
};
const privateEnvironment = { HOME: "/scratch/home", XDG_DATA_HOME: "/scratch/xdg-data" };
function fixture({ failSpawn, locked = false, ownerPid, env = privateEnvironment } = {}) {
  const children = [],
    writes = [],
    signals = new NodeEvents.EventEmitter();
  let clock = 0,
    nextPid = 100,
    keyringPid;
  const fs = {
    async mkdtemp() {
      return "/scratch/tmp/secret-service-fixture";
    },
    async mkdir() {},
    async writeFile(path, content) {
      writes.push({ path, content });
    },
  };
  const spawn = (command, args, options) => {
    if (command === failSpawn) throw new Error("fixture spawn failure");
    const child = new NodeEvents.EventEmitter();
    Object.assign(child, {
      pid: nextPid++,
      stdout: new NodeStream.PassThrough(),
      stderr: new NodeStream.PassThrough(),
      stdin: new NodeStream.PassThrough(),
      exitCode: null,
      signalCode: null,
      command,
      args,
      options,
      input: "",
      kills: [],
    });
    child.stdin.on("data", (data) => {
      child.input += data;
    });
    child.finish = (code, signal = null) => {
      if (child.exitCode !== null || child.signalCode !== null) return;
      child.exitCode = code;
      child.signalCode = signal;
      child.emit("exit", code, signal);
      child.emit("close", code, signal);
    };
    child.kill = (signal) => {
      child.kills.push(signal);
      child.finish(null, signal);
      return true;
    };
    children.push(child);
    if (command === config.daemon.path) keyringPid = child.pid;
    if (command === config.client.path)
      queueMicrotask(() => {
        const method = args[args.indexOf("--method") + 1];
        let result;
        if (method.endsWith("GetId")) result = "('private-bus',)";
        if (method.endsWith("GetNameOwner")) result = "(':1.1',)";
        if (method.endsWith("GetConnectionUnixProcessID"))
          result = `(uint32 ${ownerPid ?? keyringPid},)`;
        if (method.endsWith("GetConnectionUnixUser")) result = "(uint32 1000,)";
        if (method.endsWith("ReadAlias"))
          result = "(objectpath '/org/freedesktop/secrets/collection/login',)";
        if (method.endsWith("Properties.Get")) result = locked ? "(<true>,)" : "(<false>,)";
        child.stdout.write(result);
        child.finish(0);
      });
    return child;
  };
  const service = createManagedSecretService(config, {
    fs,
    spawn,
    env,
    signals,
    uid: 1000,
    verify: async () => [config.daemon],
    processIdentity: async (pid) => `ticks-${pid}`,
    now: () => clock,
    pause: async () => {
      clock += 11000;
    },
  });
  return { service, children, signals, writes };
}

async function owned(use, options) {
  const f = fixture(options);
  try {
    await use(f);
  } finally {
    await f.service.close();
    NodeAssert.equal(f.signals.listenerCount("SIGINT"), 0);
    NodeAssert.equal(f.signals.listenerCount("SIGTERM"), 0);
    NodeAssert.ok(
      f.children.every((child) => child.exitCode !== null || child.signalCode !== null),
    );
  }
}

NodeTest.test(
  "explicit staged inputs reject digest mismatch, symlinks, escaped paths and extra libraries",
  async () => {
    const fs = {
      lstat: async () => ({ isFile: () => true, isDirectory: () => true }),
      realpath: async (path) => path,
      readFile: async () => bytes,
      readdir: async () => ["libfixture.so"],
    };
    NodeAssert.equal((await verifySecretServiceInputs(config, fs)).length, 4);
    await NodeAssert.rejects(
      verifySecretServiceInputs(
        { ...config, daemon: { ...config.daemon, sha256: "0".repeat(64) } },
        fs,
      ),
      /digest mismatch/,
    );
    await NodeAssert.rejects(
      verifySecretServiceInputs(config, { ...fs, realpath: async () => "/elsewhere" }),
      /canonical/,
    );
    await NodeAssert.rejects(
      verifySecretServiceInputs(
        { ...config, daemon: { ...config.daemon, path: "/usr/bin/gnome-keyring-daemon" } },
        fs,
      ),
      /escaped/,
    );
    await NodeAssert.rejects(
      verifySecretServiceInputs(config, { ...fs, readdir: async () => ["unverified.so"] }),
      /Unverified/,
    );
  },
);

NodeTest.test(
  "private bus accepts only the explicit same-UID EXTERNAL policy with no activation",
  () => {
    const xml = privateBusConfiguration("unix:path=/scratch/tmp/secret-service-a1/bus", 1000);
    NodeAssert.match(xml, /<auth>EXTERNAL<\/auth>/);
    NodeAssert.match(xml, /<deny user="\*"\/><allow user="1000"\/>/);
    NodeAssert.doesNotMatch(xml, /include|servicedir|servicehelper|systemd|ANONYMOUS|tcp/);
    NodeAssert.throws(
      () => privateBusConfiguration("unix:path=/run/user/1000/bus", 1000),
      /private bus/,
    );
  },
);

NodeTest.test(
  "managed setup owns both foreground processes and retains child-only environment across generations",
  async () => {
    await owned(async ({ service, children, writes }) => {
      const before = { ...privateEnvironment };
      await service.start();
      const first = await service.beforeLaunch(),
        second = await service.beforeLaunch();
      NodeAssert.deepEqual(first, second);
      NodeAssert.deepEqual(privateEnvironment, before);
      NodeAssert.equal(
        first.DBUS_SESSION_BUS_ADDRESS,
        "unix:path=/scratch/tmp/secret-service-fixture/bus",
      );
      const daemon = children.find((child) => child.command === config.daemon.path);
      NodeAssert.ok(daemon.args.includes("--foreground"));
      NodeAssert.ok(daemon.args.includes("--unlock"));
      NodeAssert.ok(daemon.args.includes("--components=secrets"));
      NodeAssert.match(daemon.input, /^[a-f0-9]{64}$/);
      NodeAssert.ok(!JSON.stringify(daemon.args).includes(daemon.input));
      NodeAssert.equal(
        daemon.options.env.XDG_DATA_HOME,
        "/scratch/tmp/secret-service-fixture/data",
      );
      NodeAssert.ok(
        children.every(
          (child) => child.options.env.DBUS_SESSION_BUS_ADDRESS === first.DBUS_SESSION_BUS_ADDRESS,
        ),
      );
      NodeAssert.ok(!JSON.stringify(service.snapshot()).includes(daemon.input));
      NodeAssert.equal(writes.length, 1);
      for (let generation = 0; generation < 2; generation++)
        await service.assertBackend({
          evaluate: async () => ({ backend: "gnome_libsecret", available: true }),
        });
      NodeAssert.equal(service.snapshot().generations.length, 2);
      const saved = service.snapshot();
      await service.close();
      NodeAssert.equal(saved.status, "ready");
      NodeAssert.equal(service.snapshot().status, "closed");
      NodeAssert.ok(service.snapshot().services.every((entry) => entry.startTicks && entry.closed));
    });
  },
);

for (const [name, options, expected] of [
  ["partial keyring startup", { failSpawn: config.daemon.path }, /spawn failure/],
  ["locked collection", { locked: true }, /locked/],
  ["foreign daemon ownership", { ownerPid: 999 }, /owned same-UID/],
  [
    "ambient bus",
    { env: { ...privateEnvironment, DBUS_SESSION_BUS_ADDRESS: "unix:path=/ambient" } },
    /Ambient/,
  ],
])
  NodeTest.test(`${name} fails closed and releases only owned children`, async () => {
    await owned(async ({ service }) => {
      await NodeAssert.rejects(service.start(), expected);
    }, options);
  });

for (const signal of ["SIGINT", "SIGTERM"])
  NodeTest.test(
    `${signal} interrupts managed work and cleanup retains sanitized evidence`,
    async () => {
      await owned(async ({ service, signals }) => {
        await service.start();
        const operation = service.wait(new Promise(() => {}));
        signals.emit(signal);
        await NodeAssert.rejects(operation, /cancelled/);
        const evidence = service.snapshot();
        NodeAssert.equal(evidence.cancelled, signal);
        await service.close();
        NodeAssert.equal(service.snapshot().status, "closed");
        NodeAssert.equal(evidence.status, "ready");
      });
    },
  );

NodeTest.test("cancellation before startup launches no subprocess", async () => {
  await owned(async ({ service, signals, children }) => {
    signals.emit("SIGTERM");
    await NodeAssert.rejects(service.start(), /cancelled/);
    NodeAssert.equal(children.length, 0);
  });
});

NodeTest.test("service loss between generations refuses restart without fallback", async () => {
  await owned(async ({ service, children }) => {
    await service.start();
    children.find((child) => child.command === config.daemon.path).finish(1);
    await NodeAssert.rejects(service.beforeLaunch(), /exited/);
  });
});

for (const value of [
  { backend: "basic_text", available: true },
  { backend: "gnome_libsecret", available: false },
  { backend: "kwallet", available: true },
])
  NodeTest.test(`backend ${value.backend}/${value.available} cannot qualify`, () => {
    NodeAssert.throws(() => assertSecretStorage(value), /fallback refused/);
  });

NodeTest.test(
  "evidence is preserved before shutdown, including when evidence writing fails",
  async () => {
    for (const fail of [false, true]) {
      await owned(async ({ service, children }) => {
        await service.start();
        const dispose = preserveAndCloseSecretService(service, async (evidence) => {
          NodeAssert.equal(evidence.status, "ready");
          NodeAssert.equal(children.filter((child) => child.kills.length).length, 0);
          if (fail) throw new Error("fixture evidence failure");
        });
        if (fail) await NodeAssert.rejects(dispose, /evidence failure/);
        else await dispose;
        NodeAssert.equal(service.snapshot().status, "closed");
      });
    }
  },
);

NodeTest.test("cleanup failure retains the original evidence failure", async () => {
  const first = new Error("evidence failure"),
    second = new Error("cleanup failure");
  await NodeAssert.rejects(
    preserveAndCloseSecretService(
      {
        snapshot: () => ({}),
        close: async () => {
          throw second;
        },
      },
      async () => {
        throw first;
      },
    ),
    (error) => {
      NodeAssert.deepEqual(error.errors, [first, second]);
      return true;
    },
  );
});

NodeTest.test("closing one owned lifecycle leaves a sibling lifecycle running", async () => {
  const first = fixture(),
    second = fixture();
  try {
    await first.service.start();
    await second.service.start();
    await first.service.close();
    NodeAssert.equal(second.service.snapshot().status, "ready");
    NodeAssert.ok(
      second.children
        .filter((child) => child.command !== config.client.path)
        .every((child) => child.kills.length === 0),
    );
    await second.service.beforeLaunch();
  } finally {
    await first.service.close();
    await second.service.close();
  }
});

NodeTest.test(
  "Electron launch seam retains private environment for every generation and preserves legacy launches",
  async () => {
    const legacy = await launchManagedElectron({
      launch: async (env) => {
        NodeAssert.deepEqual(env, {});
        return "legacy";
      },
      register: (app) => app,
      close: async () => {},
      pending: () => {},
    });
    NodeAssert.equal(legacy, "legacy");
    await owned(async ({ service }) => {
      await service.start();
      const environments = [],
        pending = [];
      for (let i = 0; i < 2; i++) {
        const app = await launchManagedElectron({
          service,
          launch: async (env) => {
            environments.push(env);
            return { index: i };
          },
          register: (app) => app,
          close: async () => {},
          pending: (value) => pending.push(value),
        });
        NodeAssert.equal(app.index, i);
      }
      NodeAssert.deepEqual(environments[0], environments[1]);
      NodeAssert.ok(environments[0].DBUS_SESSION_BUS_ADDRESS.startsWith("unix:path=/scratch/tmp/"));
      NodeAssert.deepEqual(pending, [true, false, true, false]);
    });
  },
);

NodeTest.test(
  "Electron launch failure clears pending identity and preserves the launch error",
  async () => {
    await owned(async ({ service }) => {
      await service.start();
      const states = [];
      await NodeAssert.rejects(
        launchManagedElectron({
          service,
          launch: async () => {
            throw new Error("fixture launch failure");
          },
          register: () => {
            throw new Error("must not register");
          },
          close: async () => {},
          pending: (value) => states.push(value),
        }),
        /fixture launch failure/,
      );
      NodeAssert.deepEqual(states, [true, false]);
    });
  },
);

NodeTest.test(
  "cancellation records a pending launch and closes a late returned Electron handle",
  async () => {
    await owned(async ({ service, signals }) => {
      await service.start();
      let resolveLaunch, launched;
      const started = new Promise((resolve) => {
        launched = resolve;
      });
      const states = [],
        events = [];
      const operation = launchManagedElectron({
        service,
        launch: () => {
          launched();
          return new Promise((resolve) => {
            resolveLaunch = resolve;
          });
        },
        register: (app) => {
          events.push("registered");
          return app;
        },
        close: async () => {
          events.push("closed");
        },
        pending: (value) => states.push(value),
      });
      await started;
      signals.emit("SIGTERM");
      await NodeAssert.rejects(operation, /cancelled/);
      NodeAssert.deepEqual(states, [true]);
      await service.close();
      resolveLaunch({});
      await new Promise((resolve) => setImmediate(resolve));
      NodeAssert.deepEqual(states, [true, false]);
      NodeAssert.deepEqual(events, ["registered", "closed"]);
    });
  },
);
