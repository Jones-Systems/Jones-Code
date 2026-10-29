import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeTest from "node:test";

const test = (name, body) =>
  NodeTest.test(
    name,
    {
      skip:
        // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture binds its actual host without loading application services.
        process.platform === "linux" ? false : "Packaged recovery fault injection is Linux-only",
    },
    body,
  );

const preload = NodeURL.fileURLToPath(new URL("./packaged-recovery-preload.cjs", import.meta.url));
const operationId = "12345678-1234-4234-8234-123456789abc";
const executable = NodeFS.realpathSync(process.execPath);
const stat = NodeFS.statSync(executable, { bigint: true });
const identity = Object.fromEntries(
  ["dev", "ino", "size", "mtimeNs", "ctimeNs"].map((key) => [key, String(stat[key])]),
);

function fixture(body, overrides = {}) {
  const root = NodeFS.realpathSync(
    NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-packaged-recovery-")),
  );
  NodeFS.chmodSync(root, 0o700);
  try {
    const home = NodePath.join(root, "home");
    NodeFS.mkdirSync(NodePath.join(home, "runtime"), { recursive: true, mode: 0o700 });
    const plan = {
      protocol: 1,
      fixtureId: NodeCrypto.randomBytes(16).toString("hex"),
      executable,
      identity,
      method: "readFile-before",
      target: "runtime/receipt.json",
      arm: "immediate",
      operationId,
      ...overrides,
    };
    const owner = {
      protocol: 1,
      purpose: "jones-packaged-recovery",
      fixtureId: plan.fixtureId,
      executable,
      identity,
    };
    NodeFS.writeFileSync(NodePath.join(root, "fault-plan.json"), JSON.stringify(plan), {
      mode: 0o600,
    });
    NodeFS.writeFileSync(NodePath.join(root, "fixture-owner.json"), JSON.stringify(owner), {
      mode: 0o600,
    });
    body({ root, home, plan, target: NodePath.join(home, plan.target) });
  } finally {
    NodeFS.rmSync(root, { recursive: true });
    NodeAssert.equal(NodeFS.existsSync(root), false);
  }
}

function run(f, source, extra = {}) {
  const child = NodePath.join(f.root, "child.mjs");
  NodeFS.writeFileSync(child, source, { mode: 0o600 });
  return NodeChildProcess.spawnSync(executable, ["--require", preload, child], {
    env: {
      PATH: process.env.PATH,
      HOME: f.root,
      TMPDIR: f.root,
      T3CODE_HOME: f.home,
      JONES_PACKAGED_RECOVERY_FIXTURE: NodePath.join(f.root, "fault-plan.json"),
      ...extra,
    },
    cwd: f.root,
    encoding: "utf8",
    timeout: 5000,
    maxBuffer: 65536,
  });
}

function passed(result) {
  NodeAssert.equal(result.error, undefined);
  NodeAssert.equal(result.signal, null);
  NodeAssert.equal(result.status, 0, result.stderr);
}

test("injects the named promise read once across restarts without changing payload bytes", () => {
  fixture((f) => {
    NodeFS.writeFileSync(f.target, "original");
    const source = `import {readFile} from "node:fs/promises";
      try { await readFile(${JSON.stringify(f.target)}, "utf8"); process.exitCode=2; }
      catch (error) { if (error.code !== "EIO") throw error; }`;
    passed(run(f, source));
    NodeAssert.equal(NodeFS.readFileSync(f.target, "utf8"), "original");
    NodeAssert.equal(
      JSON.parse(NodeFS.readFileSync(NodePath.join(f.root, "fault-fired.json"))).operationId,
      operationId,
    );
    passed(
      run(
        f,
        `import {readFile} from "node:fs/promises";
      if (await readFile(${JSON.stringify(f.target)}, "utf8") !== "original") process.exitCode=3;`,
      ),
    );
  });
});

test("arms only after the exact operation is durably pending", () => {
  fixture(
    (f) => {
      NodeFS.writeFileSync(f.target, "original");
      const state = NodePath.join(f.home, "runtime", "service-state.json");
      passed(
        run(
          f,
          `import {readFile,writeFile} from "node:fs/promises";
      const target=${JSON.stringify(f.target)};
      if(await readFile(target,"utf8")!=="original")throw Error("missing state");
      await writeFile(${JSON.stringify(state)},JSON.stringify({update:{id:${JSON.stringify(operationId)},status:"accepted"}}));
      if(await readFile(target,"utf8")!=="original")throw Error("changed");
      await writeFile(${JSON.stringify(state)},JSON.stringify({update:{id:"other",status:"pending"}}));
      if(await readFile(target,"utf8")!=="original")throw Error("wrong operation");
      await writeFile(${JSON.stringify(state)},JSON.stringify({update:{id:${JSON.stringify(operationId)},status:"pending"}}));
      try {await readFile(target);process.exitCode=2} catch(error){if(error.code!=="EIO")throw error}`,
        ),
      );
      NodeAssert.equal(NodeFS.existsSync(NodePath.join(f.root, "fault-fired.json")), true);
    },
    { arm: "pending-update" },
  );
});

test("backup mkdir fails before the filesystem effect and leaves sibling paths alone", () => {
  fixture(
    (f) => {
      passed(
        run(
          f,
          `import {mkdir} from "node:fs/promises";
      await mkdir(${JSON.stringify(NodePath.join(f.home, "runtime", "sibling"))});
      try {await mkdir(${JSON.stringify(f.target)});process.exitCode=2}
      catch(error){if(error.code!=="EIO")throw error}`,
        ),
      );
      NodeAssert.equal(NodeFS.existsSync(f.target), false);
      NodeAssert.equal(NodeFS.existsSync(NodePath.join(f.home, "runtime", "sibling")), true);
    },
    { method: "mkdir-before", target: "runtime/backup" },
  );
});

for (const method of ["rename-after", "rm-after"]) {
  test(method + " crashes only after the filesystem effect and durable fault claim", () => {
    fixture(
      (f) => {
        const input = NodePath.join(f.home, "runtime", "input");
        NodeFS.writeFileSync(input, "retained");
        NodeFS.writeFileSync(f.target, "old");
        const command =
          method === "rename-after"
            ? `await rename(${JSON.stringify(input)},${JSON.stringify(f.target)})`
            : `await rm(${JSON.stringify(f.target)})`;
        const result = run(
          f,
          `import {rename,rm} from "node:fs/promises";${command};process.exitCode=2;`,
        );
        NodeAssert.equal(result.error, undefined);
        NodeAssert.equal(result.signal, "SIGKILL");
        NodeAssert.equal(result.status, null);
        NodeAssert.equal(NodeFS.existsSync(NodePath.join(f.root, "fault-fired.json")), true);
        if (method === "rename-after")
          NodeAssert.equal(NodeFS.readFileSync(f.target, "utf8"), "retained");
        else NodeAssert.equal(NodeFS.existsSync(f.target), false);
      },
      { method },
    );
  });
}

test("refuses unsafe plans before the child entry can execute", () => {
  for (const patch of [
    { target: "runtime/../outside" },
    { method: "writeFile-before" },
    { identity: { ...identity, ino: "0" } },
    { extra: true },
    { method: "write-after", target: "runtime/arbitrary" },
    { method: "directory-sync-after", target: "runtime/arbitrary" },
    { method: "link-after", target: "runtime/arbitrary" },
    { method: "unlink-after", target: "runtime/arbitrary" },
  ]) {
    fixture((f) => {
      const marker = NodePath.join(f.root, "entry-ran");
      const result = run(
        f,
        `import {writeFileSync} from "node:fs";writeFileSync(${JSON.stringify(marker)},"bad");`,
      );
      NodeAssert.notEqual(result.status, 0);
      NodeAssert.match(result.stderr, /Packaged recovery fixture refused/);
      NodeAssert.equal(NodeFS.existsSync(marker), false);
    }, patch);
  }
});

test("refuses a symlinked target and a conflicting prior claim", () => {
  fixture((f) => {
    const other = NodePath.join(f.root, "outside-home");
    NodeFS.writeFileSync(other, "preserved");
    NodeFS.symlinkSync(other, f.target);
    let result = run(
      f,
      `import {readFile} from "node:fs/promises";await readFile(${JSON.stringify(f.target)});`,
    );
    NodeAssert.notEqual(result.status, 0);
    NodeAssert.match(result.stderr, /symlink in fault target/);
    NodeAssert.equal(NodeFS.readFileSync(other, "utf8"), "preserved");
    NodeFS.unlinkSync(f.target);
    NodeFS.writeFileSync(f.target, "original");
    NodeFS.writeFileSync(
      NodePath.join(f.root, "fault-fired.json"),
      JSON.stringify({ protocol: 1 }),
      {
        mode: 0o600,
      },
    );
    result = run(f, "process.exitCode=2;");
    NodeAssert.notEqual(result.status, 0);
    NodeAssert.match(result.stderr, /existing fault claim differs/);
  });
});

test("requires the dedicated fixture ownership record and exact isolated home", () => {
  fixture((f) => {
    let result = run(f, "process.exitCode=2;", {
      T3CODE_HOME: NodePath.join(f.root, "other-home"),
    });
    NodeAssert.notEqual(result.status, 0);
    NodeAssert.match(result.stderr, /isolated home differs/);
    NodeFS.unlinkSync(NodePath.join(f.root, "fixture-owner.json"));
    result = run(f, "process.exitCode=2;");
    NodeAssert.notEqual(result.status, 0);
    NodeAssert.match(result.stderr, /ENOENT/);
  });
});

test("is inert without an explicit external fixture plan", () => {
  fixture((f) => {
    NodeFS.writeFileSync(f.target, "original");
    passed(
      run(
        f,
        `import {readFile} from "node:fs/promises";
      if(await readFile(${JSON.stringify(f.target)},"utf8")!=="original")process.exitCode=2;`,
        { JONES_PACKAGED_RECOVERY_FIXTURE: undefined },
      ),
    );
    NodeAssert.equal(NodeFS.existsSync(NodePath.join(f.root, "fault-fired.json")), false);
  });
});

test("accepts equivalent JSON identity maps regardless of key order", () => {
  fixture((f) => {
    const reordered = Object.fromEntries(Object.entries(identity).reverse());
    f.plan.identity = reordered;
    NodeFS.writeFileSync(NodePath.join(f.root, "fault-plan.json"), JSON.stringify(f.plan), {
      mode: 0o600,
    });
    NodeFS.writeFileSync(f.target, "original");
    passed(
      run(
        f,
        `import {readFile} from "node:fs/promises";
      try{await readFile(${JSON.stringify(f.target)});process.exitCode=2}
      catch(error){if(error.code!=="EIO")throw error}`,
      ),
    );
  });
});

test("keeps launcher faults out of its inherited runtime child", () => {
  fixture((f) => {
    NodeFS.writeFileSync(f.target, "unchanged");
    const childSource = `import {readFileSync} from "node:fs";
      if(process.env.JONES_PACKAGED_RECOVERY_FIXTURE!==undefined)throw Error("fault leaked");
      if(readFileSync(${JSON.stringify(f.target)},"utf8")!=="unchanged")throw Error("changed");`;
    passed(
      run(
        f,
        `import {spawnSync} from "node:child_process";
      const child=spawnSync(process.execPath,["--input-type=module","-e",${JSON.stringify(childSource)}],
        {env:process.env,encoding:"utf8",timeout:2000,maxBuffer:8192});
      if(child.error||child.status!==0||child.signal)throw Error(child.stderr);
      if(process.env.JONES_PACKAGED_RECOVERY_FIXTURE!==undefined)throw Error("fault leaked");`,
        { NODE_OPTIONS: "--require=" + preload },
      ),
    );
    NodeAssert.equal(NodeFS.existsSync(NodePath.join(f.root, "fault-fired.json")), false);
  });
});

for (const method of ["write-after", "sync-after"]) {
  test(method + " ignores a different operation and unrecognized temporary filename", () => {
    fixture(
      (f) => {
        NodeFS.mkdirSync(f.target, { mode: 0o700 });
        passed(
          run(
            f,
            `import {open} from "node:fs/promises";
        for(const [name,id] of [
          ["22345678-1234-4234-8234-123456789abc","22345678-1234-4234-8234-123456789abc"],
          ["other-file",${JSON.stringify(operationId)}]
        ]) {
          const h=await open(${JSON.stringify(f.target)}+"/"+name,"wx",0o600);
          try{await h.writeFile(JSON.stringify({schema:1,operationId:id,status:"failed",settlement:"no-effect"}));await h.sync()}
          finally{await h.close()}
        }`,
          ),
        );
        NodeAssert.equal(NodeFS.existsSync(NodePath.join(f.root, "fault-fired.json")), false);
      },
      { method, target: "runtime/jones-update-settlements.staging" },
    );
  });
}

test("directory-sync-after waits for the exact failed settlement receipt", () => {
  fixture(
    (f) => {
      NodeFS.mkdirSync(f.target, { mode: 0o700 });
      passed(
        run(
          f,
          `import {open,writeFile} from "node:fs/promises";
      const sync=async()=>{const h=await open(${JSON.stringify(f.target)},"r");try{await h.sync()}finally{await h.close()}};
      await sync();
      await writeFile(${JSON.stringify(NodePath.join(f.target, operationId + ".json"))},
        JSON.stringify({schema:1,operationId:"other",status:"failed",settlement:"no-effect"}),{mode:0o600});
      await sync();`,
        ),
      );
      NodeAssert.equal(NodeFS.existsSync(NodePath.join(f.root, "fault-fired.json")), false);
    },
    { method: "directory-sync-after", target: "runtime/jones-update-settlements" },
  );
});

test("unlink-after leaves a different pinned directory outside the fault", () => {
  fixture(
    (f) => {
      NodeFS.mkdirSync(NodePath.dirname(f.target), { recursive: true, mode: 0o700 });
      NodeFS.writeFileSync(f.target, "retained");
      const sibling = NodePath.join(f.home, "runtime", "sibling");
      NodeFS.mkdirSync(sibling, { mode: 0o700 });
      NodeFS.writeFileSync(NodePath.join(sibling, "database"), "other");
      passed(
        run(
          f,
          `import {open,unlink} from "node:fs/promises";
      const directory=await open(${JSON.stringify(sibling)},"r");
      try{await unlink("/proc/self/fd/"+directory.fd+"/database")}finally{await directory.close()}`,
        ),
      );
      NodeAssert.equal(NodeFS.readFileSync(f.target, "utf8"), "retained");
      NodeAssert.equal(NodeFS.existsSync(NodePath.join(f.root, "fault-fired.json")), false);
    },
    { method: "unlink-after", target: "runtime/db-backup/" + operationId + "/database" },
  );
});

const observationIds = [
  "22345678-1234-4234-8234-123456789abc",
  "32345678-1234-4234-8234-123456789abc",
];
function observerPlan(f, patch = {}) {
  const plan = {
    protocol: 1,
    fixtureId: f.plan.fixtureId,
    executable,
    identity,
    operationId,
    requestIds: observationIds,
    childExecutable: executable,
    childIdentity: identity,
    childVersion: "1.0.0",
    ...patch,
  };
  NodeFS.writeFileSync(NodePath.join(f.root, "observer-plan.json"), JSON.stringify(plan), {
    mode: 0o600,
  });
  return {
    JONES_PACKAGED_RECOVERY_FIXTURE: undefined,
    JONES_PACKAGED_RECOVERY_OBSERVER: NodePath.join(f.root, "observer-plan.json"),
  };
}

test("observes the real captured child IPC pair and callback without recording unrelated data", () => {
  fixture((f) => {
    const extra = observerPlan(f);
    NodeFS.writeFileSync(
      NodePath.join(f.root, "serve"),
      `
      if (process.env.JONES_PACKAGED_RECOVERY_OBSERVER !== undefined) throw Error("observer inherited");
      const operationId=${JSON.stringify(operationId)}, ids=${JSON.stringify(observationIds)};
      process.send({type:"unrelated",secret:"fixture-secret-must-not-be-recorded"});
      process.send({type:"reconcile-failed-update",operationId,requestId:ids[0]});
      process.on("message",m=>{
        if(m.type!=="update-reconciled")throw Error("unexpected reply");
        if(m.requestId===ids[0]&&m.result==="settled")
          process.send({type:"reconcile-failed-update",operationId,requestId:ids[1]});
        else if(m.requestId===ids[1]&&m.result==="already-settled")process.disconnect();
        else throw Error("wrong reply");
      });`,
      { mode: 0o600 },
    );
    const source = `
      import {spawn} from "node:child_process"; import {once} from "node:events";
      const ids=${JSON.stringify(observationIds)}, operationId=${JSON.stringify(operationId)};
      const c=spawn(process.execPath,["serve"],{env:{...process.env,
        NODE_OPTIONS:${JSON.stringify("--require " + preload)},
        T3_SERVICE_LAUNCHER_CONTEXT:JSON.stringify({protocol:4,childVersion:"1.0.0"})},
        stdio:["inherit","inherit","inherit","ipc"]});
      let callbacks=0;
      c.on("message",m=>{if(m.type==="reconcile-failed-update")
        c.send({type:"update-reconciled",operationId,requestId:m.requestId,
          result:m.requestId===ids[0]?"settled":"already-settled"},error=>{
            if(error)throw error;callbacks++;
          });
      });
      const [code,signal]=await once(c,"close");
      if(code!==0||signal!==null||callbacks!==2)throw Error("child/callback did not settle");`;
    passed(run(f, source, extra));
    const directory = NodePath.join(f.root, "ipc-observations");
    const names = NodeFS.readdirSync(directory).sort();
    NodeAssert.deepEqual(
      names,
      [
        "ready.json",
        ...observationIds.flatMap((id) => [id + ".request.json", id + ".reply.json"]),
      ].sort(),
    );
    for (const [i, id] of observationIds.entries()) {
      const request = JSON.parse(
        NodeFS.readFileSync(NodePath.join(directory, id + ".request.json")),
      );
      const reply = JSON.parse(NodeFS.readFileSync(NodePath.join(directory, id + ".reply.json")));
      NodeAssert.equal(request.phase, "request");
      NodeAssert.equal(reply.result, i === 0 ? "settled" : "already-settled");
      NodeAssert.equal(reply.sendCallbackCompleted, true);
      NodeAssert.equal(reply.childPid, request.childPid);
      NodeAssert.notEqual(reply.childPid, reply.launcherPid);
      NodeAssert.equal(reply.operationId, operationId);
    }
    NodeAssert.ok(
      names.every(
        (name) =>
          !NodeFS.readFileSync(NodePath.join(directory, name), "utf8").includes("fixture-secret"),
      ),
    );
    const before = names.map((name) => NodeFS.readFileSync(NodePath.join(directory, name)));
    const repeat = run(f, source, extra);
    NodeAssert.notEqual(repeat.status, 0);
    NodeAssert.deepEqual(
      names.map((name) => NodeFS.readFileSync(NodePath.join(directory, name))),
      before,
    );
  });
});

test("refuses invalid observer bindings before entry or child execution", () => {
  for (const patch of [
    { requestIds: [observationIds[0], observationIds[0]] },
    { operationId: "wrong" },
    { identity: { ...identity, ino: "0" } },
    { childIdentity: { ...identity, ino: "0" } },
    { fixtureId: "0".repeat(32) },
    { extra: true },
  ])
    fixture((f) => {
      const env = observerPlan(f, patch);
      const marker = NodePath.join(f.root, "entry-ran");
      const result = run(
        f,
        'import {writeFileSync} from "node:fs";writeFileSync(' +
          JSON.stringify(marker) +
          ',"bad");',
        env,
      );
      NodeAssert.notEqual(result.status, 0);
      NodeAssert.equal(NodeFS.existsSync(marker), false);
    });
});

test("refuses a matching launcher context with different child arguments before spawning", () => {
  fixture((f) => {
    const env = observerPlan(f);
    const result = run(
      f,
      `import {spawn} from "node:child_process";
      spawn(process.execPath,["--version"],{env:{...process.env,T3_SERVICE_LAUNCHER_CONTEXT:
        JSON.stringify({protocol:4,childVersion:"1.0.0"})},stdio:["inherit","inherit","inherit","ipc"]});`,
      env,
    );
    NodeAssert.notEqual(result.status, 0);
    NodeAssert.match(result.stderr, /observer active child launch differs/);
    NodeAssert.deepEqual(NodeFS.readdirSync(NodePath.join(f.root, "ipc-observations")), [
      "ready.json",
    ]);
  });
});

for (const mismatch of [false, true]) {
  test(
    "live acceptance " +
      (mismatch
        ? "refuses mismatched durable evidence"
        : "arms only after captured request and native send callback"),
    () => {
      fixture(
        (f) => {
          const binding = {
            baseDir: f.home,
            dbPath: NodePath.join(f.home, "userdata", "statev2.sqlite"),
            environmentId: "synthetic-fixture",
            currentVersion: "1.0.0",
            expectedInstalledSource: "c".repeat(40),
            targetSource: "a".repeat(40),
            stagedHandle: "fixed-staged-handle",
            targetVersion: "1.0.1",
          };
          NodeFS.writeFileSync(f.target, "retained");
          const statePath = NodePath.join(f.home, "runtime", "service-state.json");
          NodeFS.writeFileSync(statePath, JSON.stringify({ protocol: 4, activeVersion: "1.0.0" }), {
            mode: 0o600,
          });
          const env = observerPlan(f);
          const planPath = NodePath.join(f.root, "observer-plan.json");
          const plan = JSON.parse(NodeFS.readFileSync(planPath));
          delete plan.requestIds;
          plan.protocol = 2;
          plan.binding = binding;
          NodeFS.writeFileSync(planPath, JSON.stringify(plan), { mode: 0o600 });
          NodeFS.writeFileSync(
            NodePath.join(f.root, "serve"),
            `
        if(process.env.JONES_PACKAGED_RECOVERY_OBSERVER!==undefined ||
          process.env.JONES_PACKAGED_RECOVERY_FIXTURE!==undefined)throw Error("fixture activation inherited");
        process.send({type:"request-update",operationId:${JSON.stringify(operationId)},
          targetVersion:"1.0.1",dbPath:${JSON.stringify(binding.dbPath)},stagedHandle:"fixed-staged-handle"});
        process.on("message",message=>{if(message.type==="update-accepted")process.disconnect()});
      `,
            { mode: 0o600 },
          );
          const source = `
        import * as fs from "node:fs/promises";
        import {spawn} from "node:child_process"; import {once} from "node:events";
        const home=${JSON.stringify(f.home)},statePath=${JSON.stringify(statePath)},target=${JSON.stringify(f.target)};
        const binding=${JSON.stringify(binding)},id=${JSON.stringify(operationId)};
        const write=async(file,value)=>{const h=await fs.open(file,"w",0o600);
          try{await h.writeFile(JSON.stringify(value));await h.sync()}finally{await h.close()}};
        const c=spawn(process.execPath,["serve"],{env:{...process.env,
          NODE_OPTIONS:${JSON.stringify("--require " + preload)},
          T3_SERVICE_LAUNCHER_CONTEXT:JSON.stringify({protocol:4,childVersion:"1.0.0"})},
          stdio:["inherit","inherit","inherit","ipc"]});
        let finished;
        c.once("message",message=>{
          finished=(async()=>{
            if(message.type!=="request-update"||message.operationId!==id)throw Error("request differs");
            await write(statePath,{protocol:4,activeVersion:"1.0.0",update:{
              id,status:"pending",phase:"accepted",fromVersion:"1.0.0",targetVersion:"1.0.1",
              dbPath:binding.dbPath,qualified:{stagedHandle:"fixed-staged-handle"}}});
            if(await fs.readFile(target,"utf8")!=="retained")throw Error("armed before acceptance");
            await fs.mkdir(home+"/runtime/jones-update-operations",{mode:0o700});
            await write(home+"/runtime/jones-update-operations/"+id+".json",
              {schema:1,operationId:id,binding:{...binding,targetSource:${JSON.stringify(mismatch ? "b".repeat(40) : "a".repeat(40))}}});
            const dir=await fs.open(home+"/runtime/jones-update-operations","r");
            try{await dir.sync()}finally{await dir.close()}
            await new Promise((resolve,reject)=>c.send({type:"update-accepted",updateId:id},error=>error?reject(error):resolve()));
            try{await fs.readFile(target);throw Error("fault did not arm")}
            catch(error){if(error.code!=="EIO")throw error}
          })();
          finished.catch(error=>{console.error(error);process.exitCode=1;c.kill("SIGTERM")});
        });
        const [code,signal]=await once(c,"close");await finished;
        if(code!==0||signal!==null)throw Error("child not settled");
      `;
          const result = run(f, source, {
            ...env,
            JONES_PACKAGED_RECOVERY_FIXTURE: NodePath.join(f.root, "fault-plan.json"),
          });
          const directory = NodePath.join(f.root, "ipc-observations");
          if (mismatch) {
            NodeAssert.notEqual(result.status, 0);
            NodeAssert.match(result.stderr, /matching durable native evidence/);
            NodeAssert.equal(
              NodeFS.existsSync(NodePath.join(directory, operationId + ".accepted.json")),
              false,
            );
            NodeAssert.equal(NodeFS.existsSync(NodePath.join(f.root, "fault-fired.json")), false);
          } else {
            passed(result);
            const receipt = JSON.parse(
              NodeFS.readFileSync(NodePath.join(directory, operationId + ".accepted.json")),
            );
            NodeAssert.equal(receipt.sendCallbackCompleted, true);
            NodeAssert.equal(receipt.operationId, operationId);
            NodeAssert.match(receipt.stateSha256, /^[a-f0-9]{64}$/);
            NodeAssert.equal(
              receipt.reservationSha256,
              NodeCrypto.createHash("sha256")
                .update(
                  NodeFS.readFileSync(
                    NodePath.join(
                      f.home,
                      "runtime",
                      "jones-update-operations",
                      operationId + ".json",
                    ),
                  ),
                )
                .digest("hex"),
            );
            NodeAssert.equal(NodeFS.existsSync(NodePath.join(f.root, "fault-fired.json")), true);
          }
          NodeAssert.equal(NodeFS.readFileSync(f.target, "utf8"), "retained");
        },
        { arm: "accepted-update" },
      );
    },
  );
}

test("live acceptance refuses seeded pending state before the entry runs", () => {
  fixture((f) => {
    const env = observerPlan(f);
    const name = NodePath.join(f.root, "observer-plan.json"),
      plan = JSON.parse(NodeFS.readFileSync(name));
    delete plan.requestIds;
    plan.protocol = 2;
    plan.binding = {
      baseDir: f.home,
      dbPath: NodePath.join(f.home, "userdata", "statev2.sqlite"),
      environmentId: "fixture",
      currentVersion: "1.0.0",
      expectedInstalledSource: "c".repeat(40),
      targetSource: "a".repeat(40),
      stagedHandle: "fixed-staged-handle",
      targetVersion: "1.0.1",
    };
    NodeFS.writeFileSync(name, JSON.stringify(plan), { mode: 0o600 });
    NodeFS.writeFileSync(
      NodePath.join(f.home, "runtime", "service-state.json"),
      JSON.stringify({
        protocol: 4,
        activeVersion: "1.0.0",
        update: { id: operationId, status: "pending", phase: "accepted" },
      }),
      { mode: 0o600 },
    );
    const marker = NodePath.join(f.root, "entry-ran");
    const result = run(
      f,
      'import {writeFileSync} from "node:fs";writeFileSync(' + JSON.stringify(marker) + ',"bad")',
      env,
    );
    NodeAssert.notEqual(result.status, 0);
    NodeAssert.match(result.stderr, /idle native state/);
    NodeAssert.equal(NodeFS.existsSync(marker), false);
  });
});
