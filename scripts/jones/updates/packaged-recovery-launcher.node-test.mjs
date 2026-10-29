import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import * as NodeHttp from "node:http";
import * as NodeCrypto from "node:crypto";
import * as NodeURL from "node:url";
import * as NodeTest from "node:test";
import { seedPackagedRecoveryFixture } from "./packaged-recovery-fixture.mjs";
import { qualifiedPayloadDigest } from "../../../apps/server/src/jones/cloud/qualifiedRuntime.ts";
import {
  probePackagedLauncherPreload,
  exercisePackagedSettlementRestart,
  settlementRestartPhases,
  createPackagedRecoveryHttpClient,
  preparePackagedReconciliationObservation,
  exercisePackagedLegacyReconciliation,
} from "./packaged-recovery-launcher.mjs";

NodeTest.test(
  "proves the external preload against a source-generated SEA without claiming Jones acceptance",
  {
    // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture binds its actual host without loading application services.
    skip: process.platform === "linux" ? false : "Packaged launcher preload probe requires Linux",
    timeout: 60000,
  },
  async (t) => {
    const parent = await NodeFSP.realpath(
      await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-packaged-recovery-probe-test-")),
    );
    let failure;
    try {
      await NodeFSP.chmod(parent, 0o700);
      const main = NodePath.join(parent, "main.cjs");
      const payloadDirectory = NodePath.join(parent, "payload");
      await NodeFSP.mkdir(NodePath.join(payloadDirectory, "client"), {
        recursive: true,
        mode: 0o700,
      });
      await NodeFSP.writeFile(
        NodePath.join(payloadDirectory, "client", "index.html"),
        "synthetic recovery runner",
        { mode: 0o600 },
      );
      const executable = NodePath.join(payloadDirectory, "t3");
      const ledger = new URL(
        "../../../apps/server/src/jones/updates/launcherOperation.ts",
        import.meta.url,
      ).href;
      const backup = new URL(
        "../../../apps/server/src/jones/cloud/qualifiedBackup.ts",
        import.meta.url,
      ).href;
      await NodeFSP.writeFile(
        main,
        `const fs=require("node:fs/promises");
      const path=require("node:path");
      const {spawn}=require("node:child_process");
      const load=require("node:module").createRequire(process.execPath);
      (async()=>{
        const home=process.env.T3CODE_HOME;
        if(process.argv.includes("serve")){
          const http=require("node:http");
          const context=JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT);
          const pending=new Map();
          process.on("message",message=>{
            if(message.type==="update-reconciled"){
              const reply=pending.get(message.requestId);
              if(reply){pending.delete(message.requestId);reply.resolve(message)}
            }
          });
          const snapshot=async()=>({
            source:"jones-actions",channel:"jones-main",phase:"blocked",
            environmentId:await fs.readFile(path.join(home,"userdata","environment-id"),"utf8"),
            currentVersion:context.childVersion,capability:{check:true,download:true,install:true},
          });
          const server=http.createServer(async(request,response)=>{
            response.setHeader("content-type","application/json");
            if(request.headers.authorization!=="Bearer synthetic-legacy-fixture-token"){
              response.writeHead(401);response.end("{}");return
            }
            try{
              if(request.method==="POST" && request.url==="/api/jones-updates/reconcile"){
                const chunks=[];let size=0;
                for await(const chunk of request){size+=chunk.length;if(size>4096)throw new Error("fixture input bound");chunks.push(chunk)}
                const body=JSON.parse(Buffer.concat(chunks).toString("utf8"));
                await new Promise((resolve,reject)=>{
                  pending.set(body.requestId,{resolve,reject});
                  process.send({type:"reconcile-failed-update",operationId:body.operationId,requestId:body.requestId},error=>{if(error)reject(error)});
                });
              }else if(request.method!=="GET" || request.url!=="/api/jones-updates"){
                response.writeHead(404);response.end("{}");return
              }
              response.end(JSON.stringify(await snapshot()));
            }catch(error){response.writeHead(500);response.end("{}");console.error(error.message)}
          });
          await new Promise((resolve,reject)=>{server.once("error",reject);server.listen(0,"127.0.0.1",resolve)});
          const runtime=path.join(home,"userdata","server-runtime.json");
          const port=server.address().port;
          await fs.writeFile(runtime+".synthetic.tmp",JSON.stringify({
            version:1,pid:process.pid,host:"127.0.0.1",port,origin:"http://127.0.0.1:"+port,
            startedAt:new Date().toISOString(),serviceManaged:true,
          }),{mode:0o600});
          await fs.rename(runtime+".synthetic.tmp",runtime);
          process.once("SIGTERM",()=>server.close(()=>{if(process.connected)process.disconnect()}));
          return;
        }

        const statePath=path.join(home,"runtime","service-state.json");
        let state;
        try{
          state=JSON.parse(await fs.readFile(statePath,"utf8"));
          if(state.protocol!==4 || !state.update) throw new Error("Synthetic launcher refuses invalid service state.");
        }catch(error){console.error(error.message);process.exitCode=1;return}
        const {writeFailedUpdateOperation,readUpdateOperationSettlement,publishUpdateOperationSettlement,evaluateLegacyUpdateSettlement}=load(${JSON.stringify(NodeURL.fileURLToPath(ledger))});
        const {removeQualifiedRecoveryPayload}=load(${JSON.stringify(NodeURL.fileURLToPath(backup))});

        if(state.update.status==="failed"){
          const qualified=state.update.qualified,id=state.update.id;
          const active=path.join(home,"runtime","versions",state.activeVersion,"t3");
          const child=spawn(active,["serve"],{
            env:{...process.env,T3CODE_HOME:home,T3_SERVICE_LAUNCHER_CONTEXT:JSON.stringify({
              protocol:4,childVersion:state.activeVersion,launcherVersion:"1.0.0",updateSettlementProtocol:1,
            })},stdio:["inherit","inherit","inherit","ipc"],
          });
          let queue=Promise.resolve();
          child.on("message",message=>{
            if(message.type!=="reconcile-failed-update")return;
            queue=queue.then(async()=>{
              let existing=await readUpdateOperationSettlement(home,message.operationId);
              let result="already-settled";
              if(!existing){
                const evaluated=await evaluateLegacyUpdateSettlement({
                  baseDir:home,operationId:message.operationId,current:state.update,
                  active:{activeVersion:state.activeVersion,activeSourceSha:qualified.binding.activeSourceSha},
                });
                if(evaluated.result!=="settle")throw new Error("Synthetic legacy evaluation refused: "+evaluated.reason);
                const nativePending={...state.update,status:"pending",phase:"accepted",dbPath:qualified.binding.dbPath};
                const digests=await writeFailedUpdateOperation(home,nativePending);
                await removeQualifiedRecoveryPayload(home,id,qualified.binding.dbPath,{transactionId:id,qualified},"failed-before-trial");
                const publication=await publishUpdateOperationSettlement(home,{
                  schema:1,operationId:id,status:"failed",settlement:"no-effect",basis:"legacy-reconciled",...digests,
                  evidence:{phaseAtFailure:"accepted",nativeReason:"launcher-restarted",candidateStarted:false,
                    activeVersion:state.activeVersion,activeSourceSha:qualified.binding.activeSourceSha,
                    recoveryPayload:"removed",restorePending:false,cause:null,writerLauncherVersion:"1.0.0",
                    recordedAt:new Date().toISOString()},
                });
                result=publication.result;
              }else{
                await publishUpdateOperationSettlement(home,existing);
              }
              await new Promise((resolve,reject)=>child.send({
                type:"update-reconciled",operationId:message.operationId,requestId:message.requestId,result,
              },error=>error?reject(error):resolve()));
            }).catch(error=>{console.error(error.message);child.kill("SIGTERM")});
          });
          process.once("SIGTERM",()=>{if(child.exitCode===null && child.signalCode===null)child.kill("SIGTERM")});
          child.once("close",()=>{process.exitCode=0});
          child.once("error",error=>{console.error(error.message);process.exitCode=1});
          const capability=path.join(home,"runtime","jones-launcher-capability.json");
          await fs.writeFile(capability+".synthetic.tmp",JSON.stringify({
            launcherPid:process.pid,childPid:child.pid,updateSettlementProtocol:1,
          }),{mode:0o600});
          await fs.rename(capability+".synthetic.tmp",capability);
          return;
        }
        const update=state.update, qualified=update.qualified, id=update.id;
        const digests=await writeFailedUpdateOperation(home,update);
        await removeQualifiedRecoveryPayload(home,id,qualified.binding.dbPath,
          {transactionId:id,qualified},"failed-before-trial");
        const existing=await readUpdateOperationSettlement(home,id);
        await publishUpdateOperationSettlement(home,existing ?? {
          schema:1,operationId:id,status:"failed",settlement:"no-effect",basis:"launcher-recorded",...digests,
          evidence:{phaseAtFailure:"accepted",nativeReason:"launcher-restarted",candidateStarted:false,
            activeVersion:qualified.binding.activeVersion,activeSourceSha:qualified.binding.activeSourceSha,
            recoveryPayload:"removed",restorePending:false,cause:null,writerLauncherVersion:"1.0.0",
            recordedAt:new Date().toISOString()},
        });
        state.update={id,fromVersion:update.fromVersion,targetVersion:update.targetVersion,
          qualified,status:"failed",reason:"launcher-restarted"};
        const temporary=statePath+".synthetic.tmp";
        await fs.writeFile(temporary,JSON.stringify(state),{mode:0o600});
        await fs.rename(temporary,statePath);
        const child=spawn(${JSON.stringify(process.execPath)},["-e","setInterval(()=>{},1000)"],
          {stdio:"ignore",env:{PATH:"/usr/bin:/bin",HOME:home,TMPDIR:home}});
        const stop=()=>{if(child.exitCode===null && child.signalCode===null)child.kill("SIGTERM")};
        process.once("SIGTERM",stop);
        child.once("close",()=>{process.exitCode=0});
        child.once("error",error=>{console.error(error.message);process.exitCode=1});
        const capability=path.join(home,"runtime","jones-launcher-capability.json");
        await fs.writeFile(capability+".synthetic.tmp",
          JSON.stringify({launcherPid:process.pid,childPid:child.pid}),{mode:0o600});
        await fs.rename(capability+".synthetic.tmp",capability);
      })().catch(error=>{console.error(error.stack);process.exitCode=1});`,
        { mode: 0o600 },
      );
      const config = NodePath.join(parent, "sea.json");
      await NodeFSP.writeFile(
        config,
        JSON.stringify({
          main,
          output: executable,
          useCodeCache: false,
          disableExperimentalSEAWarning: true,
        }),
        { mode: 0o600 },
      );
      const built = NodeChildProcess.spawnSync(process.execPath, ["--build-sea", config], {
        cwd: parent,
        env: { PATH: "/usr/bin:/bin", HOME: parent, TMPDIR: parent },
        encoding: "utf8",
        timeout: 30000,
        maxBuffer: 128 * 1024,
      });
      NodeAssert.equal(built.error, undefined);
      NodeAssert.equal(built.signal, null);
      NodeAssert.equal(built.status, 0, built.stderr);
      await NodeFSP.chmod(executable, 0o700);
      const sha256 = NodeCrypto.createHash("sha256")
        .update(await NodeFSP.readFile(executable))
        .digest("hex");
      const root = NodePath.join(parent, "jones-packaged-recovery-probe");
      await NodeFSP.mkdir(root, { mode: 0o700 });
      const result = await probePackagedLauncherPreload({ root, executable, sha256 });
      NodeAssert.equal(result.artifactUnchanged, true);
      NodeAssert.equal(result.packagedRecoveryMatrixPassed, false);
      NodeAssert.equal(result.externalDescendantSettlementRequired, true);
      NodeAssert.equal(result.processIdentityScope, "captured-child-handle-only");
      NodeAssert.equal(result.baseline.code, 1);
      NodeAssert.doesNotMatch(result.baseline.stderr, /Injected packaged recovery/);
      NodeAssert.match(result.injected.stderr, /Injected packaged recovery readFile-before/);
      NodeAssert.equal(result.claim.target, "runtime/service-state.json");
      NodeAssert.equal(
        NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(executable))
          .digest("hex"),
        sha256,
      );

      const payloadSha256 = await qualifiedPayloadDigest(payloadDirectory);
      const receipt = (active) => ({
        protocol: 1,
        repository: "Jones-Systems/Jones-Code",
        channel: "jones-main",
        version: active ? "0.0.0-preview.20261002.100" : "0.0.0-preview.20261002.101.1",
        sourceSha: (active ? "c" : "a").repeat(40),
        sourceTree: "b".repeat(40),
        installedSourceSha: (active ? "f" : "c").repeat(40),
        runId: active ? 100 : 101,
        runAttempt: 1,
        artifactId: active ? 200 : 201,
        workflow: ".github/workflows/artifact-cli-linux.yml",
        artifactDigest: "sha256:" + "d".repeat(64),
        archiveSha256: "e".repeat(64),
        platform: "linux",
        // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture binds its actual host without loading application services.
        architecture: process.arch,
        payloadSha256,
      });
      for (const phase of settlementRestartPhases) {
        let caseFailed = false;
        await t.test("synthetic restart control at " + phase, async () => {
          try {
            const caseRoot = NodePath.join(parent, "jones-packaged-recovery-" + phase);
            await NodeFSP.mkdir(caseRoot, { mode: 0o700 });
            const prepared = await seedPackagedRecoveryFixture({
              root: caseRoot,
              active: { payloadDirectory, receipt: receipt(true) },
              candidate: { payloadDirectory, receipt: receipt(false) },
              operationId: "12345678-1234-4234-8234-123456789abc",
            });
            const restarted = await exercisePackagedSettlementRestart({ prepared, phase });
            NodeAssert.equal(restarted.assertionsPassed, true);
            NodeAssert.equal(restarted.phase, phase);
            NodeAssert.equal(restarted.packagedRecoveryMatrixPassed, false);
            NodeAssert.equal(restarted.externalDescendantSettlementRequired, true);
            NodeAssert.equal(restarted.processIdentityScope, "captured-child-handle-only");
            await NodeFSP.rm(caseRoot, { recursive: true });
            await NodeAssert.rejects(NodeFSP.lstat(caseRoot), { code: "ENOENT" });
          } catch (cause) {
            caseFailed = true;
            throw cause;
          }
        });
        if (caseFailed) break;
      }

      await t.test(
        "bounds the existing HTTP routes to the named fixture child without retries",
        async () => {
          const caseRoot = NodePath.join(parent, "jones-packaged-recovery-http");
          await NodeFSP.mkdir(caseRoot, { mode: 0o700 });
          const prepared = await seedPackagedRecoveryFixture({
            root: caseRoot,
            active: { payloadDirectory, receipt: receipt(true) },
            candidate: { payloadDirectory, receipt: receipt(false) },
            operationId: "12345678-1234-4234-8234-123456789abc",
          });
          const token = "synthetic-fixture-only-token";
          const calls = [];
          let mode = "ok";
          const snapshot = {
            source: "jones-actions",
            channel: "jones-main",
            phase: "blocked",
            environmentId: prepared.staged.binding.environmentId,
            currentVersion: prepared.staged.binding.activeVersion,
            capability: { check: true, download: true, install: true },
            updateId: prepared.operationId,
            settlement: "legacy-reconciled",
          };
          const server = NodeHttp.createServer(async (request, response) => {
            const chunks = [];
            for await (const chunk of request) chunks.push(chunk);
            calls.push({
              method: request.method,
              path: request.url,
              authorization: request.headers.authorization,
              body: chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : undefined,
            });
            response.setHeader("content-type", "application/json");
            if (mode === "redirect") {
              response.writeHead(302, {
                location: "http://127.0.0.1:" + server.address().port + "/must-not-follow",
              });
              response.end("{}");
              return;
            }
            if (mode === "unauthorized") {
              response.writeHead(401);
              response.end("{}");
              return;
            }
            if (mode === "oversize") {
              response.end(JSON.stringify({ ...snapshot, padding: "x".repeat(65536) }));
              return;
            }
            if (mode === "malformed") {
              response.end("{");
              return;
            }
            if (mode === "foreign") {
              response.end(JSON.stringify({ ...snapshot, environmentId: "different-fixture" }));
              return;
            }
            if (mode === "null") {
              response.end("null");
              return;
            }
            response.end(JSON.stringify(snapshot));
          });
          let listening = false;
          try {
            await new Promise((resolve, reject) => {
              server.once("error", reject);
              server.listen(0, "127.0.0.1", resolve);
            });
            listening = true;
            const port = server.address().port;
            const runtime = {
              version: 1,
              pid: process.pid,
              host: "127.0.0.1",
              port,
              origin: "http://127.0.0.1:" + port,
              startedAt: "2026-10-28T00:00:00Z",
              serviceManaged: true,
            };
            const runtimeFile = NodePath.join(prepared.baseDir, "userdata", "server-runtime.json");
            const capability = { launcherPid: process.ppid, childPid: process.pid };
            await NodeFSP.writeFile(runtimeFile, JSON.stringify(runtime), { mode: 0o600 });
            await NodeFSP.writeFile(
              NodePath.join(prepared.baseDir, "runtime", "jones-launcher-capability.json"),
              JSON.stringify(capability),
              { mode: 0o600 },
            );
            const input = { prepared, launcherPid: process.ppid, childPid: process.pid, token };
            const client = await createPackagedRecoveryHttpClient(input);
            NodeAssert.deepEqual(await client.state(), snapshot);
            const requestId = "22345678-1234-4234-8234-123456789abc";
            NodeAssert.deepEqual(await client.reconcile(requestId), snapshot);
            NodeAssert.deepEqual(await client.install(), snapshot);
            NodeAssert.deepEqual(
              calls.map(({ method, path: requestPath, body }) => ({
                method,
                path: requestPath,
                body,
              })),
              [
                { method: "GET", path: "/api/jones-updates", body: undefined },
                {
                  method: "POST",
                  path: "/api/jones-updates/reconcile",
                  body: { operationId: prepared.operationId, requestId },
                },
                {
                  method: "POST",
                  path: "/api/jones-updates/install",
                  body: {
                    stagedHandle: prepared.staged.stagedHandle,
                    environmentId: prepared.staged.binding.environmentId,
                    currentVersion: prepared.staged.binding.activeVersion,
                  },
                },
              ],
            );
            NodeAssert.ok(calls.every((call) => call.authorization === "Bearer " + token));
            const selected = {
              operationId: prepared.operationId,
              stagedHandle: prepared.staged.stagedHandle,
              environmentId: prepared.staged.binding.environmentId,
            };
            try {
              prepared.operationId = "32345678-1234-4234-8234-123456789abc";
              prepared.staged.stagedHandle = "changed-after-capture";
              prepared.staged.binding.environmentId = "changed-after-capture";
              await client.reconcile(requestId);
              await client.install();
              NodeAssert.equal(calls.at(-2).body.operationId, selected.operationId);
              NodeAssert.equal(calls.at(-1).body.stagedHandle, selected.stagedHandle);
              NodeAssert.equal(calls.at(-1).body.environmentId, selected.environmentId);
            } finally {
              prepared.operationId = selected.operationId;
              prepared.staged.stagedHandle = selected.stagedHandle;
              prepared.staged.binding.environmentId = selected.environmentId;
            }
            mode = "null";
            NodeAssert.equal(await client.state(), null);
            for (const failure of [
              "redirect",
              "oversize",
              "malformed",
              "foreign",
              "unauthorized",
            ]) {
              mode = failure;
              const before = calls.length;
              await NodeAssert.rejects(
                client.reconcile(requestId),
                (error) =>
                  error.message === "Packaged recovery HTTP exchange failed." &&
                  error.unknownEffect === (failure !== "unauthorized") &&
                  !String(error).includes(token),
              );
              NodeAssert.equal(calls.length, before + 1, "failed exchange was retried");
            }
            const before = calls.length;
            NodeAssert.throws(
              () => client.reconcile("not-a-fixed-request"),
              /fixed reconciliation/,
            );
            await NodeFSP.writeFile(
              runtimeFile,
              JSON.stringify({ ...runtime, pid: process.pid + 1 }),
              {
                mode: 0o600,
              },
            );
            await NodeAssert.rejects(client.state(), /captured loopback child/);
            await NodeAssert.rejects(
              createPackagedRecoveryHttpClient(input),
              /captured loopback child/,
            );
            NodeAssert.equal(calls.length, before);
            await NodeFSP.writeFile(
              runtimeFile,
              JSON.stringify({ ...runtime, origin: "http://localhost:" + port }),
              { mode: 0o600 },
            );
            await NodeAssert.rejects(
              createPackagedRecoveryHttpClient(input),
              /captured loopback child/,
            );
            await NodeFSP.writeFile(runtimeFile, JSON.stringify(runtime), { mode: 0o600 });
            await NodeAssert.rejects(
              createPackagedRecoveryHttpClient({ ...input, token: "unsafe\nheader" }),
              /in-memory fixture bearer/,
            );
            const userdata = NodePath.join(prepared.baseDir, "userdata");
            const retained = NodePath.join(caseRoot, "userdata-retained");
            const outside = NodePath.join(parent, "unrelated-http-userdata");
            await NodeFSP.mkdir(outside, { mode: 0o700 });
            await NodeFSP.rename(userdata, retained);
            await NodeFSP.symlink(outside, userdata);
            await NodeAssert.rejects(client.state(), /physical HTTP fixture directories/);
            await NodeAssert.rejects(
              createPackagedRecoveryHttpClient(input),
              /physical HTTP fixture directories/,
            );
            NodeAssert.deepEqual(await NodeFSP.readdir(outside), []);
            await NodeFSP.unlink(userdata);
            await NodeFSP.rename(retained, userdata);
            await NodeFSP.rmdir(outside);
            const abort = new AbortController();
            abort.abort(new Error("cancel before HTTP"));
            await NodeAssert.rejects(
              createPackagedRecoveryHttpClient({ ...input, signal: abort.signal }),
              /cancel before HTTP/,
            );
            NodeAssert.equal(calls.length, before);
          } finally {
            if (listening) {
              await new Promise((resolve) => {
                server.close(resolve);
                server.closeAllConnections();
              });
              NodeAssert.equal(server.listening, false);
            }
          }
          await NodeFSP.rm(caseRoot, { recursive: true });
          await NodeAssert.rejects(NodeFSP.lstat(caseRoot), { code: "ENOENT" });
        },
      );

      await t.test(
        "prepares an immutable observer plan and refuses incomplete or foreign IPC receipts",
        async () => {
          const caseRoot = NodePath.join(parent, "jones-packaged-recovery-observer");
          await NodeFSP.mkdir(caseRoot, { mode: 0o700 });
          const prepared = await seedPackagedRecoveryFixture({
            root: caseRoot,
            active: { payloadDirectory, receipt: receipt(true) },
            candidate: { payloadDirectory, receipt: receipt(false) },
            operationId: "12345678-1234-4234-8234-123456789abc",
            kind: "legacy-failed",
            recoveryPayload: false,
            archivedOutcome: true,
          });
          const ids = [
            "22345678-1234-4234-8234-123456789abc",
            "32345678-1234-4234-8234-123456789abc",
          ];
          await NodeAssert.rejects(
            preparePackagedReconciliationObservation({ prepared, requestIds: [ids[0], ids[0]] }),
            /two distinct request IDs/,
          );
          const observation = await preparePackagedReconciliationObservation({
            prepared,
            requestIds: ids,
          });
          const planPath = NodePath.join(caseRoot, "observer-plan.json");
          const planBytes = await NodeFSP.readFile(planPath);
          const plan = JSON.parse(planBytes);
          NodeAssert.equal(plan.operationId, prepared.operationId);
          NodeAssert.equal(
            plan.childExecutable,
            NodePath.join(
              prepared.baseDir,
              "runtime",
              "versions",
              prepared.staged.binding.activeVersion,
              "t3",
            ),
          );
          NodeAssert.equal(observation.environment.JONES_PACKAGED_RECOVERY_OBSERVER, planPath);
          NodeAssert.match(observation.environment.NODE_OPTIONS, /--require /);
          await NodeAssert.rejects(
            preparePackagedReconciliationObservation({ prepared, requestIds: ids }),
            { code: "EEXIST" },
          );
          NodeAssert.deepEqual(await NodeFSP.readFile(planPath), planBytes);
          const directory = NodePath.join(caseRoot, "ipc-observations");
          await NodeFSP.mkdir(directory, { mode: 0o700 });
          const write = (name, value) =>
            NodeFSP.writeFile(NodePath.join(directory, name + ".json"), JSON.stringify(value), {
              mode: 0o600,
            });
          const captured = { launcherPid: process.ppid, childPid: process.pid };
          await NodeAssert.rejects(observation.readPair(captured), /incomplete or unexpected/);
          await write("ready", {
            protocol: 1,
            fixtureId: prepared.fixtureId,
            operationId: prepared.operationId,
            requestIds: [...ids],
            launcherPid: captured.launcherPid,
          });
          for (const [index, requestId] of ids.entries()) {
            const common = {
              protocol: 1,
              fixtureId: prepared.fixtureId,
              operationId: prepared.operationId,
              requestId,
              ...captured,
            };
            await write(requestId + ".request", { ...common, phase: "request" });
            await write(requestId + ".reply", {
              ...common,
              phase: "reply",
              result: index === 0 ? "settled" : "already-settled",
              sendCallbackCompleted: true,
            });
          }
          ids[0] = "42345678-1234-4234-8234-123456789abc";
          const boundOperation = prepared.operationId;
          prepared.operationId = ids[0];
          const pair = await observation.readPair(captured);
          NodeAssert.equal(pair.operationId, boundOperation);
          NodeAssert.deepEqual(
            pair.replies.map((reply) => reply.result),
            ["settled", "already-settled"],
          );
          NodeAssert.equal(pair.packagedRecoveryMatrixPassed, false);
          const secondPath = NodePath.join(directory, plan.requestIds[1] + ".reply.json");
          const originalReply = await NodeFSP.readFile(secondPath);
          const bad = JSON.parse(originalReply);
          for (const mutation of [
            { result: "settled" },
            { operationId: ids[0] },
            { childPid: captured.childPid + 1 },
            { sendCallbackCompleted: false },
            { extra: "unrelated" },
          ]) {
            await NodeFSP.writeFile(secondPath, JSON.stringify({ ...bad, ...mutation }), {
              mode: 0o600,
            });
            await NodeAssert.rejects(observation.readPair(captured), { name: "AssertionError" });
          }
          await NodeFSP.writeFile(secondPath, originalReply, { mode: 0o600 });
          await NodeFSP.chmod(directory, 0o777);
          await NodeAssert.rejects(observation.readPair(captured), /unsafe observer directory/);
          await NodeFSP.chmod(directory, 0o700);
          await NodeAssert.rejects(
            observation.readPair({ ...captured, launcherPid: captured.childPid }),
            /distinct captured/,
          );
          await NodeFSP.rm(caseRoot, { recursive: true });
          await NodeAssert.rejects(NodeFSP.lstat(caseRoot), { code: "ENOENT" });
        },
      );

      for (const archivedOutcome of [false, true]) {
        await t.test(
          "runs the real HTTP/IPC transport control with archivedOutcome=" + archivedOutcome,
          async () => {
            const caseRoot = NodePath.join(
              parent,
              "jones-packaged-recovery-legacy-" + archivedOutcome,
            );
            await NodeFSP.mkdir(caseRoot, { mode: 0o700 });
            const prepared = await seedPackagedRecoveryFixture({
              root: caseRoot,
              active: { payloadDirectory, receipt: receipt(true) },
              candidate: { payloadDirectory, receipt: receipt(false) },
              operationId: "12345678-1234-4234-8234-123456789abc",
              kind: "legacy-failed",
              archivedOutcome,
            });
            const input = {
              prepared,
              requestIds: [
                "22345678-1234-4234-8234-123456789abc",
                "32345678-1234-4234-8234-123456789abc",
              ],
              token: "synthetic-legacy-fixture-token",
            };
            await NodeAssert.rejects(
              exercisePackagedLegacyReconciliation({ ...input, token: "unsafe\nbearer" }),
              /explicit in-memory bearer/,
            );
            const fixedIds = [...input.requestIds];
            const execution = exercisePackagedLegacyReconciliation(input);
            input.requestIds[0] = "42345678-1234-4234-8234-123456789abc";
            const result = await execution;
            NodeAssert.deepEqual(
              result.observation.replies.map((reply) => reply.requestId),
              fixedIds,
            );
            NodeAssert.equal(result.assertionsPassed, true);
            NodeAssert.equal(result.packagedRecoveryMatrixPassed, false);
            NodeAssert.equal(result.externalDescendantSettlementRequired, true);
            NodeAssert.deepEqual(
              result.observation.replies.map((reply) => reply.result),
              ["settled", "already-settled"],
            );
            NodeAssert.equal(result.settlement.basis, "legacy-reconciled");
            NodeAssert.equal(result.state.update.id, prepared.operationId);
            NodeAssert.equal(result.state.update.status, "failed");
            NodeAssert.ok(!JSON.stringify(result).includes(input.token));
            await NodeFSP.rm(caseRoot, { recursive: true });
            await NodeAssert.rejects(NodeFSP.lstat(caseRoot), { code: "ENOENT" });
          },
        );
      }

      const refused = NodePath.join(parent, "jones-packaged-recovery-wrong-digest");
      await NodeFSP.mkdir(refused, { mode: 0o700 });
      await NodeAssert.rejects(
        probePackagedLauncherPreload({
          root: refused,
          executable,
          sha256: "0".repeat(64),
        }),
        /executable digest differs/,
      );
      NodeAssert.deepEqual(await NodeFSP.readdir(refused), []);
      const controller = new AbortController();
      controller.abort(new Error("cancel before fixture start"));
      await NodeAssert.rejects(
        probePackagedLauncherPreload({
          root: refused,
          executable,
          sha256,
          signal: controller.signal,
        }),
        /cancel before fixture start/,
      );
      NodeAssert.deepEqual(await NodeFSP.readdir(refused), []);
    } catch (cause) {
      failure = cause;
      throw cause;
    } finally {
      if (failure?.preserveScratch === true) {
        failure.scratchRoot = parent;
      } else {
        await NodeFSP.rm(parent, { recursive: true });
        await NodeAssert.rejects(NodeFSP.lstat(parent), { code: "ENOENT" });
      }
    }
  },
);
