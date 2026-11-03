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
  exercisePackagedAcceptedFailure,
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
            if(message.type==="update-reconciled" || message.type==="update-rejected"){
              const reply=pending.get(message.requestId ?? message.operationId);
              if(reply){pending.delete(message.requestId ?? message.operationId);reply.resolve(message)}
            }
          });
          const snapshot=async()=>({
            source:"jones-actions",channel:"jones-main",phase:"blocked",
            environmentId:await fs.readFile(path.join(home,"userdata","environment-id"),"utf8"),
            currentVersion:context.childVersion,capability:{check:true,download:true,install:true},
          });
          const requestedPort=Number(process.env.T3CODE_PORT);
          if(!Number.isInteger(requestedPort)||requestedPort<1||requestedPort>65535)
            throw new Error("Packaged server requires a port from 1 to 65535.");
          const server=http.createServer(async(request,response)=>{
            response.setHeader("content-type","application/json");
            if(request.headers.authorization!=="Bearer synthetic-legacy-fixture-token"){
              response.writeHead(401);response.end("{}");return
            }
            try{
              if(request.method==="POST" && request.url==="/api/jones-updates/operations/start"){
                const chunks=[];let size=0;
                for await(const chunk of request){size+=chunk.length;if(size>4096)throw new Error("fixture input bound");chunks.push(chunk)}
                const body=JSON.parse(Buffer.concat(chunks).toString("utf8"));
                const reply=await new Promise((resolve,reject)=>{
                  pending.set(body.operationId,{resolve,reject});
                  process.send({type:"request-update",operationId:body.operationId,
                    targetVersion:"0.0.0-preview.20261002.101.1",dbPath:path.join(home,"userdata","statev2.sqlite"),
                    stagedHandle:body.action.stagedHandle},error=>{if(error)reject(error)});
                });
                response.end(JSON.stringify({...body,protocol:1,phase:"blocked",updatedAt:new Date().toISOString(),
                  code:"failed-operation-unsettled",message:reply.reason}));
                return;
              }
              if(request.method==="POST" && request.url==="/api/jones-updates/reconcile"){
                const chunks=[];let size=0;
                for await(const chunk of request){size+=chunk.length;if(size>4096)throw new Error("fixture input bound");chunks.push(chunk)}
                const body=JSON.parse(Buffer.concat(chunks).toString("utf8"));
                const reply=await new Promise((resolve,reject)=>{
                  pending.set(body.requestId,{resolve,reject});
                  process.send({type:"reconcile-failed-update",operationId:body.operationId,requestId:body.requestId},error=>{if(error)reject(error)});
                });
                if(reply.result==="refused"){
                  response.end(JSON.stringify({
                    source:"jones-actions",channel:"jones-main",phase:"blocked",
                    message:"Reconciliation refused: "+reply.reason+".",
                    capability:{check:false,download:false,install:false,reason:"bootstrap-required"},
                    childVersion:context.childVersion,launcherVersion:context.launcherVersion,
                    launcherSettlement:"supported",launcherRefresh:"none",settlement:"none",
                    updateId:body.operationId,settlementRefusal:reply.reason,
                  }));return;
                }
              }else if(request.method!=="GET" || request.url!=="/api/jones-updates"){
                response.writeHead(404);response.end("{}");return
              }
              response.end(JSON.stringify(await snapshot()));
            }catch(error){response.writeHead(500);response.end("{}");console.error(error.message)}
          });
          await new Promise((resolve,reject)=>{server.once("error",reject);server.listen(requestedPort,"127.0.0.1",resolve)});
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
        const {archiveUpdateOperation,writeFailedUpdateOperation,readUpdateOperationSettlement,publishUpdateOperationSettlement,evaluateLegacyUpdateSettlement,assertNoUnreconciledUpdateOperations,FailedOperationUnsettledError}=load(${JSON.stringify(NodeURL.fileURLToPath(ledger))});
        const {removeQualifiedRecoveryPayload}=load(${JSON.stringify(NodeURL.fileURLToPath(backup))});

        if(state.update.status==="failed"){
          const qualified=state.update.qualified,id=state.update.id;
          const active=path.join(home,"runtime","versions",state.activeVersion,"t3");
          const child=spawn(active,["serve"],{
            env:{...process.env,T3CODE_HOME:home,T3_SERVICE_LAUNCHER_CONTEXT:JSON.stringify({
              protocol:4,childVersion:state.activeVersion,launcherVersion:qualified.receipt.version,updateSettlementProtocol:1,
              update:state.update,
            })},stdio:["inherit","inherit","inherit","ipc"],
          });
          let queue=Promise.resolve();
          child.on("message",message=>{
            if(message.type==="request-update"){
              queue=queue.then(async()=>{
                try {
                  await archiveUpdateOperation(home,state.update);
                  await assertNoUnreconciledUpdateOperations(home,state.update);
                  throw new Error("synthetic fixture expected an unsettled operation");
                } catch(error) {
                  if(!(error instanceof FailedOperationUnsettledError))throw error;
                  await new Promise((resolve,reject)=>child.send({type:"update-rejected",operationId:message.operationId,
                    reason:"failed-operation-unsettled: "+error.operationId+
                      " requires reconciliation before another Install; the current server remains active.",
                  },error=>error?reject(error):resolve()));
                }
              }).catch(error=>{console.error(error.message);child.kill("SIGTERM")});
              return;
            }
            if(message.type!=="reconcile-failed-update")return;
            queue=queue.then(async()=>{
              let existing=await readUpdateOperationSettlement(home,message.operationId);
              let result="already-settled",reason;
              if(!existing){
                const evaluated=await evaluateLegacyUpdateSettlement({
                  baseDir:home,operationId:message.operationId,current:state.update,
                  active:{activeVersion:state.activeVersion,activeSourceSha:JSON.parse(
                    await fs.readFile(path.join(home,"runtime","versions",state.activeVersion,".jones-runtime-receipt.json"),"utf8")).sourceSha},
                });
                if(evaluated.result==="refused"){
                  result="refused";reason=evaluated.reason;
                }else{
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
                }
              }else{
                await publishUpdateOperationSettlement(home,existing);
              }
              await new Promise((resolve,reject)=>child.send({
                type:"update-reconciled",operationId:message.operationId,requestId:message.requestId,result,...(reason===undefined?{}:{reason}),
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
            kind: "legacy-failed",
            legacyVariant: "restore-pending",
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
            if (mode.startsWith("refusal")) {
              const value = {
                source: "jones-actions",
                channel: "jones-main",
                phase: "blocked",
                message: "Reconciliation refused: restore-pending.",
                capability: {
                  check: false,
                  download: false,
                  install: false,
                  reason: "bootstrap-required",
                },
                childVersion: prepared.state.activeVersion,
                launcherVersion: prepared.inputs.candidate.receipt.version,
                launcherSettlement: "supported",
                launcherRefresh: "none",
                settlement: "none",
                updateId: prepared.operationId,
                settlementRefusal: "restore-pending",
              };
              if (mode === "refusal-child") value.childVersion = "foreign";
              if (mode === "refusal-operation") value.updateId = "foreign";
              if (mode === "refusal-reason") value.settlementRefusal = "insufficient-evidence";
              if (mode === "refusal-capability") value.capability.install = true;
              if (mode === "refusal-status") response.writeHead(409);
              response.end(JSON.stringify(value));
              return;
            }
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
            if (request.url === "/api/jones-updates/operations/start") {
              const input = calls.at(-1).body;
              const receipt = {
                ...input,
                protocol: 1,
                phase: "accepted",
                acceptedAt: "2026-10-28T00:00:00Z",
                updatedAt: "2026-10-28T00:00:00Z",
              };
              if (mode === "op-foreign")
                receipt.operationId = "42345678-1234-4234-8234-123456789abc";
              if (mode === "op-source") receipt.expectedInstalledSource = "f".repeat(40);
              if (mode === "op-stage")
                receipt.action = { ...receipt.action, stagedHandle: "other-stage" };
              if (mode === "op-update") receipt.updateId = "other-operation";
              if (mode === "op-phase") receipt.phase = "imaginary";
              if (mode === "op-time") receipt.updatedAt = "not-a-time";
              if (mode === "op-message") receipt.message = "x".repeat(1025);
              if (mode === "op-extra") receipt.unrelated = "not-returned-in-projection";
              response.end(JSON.stringify(mode === "op-state" ? snapshot : receipt));
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
            mode = "refusal";
            const refused = await client.reconcile(requestId, "restore-pending");
            NodeAssert.equal(refused.settlementRefusal, "restore-pending");
            NodeAssert.equal(refused.childVersion, prepared.state.activeVersion);
            await NodeAssert.rejects(client.reconcile(requestId), /HTTP exchange failed/);
            NodeAssert.throws(
              () => client.reconcile(requestId, "insufficient-evidence"),
              /expected fixture reconciliation refusal/,
            );
            for (mode of [
              "refusal-child",
              "refusal-operation",
              "refusal-reason",
              "refusal-capability",
              "refusal-status",
            ])
              await NodeAssert.rejects(
                client.reconcile(requestId, "restore-pending"),
                /HTTP exchange failed/,
              );
            mode = "ok";
            const freshId = "42345678-1234-4234-8234-123456789abc";
            const initialInstall = await client.installOperation();
            NodeAssert.equal(initialInstall.operationId, prepared.operationId);
            NodeAssert.equal(initialInstall.phase, "accepted");
            NodeAssert.deepEqual(calls.at(-1).body, {
              operationId: prepared.operationId,
              environmentId: prepared.staged.binding.environmentId,
              expectedInstalledSource: prepared.staged.binding.activeSourceSha,
              action: {
                kind: "install",
                stagedHandle: prepared.staged.stagedHandle,
                currentVersion: prepared.staged.binding.activeVersion,
                targetSource: prepared.staged.receipt.sourceSha,
              },
            });
            const freshInstall = await client.installOperation(freshId);
            NodeAssert.equal(freshInstall.operationId, freshId);
            NodeAssert.deepEqual(freshInstall.action, initialInstall.action);
            for (const invalid of [
              "op-foreign",
              "op-source",
              "op-stage",
              "op-update",
              "op-phase",
              "op-time",
              "op-message",
              "op-state",
            ]) {
              mode = invalid;
              const count = calls.length;
              await NodeAssert.rejects(
                client.installOperation(),
                (error) =>
                  error.message === "Packaged recovery HTTP exchange failed." &&
                  error.unknownEffect === true &&
                  !String(error).includes(token),
              );
              NodeAssert.equal(calls.length, count + 1, "operation exchange was retried");
            }
            mode = "op-extra";
            NodeAssert.equal((await client.installOperation()).unrelated, undefined);
            const count = calls.length;
            NodeAssert.throws(
              () => client.installOperation("invalid"),
              /fixed Install operation ID/,
            );
            NodeAssert.equal(calls.length, count, "invalid ID was sent");
            mode = "ok";

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
          for (const installOperationId of ["invalid", prepared.operationId, ids[0]]) {
            await NodeAssert.rejects(
              preparePackagedReconciliationObservation({
                prepared,
                requestIds: ids,
                installOperationId,
              }),
              /distinct fixed unsettled Install/,
            );
          }
          const observation = await preparePackagedReconciliationObservation({
            prepared,
            requestIds: ids,
          });
          const planPath = NodePath.join(caseRoot, "observer-plan.json");
          const planBytes = await NodeFSP.readFile(planPath);
          const plan = JSON.parse(planBytes);
          NodeAssert.equal(plan.operationId, prepared.operationId);
          NodeAssert.deepEqual(plan.retainedUpdate, prepared.state.update);
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

      for (const { archivedOutcome, legacyVariant } of [
        { archivedOutcome: false },
        { archivedOutcome: true },
        { archivedOutcome: true, legacyVariant: "restore-pending" },
        { archivedOutcome: true, legacyVariant: "active-changed" },
      ]) {
        await t.test(
          "runs the real HTTP/IPC transport control with archivedOutcome=" +
            archivedOutcome +
            (legacyVariant ?? ""),
          async () => {
            const caseRoot = NodePath.join(
              parent,
              "jones-packaged-recovery-legacy-" + archivedOutcome + (legacyVariant ?? ""),
            );
            await NodeFSP.mkdir(caseRoot, { mode: 0o700 });
            const prepared = await seedPackagedRecoveryFixture({
              root: caseRoot,
              active: { payloadDirectory, receipt: receipt(true) },
              candidate: { payloadDirectory, receipt: receipt(false) },
              operationId: "12345678-1234-4234-8234-123456789abc",
              kind: "legacy-failed",
              archivedOutcome,
              legacyVariant,
            });
            const input = {
              prepared,
              requestIds: [
                "22345678-1234-4234-8234-123456789abc",
                "32345678-1234-4234-8234-123456789abc",
              ],
              token: "synthetic-legacy-fixture-token",
              installOperationId: "52345678-1234-4234-8234-123456789abc",
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
            NodeAssert.equal(result.installArchive.created, !archivedOutcome);
            NodeAssert.match(result.installArchive.sha256, /^[a-f0-9]{64}$/);
            NodeAssert.equal(result.packagedRecoveryMatrixPassed, false);
            NodeAssert.equal(result.externalDescendantSettlementRequired, true);
            NodeAssert.equal(result.installReceipt.operationId, input.installOperationId);
            NodeAssert.equal(result.installReceipt.phase, "blocked");
            NodeAssert.equal(
              result.observation.admissionRefusal.requestId,
              input.installOperationId,
            );
            NodeAssert.equal(result.observation.admissionRefusal.sendCallbackCompleted, true);
            NodeAssert.equal(
              result.observation.admissionRefusal.reason,
              "failed-operation-unsettled: " +
                prepared.operationId +
                " requires reconciliation before another Install; the current server remains active.",
            );
            NodeAssert.deepEqual(
              result.observation.replies.map((reply) => reply.result),
              legacyVariant ? ["refused", "refused"] : ["settled", "already-settled"],
            );
            if (legacyVariant) {
              NodeAssert.equal(result.settlement, null);
              NodeAssert.deepEqual(result.refusal, {
                variant: legacyVariant,
                reason: legacyVariant,
                heldEvidenceUnchanged: true,
              });
              NodeAssert.ok(
                result.observation.replies.every((reply) => reply.reason === legacyVariant),
              );
              await NodeAssert.rejects(
                preparePackagedReconciliationObservation({ prepared, requestIds: fixedIds }),
                /occupied|EEXIST/,
              );
            } else NodeAssert.equal(result.settlement.basis, "legacy-reconciled");
            if (legacyVariant === "active-changed") {
              NodeAssert.equal(
                result.activeReceiptDrift.launcherPid,
                result.observation.replies[0].launcherPid,
              );
              NodeAssert.equal(
                result.activeReceiptDrift.childPid,
                result.observation.replies[0].childPid,
              );
              NodeAssert.notEqual(
                result.activeReceiptDrift.originalSha256,
                result.activeReceiptDrift.changedSha256,
              );
            } else NodeAssert.equal(result.activeReceiptDrift, undefined);
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

NodeTest.test(
  "drives accepted failure, durable restart settlement and a fresh Install through captured HTTP/IPC",
  {
    // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture binds its actual host without loading application services.
    skip: process.platform === "linux" ? false : "Packaged Linux control requires Linux",
    timeout: 60000,
  },
  async (t) => {
    const parent = await NodeFSP.realpath(
      await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-packaged-accepted-test-")),
    );
    await NodeFSP.chmod(parent, 0o700);
    let failure;
    try {
      const payload = NodePath.join(parent, "payload");
      await NodeFSP.mkdir(NodePath.join(payload, "client"), { recursive: true, mode: 0o700 });
      await NodeFSP.writeFile(
        NodePath.join(payload, "client", "index.html"),
        "synthetic accepted-failure control",
        { mode: 0o600 },
      );
      const executable = NodePath.join(payload, "t3"),
        main = NodePath.join(parent, "accepted.cjs");
      const ledger = NodeURL.fileURLToPath(
        new URL("../../../apps/server/src/jones/updates/launcherOperation.ts", import.meta.url),
      );
      const runtime = NodeURL.fileURLToPath(
        new URL("../../../apps/server/src/jones/cloud/qualifiedRuntime.ts", import.meta.url),
      );
      const ownership = NodeURL.fileURLToPath(
        new URL("../../../apps/server/src/jones/cloud/stoppedServerOwnership.ts", import.meta.url),
      );
      await NodeFSP.writeFile(
        main,
        `
      const fs=require("node:fs/promises"),path=require("node:path"),{spawn}=require("node:child_process");
      const load=require("node:module").createRequire(process.execPath);
      (async()=>{
        const home=process.env.T3CODE_HOME,statePath=path.join(home,"runtime","service-state.json");
        const write=async(file,value)=>{const tmp=file+".synthetic.tmp";const h=await fs.open(tmp,"w",0o600);
          try{await h.writeFile(JSON.stringify(value));await h.sync()}finally{await h.close()}
          await fs.rename(tmp,file);const d=await fs.open(path.dirname(file),"r");try{await d.sync()}finally{await d.close()}};
        if(process.argv.includes("serve")){
          const context=JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT);
          const http=require("node:http"),pending=new Map();
          process.on("message",m=>{if(m.type==="update-accepted"){
            const resolve=pending.get(m.updateId);if(resolve){pending.delete(m.updateId);resolve()}}});
          const requestedPort=Number(process.env.T3CODE_PORT);
          if(!Number.isInteger(requestedPort)||requestedPort<1||requestedPort>65535)
            throw new Error("Packaged server requires a port from 1 to 65535.");
          const server=http.createServer(async(req,res)=>{
            res.setHeader("content-type","application/json");
            if(req.headers.authorization!=="Bearer synthetic-accepted-fixture-token"){res.writeHead(401);res.end("{}");return}
            try{
              if(req.method!=="POST"||req.url!=="/api/jones-updates/operations/start"){res.writeHead(404);res.end("{}");return}
              let size=0,chunks=[];for await(const b of req){size+=b.length;if(size>4096)throw Error("fixture input bound");chunks.push(b)}
              const input=JSON.parse(Buffer.concat(chunks).toString("utf8")),id=input.operationId;
              const state=JSON.parse(await fs.readFile(statePath,"utf8"));
              const refused=state.update?.id===id && state.update.status==="failed";
              if(!refused)await new Promise((resolve,reject)=>{
                pending.set(id,resolve);process.send({type:"request-update",operationId:id,targetVersion:"0.0.0-preview.20261002.101.1",
                  dbPath:path.join(home,"userdata","statev2.sqlite"),stagedHandle:input.action.stagedHandle},e=>{if(e)reject(e)});
              });
              res.end(JSON.stringify({...input,protocol:1,phase:refused?"blocked":"accepted",
                updatedAt:new Date().toISOString(),...(refused?{}:{acceptedAt:new Date().toISOString()})}));
            }catch(e){res.writeHead(500);res.end("{}");console.error(e.message)}
          });
          await new Promise((resolve,reject)=>{server.once("error",reject);server.listen(requestedPort,"127.0.0.1",resolve)});const port=server.address().port;
          await write(path.join(home,"userdata","server-runtime.json"),{version:1,pid:process.pid,host:"127.0.0.1",port,
            origin:"http://127.0.0.1:"+port,startedAt:new Date().toISOString(),serviceManaged:true});
          process.once("SIGTERM",()=>server.close(()=>{if(process.connected)process.disconnect()}));
          return;
        }
        let state=JSON.parse(await fs.readFile(statePath,"utf8"));
        const ledger=load(${JSON.stringify(ledger)}),runtime=load(${JSON.stringify(runtime)}),
          {withStoppedServerOwnership}=load(${JSON.stringify(ownership)});
        if(state.update?.status==="pending"){
          const old=state.update,id=old.id;
          const digests=await ledger.writeFailedUpdateOperation(home,old);
          const cause=await ledger.readUpdateOperationCause(home,id);
          await ledger.publishUpdateOperationSettlement(home,{
            schema:1,operationId:id,status:"failed",settlement:"no-effect",basis:"launcher-recorded",...digests,
            evidence:{phaseAtFailure:"accepted",nativeReason:"launcher-restarted",candidateStarted:false,
              activeVersion:old.fromVersion,activeSourceSha:old.qualified.binding.activeSourceSha,
              recoveryPayload:"absent",restorePending:false,cause:cause.message||cause.code,
              writerLauncherVersion:"1.0.0",recordedAt:new Date().toISOString()},
          });
          state.update={id,fromVersion:old.fromVersion,targetVersion:old.targetVersion,qualified:old.qualified,
            status:"failed",reason:"launcher-restarted"};
          await write(statePath,state);
        }
        const active=path.join(home,"runtime","versions",state.activeVersion,"t3");
        const child=spawn(active,["serve"],{env:{...process.env,T3_SERVICE_LAUNCHER_CONTEXT:JSON.stringify({
          protocol:4,childVersion:state.activeVersion,launcherVersion:"1.0.0",startupGateProtocol:1,
        })},stdio:["inherit","inherit","inherit","ipc"]});
        let dispatching=false,ending=false,postWriteCleanupPending=false;
        const stopChild=()=>new Promise(resolve=>{if(child.exitCode!==null||child.signalCode!==null){resolve();return}
          child.once("close",resolve);child.kill("SIGTERM")});
        process.once("SIGTERM",()=>{if(postWriteCleanupPending){console.error("stopped during native post-write completion");process.exitCode=1}
          ending=true;stopChild()});
        child.on("message",m=>{
          if(m.type!=="request-update")return;
          dispatching=true;
          (async()=>{
            const qualified=await runtime.verifyStagedQualifiedRuntime(home,state.activeVersion,m.stagedHandle);
            await ledger.assertNoUnreconciledUpdateOperations(home);
            await ledger.reserveUpdateOperation(home,m.operationId,ledger.operationBinding(qualified));
            const pending={id:m.operationId,fromVersion:state.activeVersion,targetVersion:m.targetVersion,
              dbPath:m.dbPath,qualified,status:"pending",phase:"accepted"};
            const wasFailed=state.update?.status==="failed";
            state.update=pending;await write(statePath,state);
            await new Promise((resolve,reject)=>child.send({type:"update-accepted",updateId:m.operationId},e=>e?reject(e):resolve()));
            await new Promise(resolve=>setTimeout(resolve,100));
            ending=true;await stopChild();
            let step="reverify";
            try{
              await runtime.verifyStagedQualifiedRuntime(home,state.activeVersion,m.stagedHandle);
              step="ownership";
              await withStoppedServerOwnership(home,async()=>{
                step="backup";const dir=path.join(home,"runtime","db-backup",m.operationId+".staging");
                await fs.mkdir(dir,{recursive:true,mode:0o700});
                await fs.rmdir(dir);
              });
            }catch(e){
              await ledger.writeUpdateOperationCause(home,{schema:1,operationId:m.operationId,step,
                code:typeof e.code==="string"?e.code:"qualified-"+step+"-failed",message:e.message});
              process.exitCode=1;return;
            }
            if(!wasFailed)throw Error("expected injected first-attempt fault");
            const terminalBackup=path.join(home,"runtime","db-backup",m.operationId);
            await fs.mkdir(terminalBackup,{recursive:true,mode:0o700});
            await write(path.join(terminalBackup,"paired-binding.json"),{transactionId:m.operationId,qualified});
            await fs.writeFile(path.join(terminalBackup,"database"),"synthetic retained backup",{mode:0o600});
            postWriteCleanupPending=true;
            state.activeVersion=m.targetVersion;state.update={...pending,status:"committed"};await write(statePath,state);
            await new Promise(resolve=>setImmediate(resolve));
            await fs.unlink(path.join(terminalBackup,"database"));postWriteCleanupPending=false;
            const cleanupDir=await fs.open(terminalBackup,"r");try{await cleanupDir.sync()}finally{await cleanupDir.close()}
          })().catch(e=>{console.error(e.stack);process.exitCode=1;ending=true;stopChild()});
        });
        child.once("error",e=>{console.error(e.message);process.exitCode=1});
        child.once("close",()=>{if(!dispatching&&!ending)process.exitCode=1});
        await write(path.join(home,"runtime","jones-launcher-capability.json"),{
          launcherPid:process.pid,childPid:child.pid,startupGateProtocol:1,
        });
      })().catch(e=>{console.error(e.stack);process.exitCode=1});
    `,
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
      NodeAssert.equal(built.status, 0, built.stderr);
      NodeAssert.equal(built.signal, null);
      await NodeFSP.chmod(executable, 0o700);
      const payloadSha256 = await qualifiedPayloadDigest(payload);
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
      for (const step of ["reverify", "backup", "ownership"]) {
        let caseFailure;
        await t.test(
          step + " settles the failed ID then commits a distinct fresh Install",
          async () => {
            try {
              const root = NodePath.join(parent, "jones-packaged-recovery-" + step);
              await NodeFSP.mkdir(root, { mode: 0o700 });
              const prepared = await seedPackagedRecoveryFixture({
                root,
                active: { payloadDirectory: payload, receipt: receipt(true) },
                candidate: { payloadDirectory: payload, receipt: receipt(false) },
                kind: "staged",
                recoveryPayload: false,
                operationId: "12345678-1234-4234-8234-123456789abc",
              });
              const result = await exercisePackagedAcceptedFailure({
                prepared,
                step,
                freshOperationId: "42345678-1234-4234-8234-123456789abc",
                token: "synthetic-accepted-fixture-token",
              });
              NodeAssert.equal(result.assertionsPassed, true);
              NodeAssert.equal(result.step, step);
              NodeAssert.equal(result.packagedRecoveryMatrixPassed, false);
              NodeAssert.equal(result.externalDescendantSettlementRequired, true);
              NodeAssert.equal(result.committed.update.id, result.freshOperationId);
              NodeAssert.equal(result.settlement.basis, "launcher-recorded");
              NodeAssert.equal(result.acceptance.receipt.sendCallbackCompleted, true);
              await NodeFSP.rm(root, { recursive: true });
              await NodeAssert.rejects(NodeFSP.lstat(root), { code: "ENOENT" });
            } catch (error) {
              caseFailure = error;
              throw error;
            }
          },
        );
        if (caseFailure) throw caseFailure;
      }
    } catch (error) {
      failure = error;
      throw error;
    } finally {
      if (!failure?.preserveScratch) {
        await NodeFSP.rm(parent, { recursive: true });
        await NodeAssert.rejects(NodeFSP.lstat(parent), { code: "ENOENT" });
      }
    }
  },
);
