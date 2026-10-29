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
        const statePath=path.join(home,"runtime","service-state.json");
        let state;
        try{
          state=JSON.parse(await fs.readFile(statePath,"utf8"));
          if(state.protocol!==4 || !state.update) throw new Error("Synthetic launcher refuses invalid service state.");
        }catch(error){console.error(error.message);process.exitCode=1;return}
        const {writeFailedUpdateOperation,readUpdateOperationSettlement,publishUpdateOperationSettlement}=load(${JSON.stringify(NodeURL.fileURLToPath(ledger))});
        const {removeQualifiedRecoveryPayload}=load(${JSON.stringify(NodeURL.fileURLToPath(backup))});
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
