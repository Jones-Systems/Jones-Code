import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeModule from "node:module";
import * as NodeURL from "node:url";
import * as NodeCrypto from "node:crypto";

// Importing this module never compiles, loads FFI, creates scratch, or starts a child.
export function coldFixtureFailure(stderr, limit = 4096) {
  return {
    stderrPresent: stderr.length > 0,
    stderrTruncated: stderr.length > limit,
    stderrClass: stderr.includes("COLD_FIXTURE_FAILURE")
      ? "fixture-rejected"
      : stderr.length
        ? "redacted"
        : "empty",
  };
}

const retainedOwners = new Set();

export function coldFixtureEnvironment(root) {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- This standalone fixture checks the actual host before creating resources, without loading the application runtime.
  if (process.platform !== "darwin" || typeof process.getuid !== "function")
    throw new Error("Fixture requires a Darwin real UID.");
  const uid = process.getuid();
  if (!Number.isInteger(uid) || uid < 0 || uid > 0xffffffff)
    throw new Error("Invalid fixture real UID.");
  const home = NodePath.join(root, "home");
  return Object.freeze({
    PATH: "/usr/bin:/bin",
    LANG: "C",
    LC_ALL: "C",
    HOME: home,
    // Bound historical CF fallback inputs; this does not prove installed profile-read behavior.
    __CF_USER_TEXT_ENCODING: `0x${uid.toString(16).toUpperCase()}:0:0`,
    CFFIXED_USER_HOME: home,
    XDG_CONFIG_HOME: NodePath.join(root, "config"),
    XDG_CACHE_HOME: NodePath.join(root, "cache"),
    TMPDIR: root,
    CLANG_MODULE_CACHE_PATH: NodePath.join(root, "cache"),
    ELECTRON_RUN_AS_NODE: "1",
    BREAKPAD_DUMP_LOCATION: root,
  });
}

async function captured(
  command,
  args,
  cwd,
  environment,
  signal,
  familyOwner = false,
  cancelOnReady = false,
) {
  // Retain the actual constructor result before installing any asynchronous work.
  if (signal.aborted) throw new Error("Fixture cancelled before spawn.");
  const child = NodeChildProcess.spawn(command, args, {
    cwd,
    env: environment,
    stdio: ["ignore", "pipe", "pipe"],
  });
  const owner = { child, keeper: setInterval(() => {}, 1000) };
  retainedOwners.add(owner);
  const result = {
    code: null,
    signal: null,
    exitObserved: false,
    closeObserved: false,
    stdoutEof: false,
    stderrEof: false,
    spawnError: false,
    timedOut: false,
    outputExceeded: false,
    streamError: false,
    cancelled: false,
    heldKeeper: false,
  };
  let stdout = "",
    stderr = "",
    total = 0;
  child.stdout.on("data", (chunk) => {
    total += chunk.length;
    if (total <= 16384) stdout += chunk.toString();
    else result.outputExceeded = true;
    if (cancelOnReady && stdout.includes('{"controlReady":true}\n')) {
      cancelOnReady = false;
      // This separately admitted control delivers a real signal to its own outer owner.
      process.kill(process.pid, "SIGTERM");
    }
  });
  child.stderr.on("data", (chunk) => {
    if (stderr.length <= 4096) stderr += chunk.toString().slice(0, 4097);
  });
  child.stdout.on("end", () => {
    result.stdoutEof = true;
  });
  child.stderr.on("end", () => {
    result.stderrEof = true;
  });
  child.stdout.on("error", () => {
    result.streamError = true;
  });
  child.stderr.on("error", () => {
    result.streamError = true;
  });
  child.on("exit", (code, exitSignal) => {
    Object.assign(result, { code, signal: exitSignal, exitObserved: true });
  });
  child.on("error", () => {
    result.spawnError = true;
  });
  const requestStop = () => {
    result.cancelled ||= signal.aborted;
    // Python owns a descendant. SIGTERM requests its finalizer; never kill that owner first.
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
  };
  signal.addEventListener("abort", requestStop);
  await new Promise((resolve) => {
    const deadline = setTimeout(() => {
      result.timedOut = true;
      requestStop();
    }, 20000);
    const force = setTimeout(() => {
      if (!familyOwner && !result.exitObserved) child.kill("SIGKILL");
    }, 22000);
    const unknown = setTimeout(() => {
      finish();
    }, 25000);
    const finish = () => {
      clearTimeout(deadline);
      clearTimeout(force);
      clearTimeout(unknown);
      resolve();
    };
    child.once("close", () => {
      result.closeObserved = true;
      finish();
    });
    if (signal.aborted) requestStop();
  });
  const terminal =
    result.exitObserved &&
    result.closeObserved &&
    result.stdoutEof &&
    result.stderrEof &&
    !result.streamError;
  if (terminal && !familyOwner) {
    clearInterval(owner.keeper);
    signal.removeEventListener("abort", requestStop);
    retainedOwners.delete(owner);
  } else {
    result.heldKeeper = !terminal;
    // A receipt cannot replace custody. Keep this object and the outer owner alive on uncertainty.
  }
  const releaseFamily = () => {
    if (!familyOwner || !terminal) throw new Error("Family owner closure is unknown.");
    clearInterval(owner.keeper);
    signal.removeEventListener("abort", requestStop);
    retainedOwners.delete(owner);
  };
  return { ...result, ...coldFixtureFailure(stderr), stdout, releaseFamily };
}

const parentSource = String.raw`
import os, sys, socket, fcntl, subprocess, time, json, signal
root, executable, scenario = sys.argv[1:]
stop_requested = False
p = None
lock = left = right = None
streams = {}
data = {'stdout':bytearray(), 'stderr':bytearray()}
counts = {'stdout':0,'stderr':0}
eof = {'stdout':False,'stderr':False}
stream_failure = False
phase = 'prepare'

def request_stop(signum, frame):
  global stop_requested
  stop_requested = True

# Install cancellation handling before Popen can create a descendant.
signal.signal(signal.SIGTERM, request_stop)
signal.signal(signal.SIGINT, request_stop)

def drain():
  global stream_failure
  for name, stream in list(streams.items()):
    if eof[name]: continue
    try:
      os.set_blocking(stream.fileno(), False)
    except BaseException:
      stream_failure = True
      continue
    try:
      chunk = os.read(stream.fileno(), 4096)
      if not chunk:
        eof[name] = True
        stream.close()
      else:
        counts[name] += len(chunk)
        if len(data[name]) < 4096: data[name].extend(chunk[:4096-len(data[name])])
    except BlockingIOError: pass
    except BaseException: stream_failure = True

def finalize(force_stop):
  deadline = time.monotonic() + 4
  terminated = killed = False
  while True:
    drain()
    code = p.poll()
    if code is not None and all(eof.values()) and not stream_failure:
      p.wait(timeout=0)
      return True
    now = time.monotonic()
    if (force_stop or stop_requested) and code is None and not terminated:
      p.terminate(); terminated = True
    if now >= deadline - 2 and code is None and not killed:
      p.kill(); killed = True
    if now >= deadline: return False
    time.sleep(0.02)

def keep_unknown():
  # No process ID is adopted. The live Popen object and streams remain in this owner.
  print(json.dumps({'heldKeeper':True,'phase':phase}), flush=True)
  while True: time.sleep(1)

try:
  lock = open(os.path.join(root, 'lock'), 'x+b')
  os.chmod(lock.name, 0o600)
  fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
  left, right = socket.socketpair()
  assert (lock.fileno(), left.fileno(), right.fileno()) == (3,4,5)
  uid = os.getuid()
  if sys.platform != 'darwin' or not isinstance(uid, int) or not 0 <= uid <= 0xffffffff:
    raise RuntimeError('unsupported-real-uid')
  environment = {'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C','__CF_USER_TEXT_ENCODING':f'0x{uid:X}:0:0',
    'CFFIXED_USER_HOME':os.path.join(root,'home'),
    'HOME':os.path.join(root,'home'),'XDG_CONFIG_HOME':os.path.join(root,'config'),
    'XDG_CACHE_HOME':os.path.join(root,'cache'),'TMPDIR':root,
    'ELECTRON_RUN_AS_NODE':'1','BREAKPAD_DUMP_LOCATION':root}
  if stop_requested: raise RuntimeError('cancelled-before-spawn')
  phase = 'spawn'
  # Signals only set a flag, so the actual constructor result is assigned before cancellation acts.
  p = subprocess.Popen([executable, os.path.join(root,'actor.mjs')], cwd=root,
    env=environment, pass_fds=(3,4,5), stdin=subprocess.DEVNULL,
    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  streams = {'stdout':p.stdout,'stderr':p.stderr}
  for stream in streams.values(): os.set_blocking(stream.fileno(), False)
  phase = 'observe'
  if scenario == 'cancel': print('{"controlReady":true}', flush=True)
  deadline = time.monotonic() + (0.25 if scenario != 'native' else 12)
  if scenario == 'parent-exception': raise RuntimeError('owned-control-exception')
  while p.poll() is None or not all(eof.values()):
    drain()
    if stop_requested or stream_failure or time.monotonic() >= deadline: break
    time.sleep(0.02)
except BaseException:
  phase = 'failed'
finally:
  if p is not None:
    try:
      if not finalize(True): keep_unknown()
    except BaseException: keep_unknown()
  try:
    for resource in (left, right, lock):
      if resource is not None: resource.close()
  except BaseException: keep_unknown()

code = p.returncode if p is not None else None
result = {'code':code if code is not None and code>=0 else None,
  'signal':-code if code is not None and code<0 else None,
  'exitObserved':p is not None and code is not None,'reaped':p is not None and code is not None,
  'stdoutEof':eof['stdout'],'stderrEof':eof['stderr'],'ownEndpointsClosed':True,
  'phase':phase,'cancelled':stop_requested,'stderrPresent':counts['stderr']>0,
  'stderrTruncated':counts['stderr']>4096,'stdoutTruncated':counts['stdout']>4096}
if scenario == 'native' and code == 0 and counts['stdout'] <= 4096:
  try:
    candidate=json.loads(data['stdout'])
    runtime=candidate.pop('runtime')
    assert set(runtime)=={'node','electron','platform','architecture'}
    assert all(isinstance(x,str) and len(x)<=64 for x in runtime.values())
    allowed={'sdkLayoutMatches','decoderMatches','negativeComparisonsRejected','held','reciprocal','closeOnExec','scanComplete','noUnexpectedAliases','privateIdentityQualified','bootQualified'}
    assert set(candidate)==allowed and all(type(x)==bool for x in candidate.values())
    result['observation']=candidate; result['runtime']=runtime
  except BaseException: result['phase']='invalid-observation'
print(json.dumps(result),flush=True)
`;

const actorSource = String.raw`
import * as assert from 'node:assert/strict';
const oracleFfi = await import(FFI_SPECIFIER);
const T = oracleFfi.DataType;
oracleFfi.open({library:'cold-sdk-oracle',path:new URL('./oracle.dylib',import.meta.url).pathname});
const call=(name,types,values)=>oracleFfi.load({library:'cold-sdk-oracle',funcName:name,retType:T.I32,paramsType:types,paramsValue:values,runInNewThread:false,freeResultMemory:false});
const checked=(size,fn)=>{ const storage=Buffer.alloc(size+32,0xa5),bytes=storage.subarray(16,size+16);bytes.fill(0);try{const result=fn(bytes);assert.ok(storage.subarray(0,16).every(x=>x===0xa5)&&storage.subarray(size+16).every(x=>x===0xa5));return result;}finally{storage.fill(0);} };
try {
 const L=await import('./layout.mjs');
 checked(124,b=>{assert.equal(call('cold_oracle_layout',[T.U8Array,T.I32],[b,b.length]),0);
  const expected=[L.coldDarwinLayout.bsd,L.coldDarwinLayout.socket,L.coldDarwinLayout.vnode,L.coldDarwinLayout.fd,12,16,20,120,128,4,160,176,184,256,264,24,28,32,1,1,0x20006601,2,3,3,1,1,2,56,16,24,32];
  assert.deepEqual(Array.from({length:31},(_,i)=>b.readUInt32LE(i*4)),expected); });
 for(let kind=0;kind<7;kind++){
  const size=kind<2?136:kind<4?792:kind===4?176:56;
  checked(size,b=>{
   const count=call('cold_oracle_capture',[T.I32,T.U8Array,T.I32],[kind,b,b.length]);
   const value=kind<2?L.decodeColdDarwinBsd(b,count):kind<4?L.decodeColdDarwinSocket(b,count):kind===4?L.decodeColdDarwinVnode(b,count):L.decodeColdDarwinPrivateUnique(b,count);
   const values=kind<2?[value.pid,value.parentPid,value.uid,value.startSeconds,value.startMicroseconds]:kind<4?[value.endpoint,value.peer,value.streamUnix,value.nonzero,value.closeOnExec]:kind===4?[value.device,value.inode,value.regular]:[value.uniqueId,value.parentUniqueId,value.pidVersion];
   checked(values.length*8,decoded=>{values.forEach((v,i)=>decoded.writeBigUInt64LE(BigInt(v),i*8));assert.equal(call('cold_oracle_compare',[T.I32,T.U8Array,T.I32],[kind,decoded,decoded.length]),1);decoded[0]^=1;assert.equal(call('cold_oracle_compare',[T.I32,T.U8Array,T.I32],[kind,decoded,decoded.length]),0);});
   assert.throws(()=>L.decodeColdDarwinSocket(b,-1));
  });
 }
 for (const kind of [7,8]) checked(792,b=>{const n=call('cold_oracle_capture',[T.I32,T.U8Array,T.I32],[kind,b,b.length]);assert.throws(()=>L.decodeColdDarwinSocket(b,n));});
 const observation=await coldNative.observe();
 assert.equal(observation.supported,true);
 assert.equal(observation.unexpectedAliases,false);
 console.log(JSON.stringify({sdkLayoutMatches:true,decoderMatches:true,negativeComparisonsRejected:true,
  held:observation.held,reciprocal:observation.reciprocal,closeOnExec:observation.closeOnExec,
  scanComplete:observation.scanComplete,noUnexpectedAliases:!observation.unexpectedAliases,
  privateIdentityQualified:observation.privateIdentityQualified,bootQualified:observation.bootQualified,
  runtime:{node:process.versions.node,electron:process.versions.electron??'none',platform:process.platform,architecture:process.arch}}));
} catch { console.error('COLD_FIXTURE_FAILURE'); process.exitCode=1; }
finally { oracleFfi.close('cold-sdk-oracle'); }
`;

let fixtureActive = false;

async function ownedFixture(options, scenario) {
  if (fixtureActive || retainedOwners.size !== 0)
    throw new Error("Previous fixture ownership remains held.");
  fixtureActive = true;
  const cancellation = new AbortController();
  const requestStop = () => cancellation.abort();
  // Keep the outer owner alive on cancellation until its captured family is settled.
  process.on("SIGTERM", requestStop);
  process.on("SIGINT", requestStop);
  options.signal?.addEventListener("abort", requestStop);
  if (options.signal?.aborted) requestStop();
  try {
    return await runColdDarwinFixtureBody({ ...options, signal: cancellation.signal, scenario });
  } finally {
    if (retainedOwners.size === 0) {
      process.removeListener("SIGTERM", requestStop);
      process.removeListener("SIGINT", requestStop);
      options.signal?.removeEventListener("abort", requestStop);
      fixtureActive = false;
    }
  }
}

/** Explicit native entry; no import or pure test invokes it. */
export function runColdDarwinNativeFixture(options) {
  return ownedFixture(options, "native");
}

/** Prepared, separately admitted lifecycle controls. These actors never import FFI. */
export function runColdDarwinLifecycleControl(options) {
  if (!["eof-live", "nonzero", "signal", "parent-exception", "cancel"].includes(options.scenario)) {
    return Promise.reject(new Error("Unknown lifecycle control."));
  }
  return ownedFixture(options, options.scenario);
}

async function runColdDarwinFixtureBody({ root, runtime = "current-node", signal, scenario }) {
  if (
    !NodePath.isAbsolute(root) ||
    !/^cold-darwin-observer-[a-z0-9-]+$/.test(NodePath.basename(root))
  )
    throw new Error("Invalid owned fixture root.");
  const scratch = NodePath.join(root, "scratch");
  const environment = coldFixtureEnvironment(scratch);
  await NodeFSP.mkdir(root, { mode: 0o700 });
  await NodeFSP.mkdir(scratch, { mode: 0o700 });
  const receipt = {
    schema: 1,
    phase: "prepare",
    held: true,
    closureProved: true,
    cleanup: false,
    actors: [],
  };
  for (const name of ["home", "config", "cache"]) {
    await NodeFSP.mkdir(NodePath.join(scratch, name), { mode: 0o700 });
  }
  try {
    const desktop = new URL("../../../apps/desktop/package.json", import.meta.url);
    const metadata = JSON.parse(await NodeFSP.readFile(desktop, "utf8"));
    let executable;
    if (
      runtime === "current-node" &&
      ["24.13.1", "24.19.0"].includes(process.versions.node) &&
      !process.versions.electron
    ) {
      executable = process.execPath;
    } else if (runtime === "dependency-electron" && metadata.dependencies.electron === "44.4.2") {
      const entry = NodeModule.createRequire(desktop).resolve("electron");
      const electronMetadata = JSON.parse(
        await NodeFSP.readFile(NodePath.join(NodePath.dirname(entry), "package.json"), "utf8"),
      );
      if (electronMetadata.version !== "44.4.2") throw new Error("Unexpected dependency Electron.");
      executable = NodePath.join(
        NodePath.dirname(entry),
        "dist/Electron.app/Contents/MacOS/Electron",
      );
    } else throw new Error("Unsupported fixed fixture runtime.");
    if (!NodePath.isAbsolute(executable)) throw new Error("Missing fixed executable.");

    if (scenario === "native") {
      if (metadata.dependencies["ffi-rs"] !== "1.3.2")
        throw new Error("Unexpected desktop FFI dependency.");
      const ffi = NodeURL.pathToFileURL(NodeModule.createRequire(desktop).resolve("ffi-rs")).href;
      const ffiPath = NodeURL.fileURLToPath(ffi);
      const ffiMetadata = JSON.parse(
        await NodeFSP.readFile(NodePath.join(NodePath.dirname(ffiPath), "package.json"), "utf8"),
      );
      if (ffiMetadata.name !== "ffi-rs" || ffiMetadata.version !== "1.3.2")
        throw new Error("Unexpected resolved FFI package.");
      const addon = NodeModule.createRequire(ffiPath).resolve("@yuuang/ffi-rs-darwin-arm64");
      receipt.dependency = {
        name: "ffi-rs",
        version: "1.3.2",
        archiveBytesVerified: false,
        loaderSha256: NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(ffiPath))
          .digest("hex"),
        addonSha256: NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(addon))
          .digest("hex"),
      };
      const custody = await NodeFSP.readFile(
        new URL(
          "../../../apps/desktop/src/jones/updates/jonesColdBackendCustody.ts",
          import.meta.url,
        ),
        "utf8",
      );
      const layout = await NodeFSP.readFile(
        new URL(
          "../../../apps/desktop/src/jones/updates/jonesColdDarwinLayout.ts",
          import.meta.url,
        ),
        "utf8",
      );
      const oracle = await NodeFSP.readFile(
        new URL("./cold-darwin-sdk-oracle.c", import.meta.url),
        "utf8",
      );
      receipt.sourceSha256 = NodeCrypto.createHash("sha256")
        .update(custody)
        .update(layout)
        .update(oracle)
        .digest("hex");
      const actor =
        custody
          .replace('"./jonesColdDarwinLayout.ts"', '"./layout.mjs"')
          .replace('import("ffi-rs")', `import(${JSON.stringify(ffi)})`) +
        actorSource.replace("FFI_SPECIFIER", JSON.stringify(ffi));
      for (const [name, body] of [
        ["actor.mjs", NodeModule.stripTypeScriptTypes(actor)],
        ["layout.mjs", NodeModule.stripTypeScriptTypes(layout)],
        ["parent.py", parentSource],
        ["oracle.c", oracle],
      ])
        await NodeFSP.writeFile(NodePath.join(scratch, name), body, { mode: 0o600, flag: "wx" });
      receipt.phase = "compile";
      receipt.closureProved = false;
      const compiler = await captured(
        "/usr/bin/clang",
        [
          "-std=c11",
          "-Wall",
          "-Wextra",
          "-Werror",
          "-isysroot",
          "/Library/Developer/CommandLineTools/SDKs/MacOSX15.2.sdk",
          "-dynamiclib",
          "-o",
          "oracle.dylib",
          "oracle.c",
        ],
        scratch,
        environment,
        signal,
      );
      const { releaseFamily: _compilerRelease, ...compilerEvidence } = compiler;
      receipt.actors.push({ ...compilerEvidence, stdout: undefined });
      if (
        !compiler.exitObserved ||
        !compiler.closeObserved ||
        !compiler.stdoutEof ||
        !compiler.stderrEof ||
        compiler.streamError ||
        compiler.heldKeeper
      )
        throw new Error("Compiler closure unknown.");
      receipt.closureProved = true;
      if (compiler.code !== 0) throw new Error("Compiler rejected fixture.");
      await NodeFSP.chmod(NodePath.join(scratch, "oracle.dylib"), 0o700);
    } else {
      const controls = {
        "eof-live":
          "import * as fs from 'node:fs'; fs.closeSync(1); fs.closeSync(2); setInterval(()=>{},1000);",
        nonzero: "console.error('COLD_FIXTURE_FAILURE'); process.exitCode=7;",
        signal: "process.kill(process.pid,'SIGTERM');",
        "parent-exception": "setInterval(()=>{},1000);",
        cancel: "setInterval(()=>{},1000);",
      };
      await NodeFSP.writeFile(NodePath.join(scratch, "actor.mjs"), controls[scenario], {
        mode: 0o600,
        flag: "wx",
      });
      await NodeFSP.writeFile(NodePath.join(scratch, "parent.py"), parentSource, {
        mode: 0o600,
        flag: "wx",
      });
    }
    receipt.phase = scenario === "native" ? "native" : "lifecycle-control";
    receipt.closureProved = false;
    const parent = await captured(
      "/usr/bin/python3",
      ["-I", "-S", "-B", "parent.py", scratch, executable, scenario],
      scratch,
      environment,
      signal,
      true,
      scenario === "cancel",
    );
    const { releaseFamily, ...parentEvidence } = parent;
    receipt.actors.push({ ...parentEvidence, stdout: undefined });
    if (
      parent.code !== 0 ||
      !parent.exitObserved ||
      !parent.closeObserved ||
      !parent.stdoutEof ||
      !parent.stderrEof ||
      parent.outputExceeded ||
      parent.streamError ||
      parent.heldKeeper
    )
      throw new Error("Fixture family closure unknown.");
    const child = JSON.parse(parent.stdout.replace('{"controlReady":true}\n', ""));
    if (
      child.exitObserved !== true ||
      child.reaped !== true ||
      child.stdoutEof !== true ||
      child.stderrEof !== true ||
      child.ownEndpointsClosed !== true
    )
      throw new Error("Owned child closure unknown.");
    receipt.child = child;
    releaseFamily();
    receipt.closureProved = true;
    receipt.phase =
      scenario !== "native"
        ? "control-closed"
        : child.code === 0 && child.observation?.decoderMatches === true
          ? "observed"
          : "rejected";
  } catch {
    receipt.failure = "bounded-fixture-failure";
  }
  const bytes = JSON.stringify(receipt) + "\n";
  const evidence = NodePath.join(root, "receipt.json");
  const handle = await NodeFSP.open(evidence, "wx", 0o600);
  try {
    await handle.writeFile(bytes);
    await handle.sync();
  } finally {
    await handle.close();
  }
  const directory = await NodeFSP.open(root, "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
  if ((await NodeFSP.readFile(evidence, "utf8")) !== bytes)
    throw new Error("Evidence readback failed; scratch retained.");
  if (receipt.closureProved) {
    await NodeFSP.rm(scratch, { recursive: true });
    receipt.cleanup = true;
    const cleanup = await NodeFSP.open(NodePath.join(root, "cleanup.json"), "wx", 0o600);
    try {
      await cleanup.writeFile('{"scratchRemoved":true}\n');
      await cleanup.sync();
    } finally {
      await cleanup.close();
    }
    const parent = await NodeFSP.open(root, "r");
    try {
      await parent.sync();
    } finally {
      await parent.close();
    }
    if (
      (await NodeFSP.readFile(NodePath.join(root, "cleanup.json"), "utf8")) !==
      '{"scratchRemoved":true}\n'
    )
      throw new Error("Cleanup evidence readback failed.");
  }
  return receipt;
}
