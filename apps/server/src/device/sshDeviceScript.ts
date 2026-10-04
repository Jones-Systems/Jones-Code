import { directDeviceGatewaySource } from "./directDeviceGateway.ts";
import { deviceToolMaintenanceScript } from "./deviceToolMaintenance.ts";
import { AGENT_DEVICE_VERSION, DEVICE_HUB_VERSION } from "./DeviceToolchain.ts";

export const quoteRemoteArg = (value: string) => `'${value.replaceAll("'", "'\"'\"'")}'`;

/** Resolve common non-interactive SDK and Node locations without sourcing user shell scripts. */
export const remoteDeviceEnvironment = `export PATH="$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"
if [ -z "$ANDROID_HOME" ]; then
  if [ -d "$HOME/Library/Android/sdk" ]; then export ANDROID_HOME="$HOME/Library/Android/sdk";
  elif [ -d "$HOME/Android/Sdk" ]; then export ANDROID_HOME="$HOME/Android/Sdk"; fi
fi
if [ -n "$ANDROID_HOME" ]; then export PATH="$ANDROID_HOME/platform-tools:$ANDROID_HOME/emulator:$PATH"; fi
if [ -z "$JAVA_HOME" ] && ! command -v java >/dev/null 2>&1; then
  for device_java_home in "$HOME/.local/opt/android-studio/jbr" /opt/android-studio/jbr /Applications/Android\\ Studio.app/Contents/jbr "$HOME/Applications/Android Studio.app/Contents/jbr"; do
    if [ -x "$device_java_home/bin/java" ]; then export JAVA_HOME="$device_java_home"; break; fi
  done
fi
if [ -n "$JAVA_HOME" ]; then export PATH="$JAVA_HOME/bin:$PATH"; fi
`;

/** Node runs this on the host. All paths it returns belong to that host. */
export const remoteDeviceScript = (
  owner: string,
  mode: "probe" | "start" | "agent-start" | "stop-agent" | "stop" | "stop-direct",
  direct?: { readonly hostId: string; readonly generation: string },
) =>
  `
const owner = ${JSON.stringify(owner)};
const mode = ${JSON.stringify(mode)};
const hubVersion = ${JSON.stringify(DEVICE_HUB_VERSION)};
const agentVersion = ${JSON.stringify(AGENT_DEVICE_VERSION)};
const direct = ${JSON.stringify(direct ?? null)};
const gatewaySource = ${JSON.stringify(directDeviceGatewaySource)};
` +
  deviceToolMaintenanceScript +
  String.raw`
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const net = require('node:net');
const { spawn, spawnSync } = require('node:child_process');
const root = path.join(os.homedir(), '.t3', 'device');
const state = path.join(root, 'hosts', owner);
const run = (command, args, options = {}) => spawnSync(command, args, { encoding: 'utf8', timeout: 30000, ...options });
const read = (file) => { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return null; } };
const write = (file, value) => { const tmp = file + '.' + process.pid; fs.writeFileSync(tmp, JSON.stringify(value), { mode: 0o600 }); fs.renameSync(tmp, file); };
const toolVersions = (name, requiredVersion, entry, record) => {
  const directory = path.join(root, 'tools');
  const prefix = name + '@';
  let names = [];
  try { names = fs.readdirSync(directory); } catch (error) { if (error.code !== 'ENOENT') return null; }
  let unreadable = false;
  const installedVersions = names.filter(name => name.startsWith(prefix)).map(name => name.slice(prefix.length)).filter(version => {
    if (!/^[0-9]+\.[0-9]+\.[0-9]+(?:-[a-zA-Z0-9.-]+)?$/.test(version)) return false;
    const dir = path.join(directory, prefix + version);
    try { return fs.readFileSync(path.join(dir, '.install-complete'), 'utf8').trim() === version && fs.existsSync(path.join(dir, 'node_modules', name, entry)); } catch (error) { if (error.code !== 'ENOENT') unreadable = true; return false; }
  }).sort();
  if (unreadable) return null;
  let runningVersion = null;
  if (record?.entryPath && record?.pid) {
    const command = run('ps', ['-p', String(record.pid), '-o', 'command=']).stdout || '';
    runningVersion = installedVersions.find(version => {
      const install = path.join(directory, prefix + version);
      return record.entryPath === path.join(install, 'node_modules', name, entry) && command.includes(install + path.sep);
    }) ?? null;
  }
  return { requiredVersion, installedVersions, runningVersion };
};
const versions = () => {
  const result = {
  hub: toolVersions('expo-device-hub', hubVersion, 'dist/server/cli.mjs', read(path.join(state, 'hub.json'))),
  agent: toolVersions('agent-device', agentVersion, 'bin/agent-device.mjs', { ...read(path.join(state, 'agent.json')), ...read(path.join(state, 'daemon.json')) }),
  };
  return result.hub && result.agent ? result : undefined;
};
const stopHub = hub => {
  if (!hub || hub.owner !== owner) return;
  const command = run('ps', ['-p', String(hub.pid), '-o', 'command=']).stdout || '';
  if (command.includes(hub.entryPath) && command.includes(String(hub.port))) {
    try { process.kill(hub.pid, 'SIGTERM'); } catch {}
  }
};
const readGateway = file => {
  let value;
  try { value = JSON.parse(fs.readFileSync(file, 'utf8')); } catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  if (!value || typeof value.owner !== 'string' || typeof value.generation !== 'string' || !/^[a-zA-Z0-9-]{1,128}$/.test(value.generation) ||
      !Number.isSafeInteger(value.pid) || value.pid <= 0 || value.entryPath !== path.join(state, 'direct-gateway-' + value.generation + '.cjs')) throw Error('Unconfirmed direct gateway record.');
  return value;
};
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const stopGateway = async gateway => {
  if (!gateway) return;
  if (gateway.owner !== owner || typeof gateway.generation !== 'string' || !/^[a-zA-Z0-9-]{1,128}$/.test(gateway.generation) ||
      !Number.isSafeInteger(gateway.pid) || gateway.pid <= 0 ||
      gateway.entryPath !== path.join(state, 'direct-gateway-' + gateway.generation + '.cjs')) throw Error('Unconfirmed direct gateway identity.');
  let source;
  try { source = fs.readFileSync(gateway.entryPath, 'utf8'); } catch (error) { if (error.code !== 'ENOENT') throw error; }
  if (source !== undefined) {
    const first = /^const config = (.+);/.exec(source)?.[1];
    const captured = first ? JSON.parse(first) : null;
    if (captured?.owner !== gateway.owner || captured?.generation !== gateway.generation) throw Error('Unconfirmed direct gateway script.');
  }
  const inspect = () => {
    const result = run('ps', ['-p', String(gateway.pid), '-o', 'stat=', '-o', 'command=']);
    if (result.error || (result.status !== 0 && result.status !== 1)) throw Error('Cannot inspect direct gateway PID.');
    const output = (result.stdout || '').trim();
    if (!output) return false;
    const state = /^(\S+)\s+(.*)$/.exec(output);
    if (!state) throw Error('Cannot parse direct gateway PID identity.');
    if (state[1].startsWith('Z')) return false;
    const command = state[2];
    if (!source || !command.endsWith(' ' + gateway.entryPath)) throw Error('Direct gateway PID identity changed.');
    return true;
  };
  if (inspect()) {
    try { process.kill(gateway.pid, 'SIGTERM'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
    const deadline = Date.now() + 5000;
    while (inspect()) { if (Date.now() >= deadline) throw Error('Direct gateway termination is unconfirmed.'); await sleep(25); }
  }
  const file = path.join(state, 'direct-gateway.json');
  const current = readGateway(file);
  if (current && (current.owner !== gateway.owner || current.generation !== gateway.generation || current.pid !== gateway.pid || current.entryPath !== gateway.entryPath)) return;
  if (source !== undefined) {
    if (fs.readFileSync(gateway.entryPath, 'utf8') !== source) throw Error('Direct gateway script changed during retirement.');
    fs.unlinkSync(gateway.entryPath);
  }
  if (current) fs.unlinkSync(file);
};
const healthy = async (port, route) => { try { return (await fetch('http://127.0.0.1:' + port + route, { signal: AbortSignal.timeout(2000) })).ok; } catch { return false; } };
const port = () => new Promise((resolve, reject) => { const server = net.createServer(); server.once('error', reject); server.listen(0, '127.0.0.1', () => { const value = server.address().port; server.close(() => resolve(value)); }); });
async function acquireLock(lock, complete = () => false) {
  const deadline = Date.now() + 600000;
  const token = process.pid + ':' + require('node:crypto').randomUUID();
  const owner = () => { try { return fs.readlinkSync(lock); } catch (error) { if (error.code === 'ENOENT') return null; throw error; } };
  while (true) {
    try {
      // Publishing the PID and token is atomic; suspension cannot leave an incomplete owner.
      fs.symlinkSync(token, lock);
      return () => { if (owner() === token) fs.unlinkSync(lock); };
    } catch (error) {
      if (error.code !== 'EEXIST') throw error;
      if (complete()) return null;
      const previous = owner();
      if (previous === null) continue;
      const pid = Number(previous.split(':')[0]);
      if (!Number.isSafeInteger(pid) || pid <= 0) throw Error('Invalid device lock at ' + lock);
      try { process.kill(pid, 0); } catch (error) {
        if (error.code === 'ESRCH' && owner() === previous) {
          try { fs.unlinkSync(lock); } catch (error) { if (error.code !== 'ENOENT') throw error; }
          continue;
        }
      }
      if (Date.now() > deadline) throw Error('Device operation is locked at ' + lock + '. Check the other installer before removing the lock.');
      await sleep(500);
    }
  }
}
async function install(name, version, entry) {
  const dir = path.join(root, 'tools', name + '@' + version);
  const file = path.join(dir, 'node_modules', name, entry);
  const complete = () => fs.existsSync(file) && fs.existsSync(path.join(dir, '.install-complete')) && fs.readFileSync(path.join(dir, '.install-complete'), 'utf8').trim() === version;
  if (complete()) return file;
  fs.mkdirSync(path.dirname(dir), { recursive: true });
  const lock = dir + '.lock';
  const release = await acquireLock(lock, complete);
  if (!release) return file;
  let staging;
  try {
    if (complete()) return file;
    staging = fs.mkdtempSync(path.join(path.dirname(dir), '.install-'));
    const result = run('npm', ['install', '--prefix', staging, '--no-fund', '--no-audit', name + '@' + version], { timeout: 600000, maxBuffer: 8 * 1024 * 1024 });
    if (result.status !== 0) throw Error('Installing ' + name + ': ' + (result.error?.message || result.stderr?.slice(-2000)));
    if (!fs.existsSync(path.join(staging, 'node_modules', name, entry))) throw Error('Missing installed entry for ' + name);
    fs.writeFileSync(path.join(staging, '.install-complete'), version);
    fs.rmSync(dir, { recursive: true, force: true });
    fs.renameSync(staging, dir);
    return file;
  } finally {
    if (staging) fs.rmSync(staging, { recursive: true, force: true });
    release();
  }
}
(async () => {
  const ios = process.platform === 'darwin' && run('xcrun', ['simctl', 'help']).status === 0;
  const android = run('adb', ['version']).status === 0;
  const platforms = [
    { platform: 'ios', available: ios, ...(!ios ? { reason: 'iOS needs macOS with Xcode and working xcrun simctl.' } : {}) },
    { platform: 'android', available: android, ...(!android ? { reason: 'Android SDK missing. Set ANDROID_HOME or put adb on the SSH PATH.' } : {}) },
  ];
  if (mode === 'probe') {
    if (Number(process.versions.node.split('.')[0]) < 22) throw Error('Node 22 or newer is required on the device host.');
    if (run('npm', ['--version']).status !== 0) throw Error('npm is missing from the non-interactive SSH PATH.');
    console.log(JSON.stringify({ nodePath: process.execPath, platforms, tools: versions() })); return;
  }
  fs.mkdirSync(state, { recursive: true, mode: 0o700 });
  // Serialize starts and stops for this environment/host owner, including agent startup.
  const hostLock = path.join(state, 'runtime.lock');
  const releaseHost = await acquireLock(hostLock);
  try {
  const hubFile = path.join(state, 'hub.json');
  const daemonFile = path.join(state, 'daemon.json');
  const agentFile = path.join(state, 'agent.json');
  const gatewayFile = path.join(state, 'direct-gateway.json');
  if (mode === 'stop-direct') {
    if (!direct) throw Error('Missing captured direct generation.');
    const gateway = readGateway(gatewayFile);
    if (gateway?.owner === owner && gateway.generation === direct.generation) await stopGateway(gateway);
    else if (!gateway && fs.existsSync(path.join(state, 'direct-gateway-' + direct.generation + '.cjs'))) throw Error('Direct gateway startup effect is unconfirmed.');
    return;
  }
  if (mode === 'stop' || mode === 'stop-agent') {
    const hub = read(hubFile);
    if (mode === 'stop') {
      const gateway = readGateway(gatewayFile);
      if (direct && gateway?.owner === owner && gateway.generation === direct.generation) await stopGateway(gateway);
    }
    if (mode === 'stop' && hub && hub.owner === owner) {
      stopHub(hub);
      fs.rmSync(hubFile, { force: true });
    }
    const entry = read(agentFile)?.entryPath || path.join(root, 'tools', 'agent-device@' + agentVersion, 'node_modules', 'agent-device', 'bin', 'agent-device.mjs');
    if (fs.existsSync(entry)) run(process.execPath, [entry, 'daemon', 'stop', '--state-dir', state]);
    return;
  }
  if (!ios && !android) throw Error(platforms.map(p => p.reason).join(' '));
  fs.mkdirSync(state, { recursive: true, mode: 0o700 });
  const hubEntry = await install('expo-device-hub', hubVersion, 'dist/server/cli.mjs');
  let hub = read(hubFile);
  if (!hub || hub.owner !== owner || hub.entryPath !== hubEntry || !await healthy(hub.port, '/readyz')) {
    stopHub(hub);
    for (let attempt = 0; attempt < 5; attempt++) {
      const hubPort = await port();
      const log = fs.openSync(path.join(state, 'hub.log'), 'a');
      const child = spawn(process.execPath, [hubEntry, '--port', String(hubPort), '--host', '127.0.0.1', '--hide-sidebar', '--hide-boot-device'], {
        cwd: state, detached: true, stdio: ['ignore', log, log], env: { ...process.env, FORCE_COLOR: '0', NO_COLOR: '1' },
      });
      try { await new Promise((resolve, reject) => { child.once('spawn', resolve); child.once('error', reject); }); }
      finally { fs.closeSync(log); }
      child.unref();
      hub = { owner, pid: child.pid, port: hubPort, entryPath: hubEntry };
      write(hubFile, hub);
      const deadline = Date.now() + 30000;
      let listening = false;
      while (child.exitCode === null && child.signalCode === null) {
        if (await healthy(hub.port, '/readyz')) { listening = true; break; }
        if (Date.now() > deadline) { stopHub(hub); throw Error('Device hub did not become ready. See ' + path.join(state, 'hub.log')); }
        await sleep(200);
      }
      if (listening) break;
      // Port reservation and binding happen in different processes. Retry an early exit with a fresh port.
      fs.rmSync(hubFile, { force: true });
      if (attempt === 4) throw Error('Device hub exited before becoming ready. See ' + path.join(state, 'hub.log'));
    }
  }
  let directResult = {};
  const previousGateway = readGateway(gatewayFile);
  if (direct) {
    if (previousGateway && previousGateway.owner !== owner) throw Error('Direct gateway ownership mismatch.');
    await stopGateway(previousGateway);
    if (!/^[a-zA-Z0-9-]{1,128}$/.test(direct.generation)) throw Error('Invalid direct generation.');
    const admissionPort = await port();
    const entryPath = path.join(state, 'direct-gateway-' + direct.generation + '.cjs');
    const config = { hostId: direct.hostId, owner, generation: direct.generation, hubPort: hub.port, hubEntry, admissionPort };
    fs.writeFileSync(entryPath, 'const config = ' + JSON.stringify(config) + ';\n' + gatewaySource, { mode: 0o600 });
    let log;
    let captured;
    try {
      log = fs.openSync(path.join(state, 'direct-gateway.log'), 'a');
      const child = spawn(process.execPath, [entryPath], { cwd: state, detached: true, stdio: ['ignore', log, log, 'ipc'] });
      if (child.pid) { captured = { owner, generation: direct.generation, pid: child.pid, entryPath }; write(gatewayFile, captured); }
      await new Promise((resolve, reject) => { child.once('spawn', resolve); child.once('error', reject); });
      const gatewayPort = await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(Error('Direct gateway startup timed out.')), 10000);
        const finish = (error, value) => { clearTimeout(timer); child.removeAllListeners('exit'); child.removeAllListeners('message'); child.removeAllListeners('error'); error ? reject(error) : resolve(value); };
        child.once('message', message => Number.isInteger(message?.port) && message.port > 0 && message.port < 65536 ? finish(null, message.port) : finish(Error('Invalid direct gateway port.')));
        child.once('exit', () => finish(Error('Direct gateway exited before becoming ready.')));
        child.once('error', error => finish(error));
      });
      write(gatewayFile, { ...captured, port: gatewayPort, admissionPort });
      child.unref();
      directResult = { directMedia: { gatewayPort, admissionPort, generation: direct.generation } };
    } catch (error) {
      if (captured) await stopGateway(captured);
      else fs.unlinkSync(entryPath);
      throw error;
    } finally { if (log !== undefined) fs.closeSync(log); }
  }
  let agentResult = {};
  if (mode === 'agent-start') {
  const agentEntry = await install('agent-device', agentVersion, 'bin/agent-device.mjs');
  const previousAgent = read(agentFile)?.entryPath;
  let daemon = read(daemonFile);
  if (daemon && (previousAgent !== agentEntry || !await healthy(daemon.httpPort, '/health'))) {
    const stopped = run(process.execPath, [previousAgent || agentEntry, 'daemon', 'stop', '--state-dir', state]);
    if (stopped.status !== 0) throw Error('Could not stop the previous agent-device version.');
    fs.rmSync(daemonFile, { force: true });
    daemon = null;
  }
  if (!daemon) {
    fs.rmSync(daemonFile, { force: true });
    const env = { ...process.env, AGENT_DEVICE_STATE_DIR: state, AGENT_DEVICE_DAEMON_SERVER_MODE: 'http', AGENT_DEVICE_DAEMON_IDLE_TIMEOUT_MS: '0', AGENT_DEVICE_NO_UPDATE_NOTIFIER: '1' };
    delete env.AGENT_DEVICE_DAEMON_BASE_URL; delete env.AGENT_DEVICE_DAEMON_AUTH_TOKEN; delete env.AGENT_DEVICE_CONFIG;
    run(process.execPath, [agentEntry, 'devices', '--json'], { env });
    daemon = read(daemonFile);
  }
  if (!daemon || !await healthy(daemon.httpPort, '/health')) throw Error('agent-device daemon did not become ready in ' + state);
  write(agentFile, { entryPath: agentEntry });
  agentResult = { daemonPort: daemon.httpPort, token: daemon.token, entryPath: agentEntry };
  }
  const vendor = path.resolve(path.dirname(hubEntry), '../../vendor/serve-sim/dist');
  const optional = file => fs.existsSync(file) ? file : null;
  await pruneTools(path.join(root, 'tools'), [['expo-device-hub', hubVersion], ...(mode === 'agent-start' ? [['agent-device', agentVersion]] : [])], true).catch(() => {});
  console.log(JSON.stringify({ nodePath: process.execPath, platforms, tools: versions(), hubPort: hub.port, ...agentResult, ...directResult,
    helpers: { serveSimAxSettings: optional(path.join(vendor, 'simax/serve-sim-ax-settings')), serveSimCli: optional(path.join(vendor, 'serve-sim.js')) } }));
  } finally { releaseHost(); }
})().catch(error => { console.error(error.message); process.exitCode = 1; });
`;
