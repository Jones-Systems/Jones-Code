'use strict';

const fs = require('node:fs');
const crypto = require('node:crypto');
const { remote, remoteArgs } = require('./transport.cjs');

function loadFleet(file) {
  const value = JSON.parse(fs.readFileSync(file, 'utf8'));
  if (value.schema !== 'server-swap-fleet/v1' || !Array.isArray(value.targets) || !value.targets.length || value.targets.length > 32) {
    throw new Error('Expected server-swap-fleet/v1 with 1–32 ordered targets');
  }
  const ids = new Set();
  const bindings = new Set();
  for (const target of value.targets) {
    if (typeof target.id !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(target.id) || ids.has(target.id)) {
      throw new Error('Each fleet target needs a unique id');
    }
    remoteArgs(target, 'status');
    if (typeof target.planDigest !== 'string' || !/^[a-f0-9]{64}$/.test(target.planDigest)) {
      throw new Error('Each target must pin the SHA-256 planDigest returned by plan');
    }
    const binding = `${target.ssh}\0${target.plan}`;
    if (bindings.has(binding)) throw new Error('Duplicate host/plan binding');
    ids.add(target.id); bindings.add(binding);
  }
  return value;
}

async function runFleet(manifest, operation, invoke = remote) {
  if (!['plan', 'apply', 'status', 'resume', 'reconcile'].includes(operation)) throw new Error('Unsupported fleet command');
  const results = [];
  for (const target of manifest.targets) {
    let observation = await invoke(target, ['plan', 'reconcile'].includes(operation) ? operation : 'status');
    if (observation.planDigest !== target.planDigest) {
      results.push({ id: target.id, observation: { status: 'unknown', reason: 'Missing or changed host plan digest' } });
      break;
    }
    if (['apply', 'resume', 'status'].includes(operation) && ['committed', 'committed-unverified'].includes(observation.status)) {
      observation = await invoke(target, 'reconcile');
      if (observation.planDigest !== target.planDigest) observation = { status: 'unknown', reason: 'Reconciliation plan digest mismatch' };
    }
    const state = observation.status;
    if (['apply', 'resume'].includes(operation) && state === 'not-started') {
      observation = await invoke(target, 'launch');
      if (observation.planDigest !== target.planDigest) observation = { status: 'unknown', reason: 'Launch receipt plan digest mismatch' };
    }
    results.push({ id: target.id, observation });
    // Launch is durably claimed once on the host, including across lost SSH responses.
    if (!['status', 'plan'].includes(operation) && observation.status !== 'committed') break;
  }
  const complete = results.length === manifest.targets.length && results.every(r => r.observation.status === 'committed');
  const ready = operation === 'plan' && results.length === manifest.targets.length && results.every(r => r.observation.status === 'ready');
  return { status: complete ? 'committed' : ready ? 'ready' : 'pending', targets: results,
    remaining: manifest.targets.slice(results.length).map(t => t.id),
    laptop: complete ? 'Remote swaps committed. Switch the laptop application manually and verify Connections.' : 'Keep the laptop switch pending.' };
}

async function main(argv) {
  if (argv.length !== 2) throw new Error('usage: fleet plan|apply|status|resume|reconcile fleet.json');
  const manifest = loadFleet(argv[1]);
  const result = await runFleet(manifest, argv[0]);
  return { ...result, fleetDigest: crypto.createHash('sha256').update(JSON.stringify(manifest)).digest('hex') };
}

module.exports = { loadFleet, runFleet, main };
