#!/usr/bin/env node
'use strict';

async function main(argv = process.argv.slice(2)) {
  if (argv[0] === 'worker') return require('./transport.cjs').workerMain(argv.slice(1));
  if (argv[0] === 'fleet') return require('./fleet.cjs').main(argv.slice(1));
  if (argv.length === 0 || argv[0] === '--help') return {
    name: 'server-swap',
    usage: ['server-swap plan|apply|status|resume|reconcile /absolute/plan.json', 'server-swap worker launch|status /absolute/plan.json', 'server-swap fleet plan|apply|status|resume|reconcile /absolute/fleet.json'],
    requirements: 'Exact staged runtime/config pair, external compatibility qualification, independent host worker, Node >=24 when running source. Elevation requires explicit systemd-system sudo-noninteractive selection; no credential prompts.',
  };
  if (argv.length !== 2 || !['plan', 'apply', 'status', 'resume', 'reconcile'].includes(argv[0])) throw new Error('Expected plan|apply|status|resume|reconcile and one absolute plan path; use --help');
  return require('./core.cjs')[argv[0]](argv[1]);
}
if (require.main === module) main().then((result) => {
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  if (['unknown', 'recovery-required', 'failed-before-start'].includes(result?.status)) process.exitCode = 2;
}, (error) => { process.stderr.write(`${JSON.stringify({ status: 'error', error: error.message })}\n`); process.exitCode = 1; });
module.exports = { main };
