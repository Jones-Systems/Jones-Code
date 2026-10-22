# Physical-host integration testing

This opt-in harness runs A/C fixture environments on one Linux host and a fresh
B worker on another. A third operator machine connects to both through existing
SSH aliases. The repository owns the driver, worker, relay and single-stage
operator; host configuration and execution receipts stay outside the checkout.

The test covers production peer linking, HTTP/MCP forwarding, dispatch,
idempotent replay, capability fences, offline refusal and durable readback.
Fixtures use synthetic metadata, disable provider/outbox workers, inject the
local caller context and substitute unused toolkit dependencies. This does not
qualify full CLI boot, normal workstream enrollment or real provider execution.
A/C are separate fixture scopes in one driver process; each repeat starts a
fresh B process and fresh roots on both hosts.

## Bind an execution

Use Linux driver and worker hosts with installed repository dependencies and a
supported Node 24 runtime. The operator needs Python 3.10+ and OpenSSH; each
remote host needs Python 3.10+, `/proc` and disk-backed task storage. Existing
SSH trust and authentication must already work. These scripts do not install
packages, edit SSH configuration, enroll credentials or start shared services.

Copy [the request example](physicalHost.request.example.json) into a fresh,
canonical, operator-owned directory outside the checkout. Replace every
placeholder. Bind existing SSH aliases, absolute executables, canonical remote
checkout paths and separate task-owned storage roots. Do not point task roots
at live application state. Choose unused loopback ports and a new `runId` for
each stage. Request files and receipts are created exclusively; a collision is
a stop requiring reconciliation, not permission to delete the old run.

`local` describes the driver checkout; `remote` describes B. For each, record
its exact Node version, source identity and SHA256 digests of all five required
files shown in the example. Additional exact source digests are supported.
A source identity may describe a verified commit plus an explicit overlay; do
not label a dirty checkout as a clean commit. The Node processes verify these
hashes before loading the fixture. No application build is needed.

Bind `transportSources` to SHA256 digests of the two Python implementation
files on the operator machine. Recompute them after any source change. Use
`sha256sum FILE` on Linux or `shasum -a 256 FILE` on macOS. Keep real host names,
paths, identity bindings and receipts out of committed examples.

Each remote `admissionCollector` must be an installed canonical
`collect_work_capacity.py` accepting `--format admission --sample-seconds 1`
and returning `work-capacity-admission/v1`. The adapter uses the explicit
`python` and collector path on that same host. It does not copy or install a
collector. Remote aliases are resolved only by the operator's existing SSH
configuration; neither remote host needs credentials for the other.

## Admit and run one stage

Reserve one serial test slot on each remote host. Immediately before starting,
collect canonical CPU/memory admission on both hosts and the operator machine.
Use the native macOS advisory collector on a macOS operator. Preserve the
original sample receipts. Missing evidence, CPU at least 85%, or a zero serial
ceiling prevents a start. Verify that SHA256 hashes of each remote host's
`/proc/sys/kernel/random/boot_id` differ. These hashes identify hosts for this
run; they do not belong in repository source.

Create an admission receipt beside the bound request. This is a declaration of
already-obtained slot ownership and fresh native evidence, not an approval
mechanism. Populate it from the samples rather than copying example values:

```json
{
  "schema": "jones-je08h-admission/v1",
  "runId": "the-request-run-id",
  "bothHostSlotsGranted": true,
  "collectedAtUtc": "REPLACE_WITH_OLDEST_SAMPLE_UTC",
  "cpuPercent": { "operator": 0, "driver": 0, "worker": 0 },
  "serialTestProcessCeiling": { "operator": 1, "driver": 1, "worker": 1 },
  "remoteRamAvailableGiB": { "driver": 0, "worker": 0 },
  "operatorMemoryDecision": "continue",
  "localBootIdSha256": "REPLACE_WITH_DRIVER_BOOT_HASH",
  "remoteBootIdSha256": "REPLACE_WITH_WORKER_BOOT_HASH"
}
```

All samples must be fresh at launch (at most 120 seconds). For a Linux operator,
record `continue` only after its canonical memory/serial gate passes. On macOS,
use the collector's `memory_decision`. The operator machine runs lightweight
SSH/relay work; the remote hosts run the fixtures.

From the operator checkout:

```sh
python3 apps/server/integration/jones-e2e/physicalHost.operator.py \
  --request /absolute/operator/task-owned-evidence/request.json \
  --admission-receipt /absolute/operator/task-owned-evidence/admission.json
```

Run these stages separately, examining receipts and releasing/reacquiring slots
between them:

1. `import-smoke`: import both fixture runtimes and close normally.
2. `lifecycle-smoke`: cancel B after ready, then start a second B with deliberate
   failure after listener acquisition. The latter's exit 1 and clean failed
   receipt are expected; other failures remain failures.
3. `qualify`: one stage containing four complete independent fixture runs.
   This is the initial qualification plus three repeats, not four messages in
   one setup.

The operator derives matching driver/relay requests and stages only its own
remote request and parent directories. The relay passes worker control bytes
unchanged and establishes loopback HTTP forwards before releasing ready. For
each qualification transition, it samples both remote hosts concurrently after
cleanup and before releasing the closed receipt/EOF. A failed admission blocks
the next A/C fixture and B process. The operator machine is admitted at stage
start only. There are no automatic retries, sleep loops or deadline extensions.

## Interpret results and cleanup

An operator exit 0 requires driver and relay success, exact remote process
identity absence, owned-root absence, and relay cleanup receipts. Inspect the
`RUN.operator.json`, `RUN.driver-evidence.json` and
`receipts/RUN-relay.json` files. Driver evidence remains in the explicitly bound
remote evidence directory; the local bundle includes results, process bindings
and cleanup receipts. Relay receipts include each next-run admission and exact
captured child outcomes. Driver result files deliberately retain
`pending-operator-route-and-process-readback`; the separate successful operator
postconditions discharge that gate without rewriting driver evidence.

Driver stdout/stderr are captured privately with a 1 MiB limit per stream.
Raw worker control frames, pairing codes and authentication state are never
logged. The receipt records diagnostic paths and digests; keep them private.
Never collect fixture secret stores or whole ambient environments for debugging.

On failure, preserve the first result and stop. The operator stops only its
captured local children. The worker owns remote server/root cleanup. A lost
boot/final receipt, live captured PID/startTicks, remaining owned root or failed
forward readback is unknown cleanup and blocks another launch. Use only the
captured identity and exact owned path to reconcile it; process-name searches
and pattern kills are not ownership evidence. Evidence and request parents are
retained for the operator's task lifetime; fixture roots are removed by their
own finalizers. No broad directory cleanup is performed.

## Source checks and qualification boundary

The focused pure group requires no SSH, apps or installed Node dependencies:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 \
  apps/server/integration/jones-e2e/physicalHost.transport.test.py
```

It covers configuration refusal, unchanged frames, admission barriers and
captured forward identity checks. Run the existing Jones integration group and
boundary check when changing the driver/worker or catalog mappings. Actual
physical-host qualification remains opt-in and must use the exact candidate's
source bindings. Passing pure tests, an old external relay receipt or a run
against different source hashes does not qualify a new transport candidate.
