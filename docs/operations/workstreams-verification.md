# Verify Workstreams assignments

Use separate evidence for source behavior and the installed application. Seeing
workstream names, passing component tests, or merging a fix does not establish
that an installed source can assign threads.

## Select the verification level

| Level                | Required evidence                                                                                                                                                                                   | What it leaves unproved                                                                          |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Component            | Menu and pointer tests, pinned-drop commit-before-unpin behavior, dismissal and permission changes                                                                                                  | Real Sidebar wiring, server qualification and durable membership                                 |
| HTTP integration     | Two distinct source environments using the real signed transport, gateway and registration checks against an isolated synthetic registry; committed receipts and cross-environment membership reads | Browser gestures, native database persistence, launcher authority and physical host connectivity |
| Isolated application | Real Sidebar and native commands against separate synthetic state homes; assignment and pin readback after reload and process restart                                                               | Another machine, independently upgraded clients, relay and tunnel routes                         |
| Physical hosts       | Exact installed identities and separate test homes on at least two machines; real registry and source enrollment, with matching readback from each                                                  | Other clients or routes not explicitly exercised                                                 |

Record the exact source revision, application and server identities, fixture,
entry point, connection route, result and teardown. Mark each unexercised level
as unverified. The HTTP fixture injects the authenticated principal, stamped
build and native authority. It does not test browser pairing, native database
ownership or whether a launcher trusts an installed build.

The loopback fixture now uses the configured registry-binding token and the
production transport-derived canonical identity. The unchanged list decoder
receives that HTTPS identity through real Jones HTTP. The fixture independently
implements the signed envelope over the existing synthetic registry; it is not
the producer's Fastify listener or PostgreSQL authorization implementation.
Golden request/response vectors and exact contract digests qualify the wire
encoding against the pinned producer source.

The joined hook test uses the real EnvironmentHttpApi client, Jones HTTP,
transport and unchanged command ledger. It exercises a grant change after
capabilities but before POST, authenticated refusal and HTTP 409, then a fresh
command without replay. Missing, invalid and replayed proof retain the original
fence, including when the fixture knows it refused before dispatch. Lost replies
after commit recover only through receipt GET. Synthetic authority restoration
is a test action, not an operational recovery procedure. This source evidence
still requires independent review and does not establish installed acceptance.

Run the focused HTTP group from the repository root:

```sh
vp test run apps/server/src/jones/workstreams/runtimeIntegration/loopbackBinding.test.ts apps/web/src/jones/workstreams/useWorkstreamsReconnect.test.tsx apps/server/src/jones/workstreams/runtimeIntegration/multiEnvironment.test.ts apps/server/src/workstreams/http.test.ts apps/server/src/jones/workstreams/registrationContext/service.test.ts apps/server/src/jones/workstreams/runtimeIntegration/native.test.ts --maxWorkers=1
```

## Verify the application paths

For each applicable local and connected environment, exercise ordinary and
pinned threads through both **Assign thread** and a pointer drop on a workstream
header. Include movement between workstreams and removal of primary membership.
Use distinct environments with colliding native thread IDs so cross-source
identity mistakes cannot hide behind globally unique fixtures.

For a pinned drop, require the assignment receipt to commit before unpinning.
On refusal, preserve the pin and prior membership. Verify menu behavior
independently; do not assume menu assignment has the same pin effect as dragging.
Read back membership and pin state through their owning APIs, then reload and
restart the isolated source to check persistence. A row moving in the DOM is
insufficient evidence.

Exercise a changed build, missing enrollment, insufficient scope, offline
source and an unknown command result. Refused operations must leave membership
unchanged. Recover an unknown result through its existing receipt; do not issue
a second command just to get a successful response.

An update transition needs its own check: qualify the original source, change
the build, observe assignment refusal, renew only that synthetic source, then
verify assignment and preserved memberships from both environments. Follow the
[native enrollment procedure](workstreams-native-enrollment.md); an unexpired
bearer alone does not qualify a different build. Test this with disposable
state, never by updating the owner's working application.

## Own test state and resources

Follow [development state and ports](development.md#state-and-ports) and the
[Browser testing skill](../../.agents/skills/test-t3-app/SKILL.md). Each source
gets its own explicit task-owned home, native database and environment identity;
the registry gets its own synthetic state. Create fixtures through commands or
APIs when claiming business behavior. Direct projection inserts establish only
visual state. Never copy live databases, credentials or enrollment files merely
to make a test instance work.

Use an on-demand run with a cleanup owner and a maximum lifetime. Reuse existing
qualified tooling where possible. Before starting, measure free storage, CPU
and memory pressure, then budget the runtime, database, captures and dependency
tree separately. Small SQLite fixtures do not imply a small development
checkout. On a constrained Mac Mini, prefer a qualified existing artifact over
another dependency tree; report a missing prerequisite rather than changing an
installed service.

Bind test listeners to loopback and retain their actual ports and process IDs.
Do not add a persistent service. A shared route, installed-artifact change or
private enrollment operation retains its own authorization boundary. A normal
unstamped development build cannot prove enabled native assignment; do not add
a production build-identity bypass for testing.

At the end of a bounded run, stop only the captured test processes and confirm
their listeners are closed. Preserve the result and required captures before
cleaning exact task-owned scratch paths. Leave live applications, services and
their stores untouched.
