# Process placement boundary

The placement modules are preparation for a server launch boundary. They are
not connected to the Jones Code launcher, provider adapters, terminals, or
telemetry. Setting a placement environment variable alone does not isolate a
Jones Code process. Integration must cover every launch route before an
installed server can advertise or require placement.

Placement belongs to the environment that owns execution, regardless of the
client's machine. The versioned [binding](../../apps/server/src/jones/processPlacement/processPlacement.ts)
selects separate control and workload siblings in an already delegated Linux
cgroup v2 hierarchy. A policy owner must establish that hierarchy and its CPU
limits. The helper only moves itself, verifies membership, then replaces itself
with the payload. It neither creates cgroups nor migrates running sessions.

The helper's canonical path, hash, device and inode must identify the executed
file, not merely the file inspected before spawn. The native helper repeats
these checks against `/proc/self/exe` to reject a replacement between validation
and execution. A required placement failure must never retry the payload
without placement. The unset configuration preserves the original command.
Successful exec preserves the PID and inherited descriptors; the integrating
adapter must preserve its existing cancellation and process-group ownership.

An external cold bootstrap must validate its own immutable artifacts, establish
and read back the delegated hierarchy, remove the bootstrap variable and emit
a ready binding before replacing itself with the launcher. The
[bootstrap contract](../../apps/server/src/jones/processPlacement/processPlacementBootstrap.ts)
validates inputs; it does not install a service or implement that bootstrap.
Optional negative nice with reset-on-fork applies only to an explicitly selected
server generation. It requires separately supplied host permissions and does
not promise priority for every runtime thread.

The Effect spawner wrapper prepares workload commands and permits explicitly
marked control commands. It is not a complete process interception mechanism:
SDK-owned children, PTYs, launcher replacement and rollback, and telemetry need
integration at their owning boundaries. Provider tools normally inherit their
parent's cgroup; independently launched agent roots need their own wrapper.

The explicit `build:process-placement` server script compiles a host Linux helper
and records source and binary hashes. It is separate from bundle and executable
packaging: those pipelines clean their output and can target another platform
or architecture. Release integration must select the target helper, retain its
manifest and bind the installed artifact before enabling placement. A compiled
helper or passing rejection test does not prove delegation, positive placement,
live responsiveness, or installed release provenance.
