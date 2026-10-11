# Standalone desktop executor preparation

A source-bound unsigned Jones Mac preview includes a standalone Python tool at
Contents/Resources/jones-bootstrap/bootstrap.py. It prepares the new updater
outside the application, so the installed application's older embedded helper is
not required to make those bytes available.

The preparation commands are inspect and stage-executor. Both require the exact active
installation manifest, an existing completed stage-selection record, and the
SHA-256 of that selection's raw bytes:

```text
python3 <candidate.app>/Contents/Resources/jones-bootstrap/bootstrap.py inspect --manifest <absolute-manifest> --selection <absolute-selection> --selection-sha256 <sha256>
python3 <candidate.app>/Contents/Resources/jones-bootstrap/bootstrap.py stage-executor --manifest <absolute-manifest> --selection <absolute-selection> --selection-sha256 <sha256>
```

Use the installer-owned manifest and selection paths from the selected update.
The tool validates their binding to the installed app and the existing artifact
and staged-app receipts. It derives its output directory under the installation's
runtime/jones-updates/bootstrap-executors directory; callers cannot select another
output location. Python 3 is a prerequisite. Pending, malformed or uncertain
update transactions require reconciliation before preparation can proceed.

Completion reports executor-ready with activationAllowed:false and the remaining
qualified-maintenance-admission requirement. This result proves immutable executor
publication only. It supplies no process drain, state snapshot, activation grant,
or completed application upgrade. The first legacy-to-new transition still needs
qualified exclusion of old writers before any mutable-state transition.

Exact completed retries reuse the retained result. An occupied unknown or partial
output is preserved and refused for reconciliation. Do not overwrite it or replace
the installed helper's hash-named file. Database and profile contents are outside
both commands' read/write scope, and neither command runs the copied executor.

To investigate retained installer objects without a completed selection, use the
separate read-only diagnostic:

```text
python3 <candidate.app>/Contents/Resources/jones-bootstrap/bootstrap.py inventory-routes --manifest <absolute-manifest>
```

It derives supported app-stage and helper roots from the manifest and reports
partial attempts and unresolved records. It does not scan arbitrary app copies,
inspect processes, or probe database and profile contents. Selection arguments
are not accepted for this command. The JSON result goes to stdout; the tool writes
nothing. Exit 0 means the bounded observation completed, 2 means observation was
partial, and 1 means the manifest anchor was refused. An observed result can still
contain unbound objects. Historical coverage remains partial, process custody is
unobserved, and activationAllowed is always false. This report cannot establish
writer exclusion, process drain, executable authenticity, or upgrade permission.
