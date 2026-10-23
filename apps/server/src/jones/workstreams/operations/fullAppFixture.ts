// @effect-diagnostics nodeBuiltinImport:off -- Test-only signing keys are generated for one isolated fixture.
import { randomBytes, randomUUID } from "node:crypto";
import type { WorkstreamsRegistrationContextT3Source, WorkstreamReceipt } from "@t3tools/contracts";
import { activationFromEnvironment } from "../activation.ts";
import { startLoopbackControlPlane } from "./loopbackFixture.ts";

const committed = (receipt: WorkstreamReceipt) => {
  if (receipt.state !== "committed") throw new Error(`Fixture seed failed: ${receipt.state}`);
  return receipt;
};

/** The caller supplies the running app's observed identity; this helper never invents a build. */
export async function startFullAppControlPlane(source: WorkstreamsRegistrationContextT3Source) {
  const cp = await startLoopbackControlPlane({
    sources: [source],
    registryId: randomUUID(),
    signingSecret: randomBytes(32).toString("base64"),
    emptyAssignments: true,
  });
  let release: (() => void) | undefined;
  try {
    const value = cp.activation.value;
    const environment = {
      T3_WORKSTREAM_CONTROL_PLANE_URL: value.baseUrl.href,
      T3_WORKSTREAM_OWNER_ID: value.ownerId,
      T3_WORKSTREAM_PRINCIPAL_ID: value.principalId,
      T3_WORKSTREAM_KEY_ID: value.keyId,
      T3_WORKSTREAM_SIGNING_SECRET: value.signingSecret,
      T3_WORKSTREAM_AUTHORIZATION_REVISION: String(value.authorizationRevision),
      T3_WORKSTREAM_REGISTRY_BINDING: value.registryBinding,
      T3_WORKSTREAM_REGISTRATION_CONTEXT_VERSION: value.registrationContextVersion,
    };
    if (activationFromEnvironment(environment).state !== "enabled")
      throw new Error("Fixture activation was rejected by the production parser");
    const groups: Record<string, string> = {};
    for (const name of ["Alpha", "Beta"]) {
      const result = committed(
        await cp.command({
          operation: "create_workstream",
          name,
          lifecycle: "active",
          progress: { state: "progressing" },
          sort_order: name === "Alpha" ? 0 : 1,
        }),
      );
      const id = result.effects.workstream_versions[0]?.workstream_id;
      if (!id) throw new Error("Fixture create returned no workstream identity");
      groups[name] = id;
    }
    return {
      environment,
      groups,
      registryId: cp.authority.registry_instance_id,
      origin: cp.origin,
      snapshot: async () => {
        const state = await cp.snapshot();
        return {
          ...state,
          memberships: await Promise.all(
            state.workstreams.items.map((group) => cp.memberships(group.workstream_id)),
          ),
          observations: { ...cp.observations },
        };
      },
      pending() {
        if (release) throw new Error("A fixture gate is already held");
        cp.faults.commandOperations = [
          "attach_primary",
          "reattach_primary",
          "move_primary",
          "remove_membership",
        ];
        cp.faults.commandGate = new Promise<void>((resolve) => {
          release = resolve;
        });
      },
      release() {
        release?.();
        release = undefined;
        cp.faults.commandGate = null;
      },
      lostReply(enabled: boolean) {
        cp.faults.commandResponse = enabled ? "lost" : "valid";
        cp.faults.commandResponseOperations = [
          "create_workstream",
          "attach_primary",
          "reattach_primary",
          "move_primary",
          "remove_membership",
        ];
        // Only hide receipts after their reply was actually lost; planning scans still work.
        cp.faults.hideLostReceipts = enabled;
        if (!enabled) cp.faults.lostCommandIds.clear();
      },
      /** Competing owner change through real synthetic CP decisions; never edits app projections. */
      async assign(threadId: string, name: "Alpha" | "Beta") {
        const destinationId = groups[name]!;
        const registration = committed(
          await cp.command({
            operation: "register_reference",
            identity: {
              provider: "t3",
              source_instance_id: source.source_instance_id,
              native_id: threadId,
              resource_kind: "thread",
              id_kind: "internal",
              account_provenance: { kind: "not_account_scoped" },
            },
            pr_locator: null,
          }),
        );
        const referenceId = registration.effects.native_reference_id;
        if (!referenceId) throw new Error("Fixture registration returned no reference");
        let state = await cp.snapshot();
        const reference = state.references.items.find(
          (item) => item.native_reference_id === referenceId,
        );
        if (!reference) throw new Error("Fixture reference missing");
        committed(
          await cp.command({
            operation: "verify_reference",
            native_reference_id: referenceId,
            expected_attestation_version: reference.registration.attestation_version,
          }),
        );
        state = await cp.snapshot();
        const destination = state.workstreams.items.find(
          (item) => item.workstream_id === destinationId,
        )!;
        for (const group of state.workstreams.items) {
          const members = await cp.memberships(group.workstream_id);
          const current = members.items.find(
            (item) =>
              item.native_reference_id === referenceId &&
              item.kind === "primary" &&
              item.closed === null,
          );
          if (!current) continue;
          if (group.workstream_id === destinationId) return;
          committed(
            await cp.command({
              operation: "move_primary",
              source_workstream_id: group.workstream_id,
              expected_source_version: group.version,
              source_membership_id: current.membership_id,
              destination_workstream_id: destinationId,
              expected_destination_version: destination.version,
            }),
          );
          return;
        }
        committed(
          await cp.command({
            operation: "attach_primary",
            workstream_id: destinationId,
            expected_version: destination.version,
            native_reference_id: referenceId,
          }),
        );
      },
      async close() {
        release?.();
        release = undefined;
        cp.faults.commandGate = null;
        await cp.close();
      },
    };
  } catch (error) {
    await cp.close();
    throw error;
  }
}
