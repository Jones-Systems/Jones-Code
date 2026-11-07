import { WORKSTREAM_SHELL_ENV_NAMES } from "@t3tools/shared/jones/workstreamShellActivation";
import { decodeRegistryBinding } from "@t3tools/shared/jones/workstreamsRegistryBinding";
import { activationFromEnvironment, type WorkstreamActivation } from "./activation.ts";
import { makeControlPlaneWorkstreamTransport } from "../../workstreams/ControlPlaneWorkstreamTransport.ts";
import type { WorkstreamTransport } from "../../workstreams/WorkstreamGateway.ts";

type Configured = ReturnType<typeof makeControlPlaneWorkstreamTransport> & {
  readonly registryInstanceId: string | null;
};
/** Recover only the initial cold-GUI discovery. Once enabled, freeze the admitted configuration. */
export function makeDeferredShellWorkstreamActivation(
  initial: WorkstreamActivation,
  readEnvironment: () => NodeJS.ProcessEnv = () => process.env,
  create = makeControlPlaneWorkstreamTransport,
): Configured {
  const configure = (activation: WorkstreamActivation): Configured => ({
    ...create(activation),
    registryInstanceId:
      activation.state === "enabled" && activation.value.registryBinding !== undefined
        ? decodeRegistryBinding(activation.value.registryBinding).registry_instance_id
        : null,
  });
  let configured = configure(initial);
  let frozen = configured.activationState === "enabled";
  let last = WORKSTREAM_SHELL_ENV_NAMES.map((name) => readEnvironment()[name]);
  const current = () => {
    if (frozen) return configured;
    const environment = readEnvironment();
    const values = WORKSTREAM_SHELL_ENV_NAMES.map((name) => environment[name]);
    if (!values.every((value, index) => value === last[index])) {
      last = values;
      const next = activationFromEnvironment(environment);
      configured = configure(next.state === "disabled" ? initial : next);
      frozen = configured.activationState === "enabled";
    }
    return configured;
  };
  const transport: WorkstreamTransport = {
    get getCapabilities() {
      return current().transport.getCapabilities;
    },
    get listWorkstreams() {
      return current().transport.listWorkstreams;
    },
    get getWorkstream() {
      return current().transport.getWorkstream;
    },
    get listReferences() {
      return current().transport.listReferences;
    },
    get getReference() {
      return current().transport.getReference;
    },
    get listMemberships() {
      return current().transport.listMemberships;
    },
    get listDeclarations() {
      return current().transport.listDeclarations;
    },
    get listEdges() {
      return current().transport.listEdges;
    },
    get listHistory() {
      return current().transport.listHistory;
    },
    get getCommand() {
      return current().transport.getCommand;
    },
    get submitCommand() {
      return current().transport.submitCommand;
    },
  };
  // Optional capabilities remain absent until the admitted transport supplies them.
  Object.defineProperties(transport, {
    readCommandReceipt: { enumerable: true, get: () => current().transport.readCommandReceipt },
    resubmitCommand: { enumerable: true, get: () => current().transport.resubmitCommand },
    getCountsCapabilities: {
      enumerable: true,
      get: () => current().transport.getCountsCapabilities,
    },
    getRegistryCounts: { enumerable: true, get: () => current().transport.getRegistryCounts },
    readAppearance: { enumerable: true, get: () => current().transport.readAppearance },
    saveAppearance: { enumerable: true, get: () => current().transport.saveAppearance },
    listThreadPlacements: { enumerable: true, get: () => current().transport.listThreadPlacements },
  });
  return {
    transport,
    binding: {
      get registryId() {
        return current().binding.registryId;
      },
      get ownerId() {
        return current().binding.ownerId;
      },
      get principalId() {
        return current().binding.principalId;
      },
      get authorizationRevision() {
        return current().binding.authorizationRevision;
      },
    },
    registrationContext: {
      get registrationContextVersion() {
        return current().registrationContext.registrationContextVersion;
      },
      get activationState() {
        return current().activationState;
      },
      get configuredBinding() {
        return current().registrationContext.configuredBinding;
      },
      get readRegistrationContext() {
        return current().registrationContext.readRegistrationContext;
      },
    },
    get activationState() {
      return current().activationState;
    },
    get registryInstanceId() {
      return current().registryInstanceId;
    },
  };
}
