import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import { OrchestratorMcpCapabilitiesResult } from "../orchestratorMcp.ts";
import {
  REMOTE_ADMISSION_OBSERVATION_VERSION,
  REMOTE_ADMISSION_PEER_STATUS_VERSION,
  REMOTE_ADMISSION_RETRY_OBSERVATION_VERSION,
  REMOTE_ADMISSION_RECOVERY_STATUS_VERSION,
  REMOTE_ADMISSION_RECOVERED_RESULT_VERSION,
  REMOTE_ADMISSION_CAPABILITIES_REGISTER_VERSION,
} from "./remoteAdmissionReview.ts";

const oldFeatures = Schema.Struct({
  appOwnedSubagents: Schema.Boolean,
  asyncPolling: Schema.Boolean,
  cancellation: Schema.Boolean,
  batchThreadCreation: Schema.Boolean,
  threadManagement: Schema.Boolean,
  incrementalThreadRead: Schema.Boolean,
  scheduledTasks: Schema.Boolean,
  maxBatchThreads: Schema.Number,
});
const legacy = {
  parentThreadId: null,
  inheritedProviderInstanceId: null,
  inheritedModel: null,
  runtimeMode: "full-access",
  interactionMode: "default",
  providers: [],
  features: {
    appOwnedSubagents: true,
    asyncPolling: true,
    cancellation: true,
    batchThreadCreation: true,
    threadManagement: true,
    incrementalThreadRead: true,
    scheduledTasks: true,
    maxBatchThreads: 20,
  },
};
const negotiated = {
  remoteAdmissionObservationVersion: REMOTE_ADMISSION_OBSERVATION_VERSION,
  remoteAdmissionPeerStatusVersion: REMOTE_ADMISSION_PEER_STATUS_VERSION,
  remoteAdmissionRetryObservationVersion: REMOTE_ADMISSION_RETRY_OBSERVATION_VERSION,
  remoteAdmissionRecoveryStatusVersion: REMOTE_ADMISSION_RECOVERY_STATUS_VERSION,
  remoteAdmissionRecoveredResultObservationVersion: REMOTE_ADMISSION_RECOVERED_RESULT_VERSION,
  remoteAdmissionCapabilitiesRegisterVersion: REMOTE_ADMISSION_CAPABILITIES_REGISTER_VERSION,
};

describe("remote admission additive MCP negotiation", () => {
  it("decodes old servers without inventing support", () => {
    const value = Schema.decodeUnknownSync(OrchestratorMcpCapabilitiesResult)(legacy);
    expect(value).toEqual(legacy);
    for (const key of Object.keys(negotiated)) expect(value.features).not.toHaveProperty(key);
  });

  it("preserves each new independent key and lets old clients strip them", () => {
    const decode = Schema.decodeUnknownSync(OrchestratorMcpCapabilitiesResult);
    for (const [key, version] of Object.entries(negotiated)) {
      const value = { ...legacy, features: { ...legacy.features, [key]: version } };
      expect(decode(value)).toEqual(value);
      expect(Schema.decodeSync(oldFeatures)(value.features)).toEqual(legacy.features);
      expect(() => decode({ ...legacy, features: { ...legacy.features, [key]: true } })).toThrow();
    }
    const value = { ...legacy, features: { ...legacy.features, ...negotiated } };
    expect(decode(value)).toEqual(value);
  });
});
