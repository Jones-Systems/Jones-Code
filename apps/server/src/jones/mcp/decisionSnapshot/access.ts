import type { ProjectId, ServerSettings } from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";

import type { McpCapability } from "../../../mcp/McpInvocationContext.ts";

export const DECISION_SNAPSHOT_CAPABILITY = "decision-snapshot" satisfies McpCapability;

export const hasAgentDecisionSnapshotProjectOverride = (settings: ServerSettings) =>
  Object.values(settings.projectSettingsOverrides).some(
    (entry) => entry.enableAgentDecisionSnapshotAccess !== undefined,
  );

/**
 * Whether thread credentials for this project carry the decision-snapshot
 * capability. Like device access, a project that cannot be resolved while any
 * project overrides the setting is withheld rather than given the environment
 * value, since its own override may be the one turning access off.
 */
export const resolveAgentDecisionSnapshotAccess = (
  settings: ServerSettings,
  projectId: ProjectId,
  projectResolved: boolean,
): boolean => {
  if (!projectResolved) {
    return hasAgentDecisionSnapshotProjectOverride(settings)
      ? false
      : settings.enableAgentDecisionSnapshotAccess;
  }
  return resolveProjectSettings(settings, projectId).settings.enableAgentDecisionSnapshotAccess;
};

/**
 * A thread credential is reused only when its grant equals the current one,
 * so turning any optional capability on or off rotates the credential.
 */
export const mcpGrantMatches = (
  issued: ReadonlySet<McpCapability>,
  current: ReadonlySet<McpCapability>,
): boolean =>
  issued.size === current.size && [...current].every((capability) => issued.has(capability));
