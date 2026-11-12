import type { DesignRequestBinding } from "@t3tools/contracts/jones/designRequests";

import type {
  PersonalDesignAssignment,
  PersonalDesignAssignments,
  DesignRequestRouteInput,
} from "./route.ts";

// Synthetic registry state for design-request tests; no live registry or thread data.
export const NOW = Date.parse("2026-10-09T12:00:00.000Z");
export const binding: DesignRequestBinding = {
  bindingId: "0".repeat(32),
  galleryOrigin: "http://100.113.248.16:4339",
  projectKey: "form2",
  workstreamId: "ws-design",
};
export const data = {
  confirmed: true,
  items: [
    { workstreamId: "ws-design", name: "Form 2 design" },
    { workstreamId: "ws-other", name: "Other" },
  ],
};
export const placement = (
  overrides: Partial<PersonalDesignAssignment> = {},
): PersonalDesignAssignment => ({
  workstreamId: "ws-design",
  environmentId: "env-primary",
  threadId: "thread-1",
  revision: 1,
  ...overrides,
});
export const placements = (
  items: readonly PersonalDesignAssignment[] = [placement()],
): PersonalDesignAssignments => ({ items });

export const thread = {
  environmentId: "env-primary",
  id: "thread-1",
  title: "Form 2 build",
  archivedAt: null,
  runtimeMode: "approval-required",
  interactionMode: "default",
} as const;

export const routeInput = (
  overrides: Partial<DesignRequestRouteInput> = {},
): DesignRequestRouteInput => ({
  binding,
  registry: "ready",
  data,
  placements: placements(),
  primaryEnvironmentId: "env-primary",
  threads: [thread, { ...thread, id: "thread-2", title: "Other thread" }],
  now: NOW,
  ...overrides,
});
