import {
  ProjectId,
  ThreadId,
  type EnvironmentId,
  type RemoteAdmissionRecord,
} from "@t3tools/contracts";
import { scopeProjectRef, scopeThreadRef } from "@t3tools/client-runtime/environment";
import { useProject, useThreadShell } from "../../state/entities";
import { admissionTarget, requestDisplayText } from "./presentation";

/** Resolve only the receiver's envelope IDs; sender context never enters these selectors. */
export function useRequestTarget(environmentId: EnvironmentId, record: RemoteAdmissionRecord) {
  const target = admissionTarget(record);
  const project = useProject(
    target.projectId === null
      ? null
      : scopeProjectRef(environmentId, ProjectId.make(target.projectId)),
  );
  const thread = useThreadShell(
    target.threadId === null ? null : scopeThreadRef(environmentId, ThreadId.make(target.threadId)),
  );
  return {
    ...target,
    projectName:
      target.projectId === null
        ? null
        : requestDisplayText(project?.title ?? "") || requestDisplayText(target.projectId),
    threadName:
      target.threadId === null
        ? null
        : requestDisplayText(thread?.title ?? "") || requestDisplayText(target.threadId),
  };
}

export type RequestTarget = ReturnType<typeof useRequestTarget>;
