import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { ThreadId, type EnvironmentId } from "@t3tools/contracts";
import { useThreadShell } from "../../state/entities";

export {
  RequestThreadStatus as PromptThreadStatus,
  RequestThreadTitle as PromptThreadTitle,
} from "../remoteAdmission/RequestThreadStatus";

export function usePromptThreadShell(environmentId: EnvironmentId, threadId: string | null) {
  return useThreadShell(threadId ? scopeThreadRef(environmentId, ThreadId.make(threadId)) : null);
}
