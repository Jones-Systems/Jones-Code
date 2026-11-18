import type { EnvironmentId, ThreadContextRecord } from "@t3tools/contracts";
import { captureThreadReferenceExcerpt } from "@t3tools/client-runtime/jones/threadReferenceExcerpt";
import { runAtomCommand, squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import * as Option from "effect/Option";

import { appAtomRegistry } from "../../state/atom-registry";
import { orchestrationEnvironment } from "../../state/orchestration";
import { environmentSession } from "../../state/session";

export function canCaptureThreadReference(environmentId: EnvironmentId): boolean {
  return Option.isSome(
    appAtomRegistry.get(environmentSession.preparedConnectionValueAtom(environmentId)),
  );
}

export async function captureThreadReference(record: ThreadContextRecord) {
  const excerpt = await captureThreadReferenceExcerpt({
    threadId: record.threadId,
    request: async () => {
      const result = await runAtomCommand(
        appAtomRegistry,
        orchestrationEnvironment.v2.threadReferenceExcerpt,
        { environmentId: record.environmentId, input: { threadId: record.threadId } },
        { reportFailure: false, reportDefect: false },
      );
      if (result._tag !== "Success") throw squashAtomCommandFailure(result);
      return result.value;
    },
  });
  return { ...record, excerpt };
}
