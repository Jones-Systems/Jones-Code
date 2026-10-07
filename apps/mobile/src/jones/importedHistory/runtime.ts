import { CommandId, type EnvironmentId, type ThreadId } from "@t3tools/contracts";
import { createImportedHistoryCommands } from "@t3tools/client-runtime/jones/imported-history/commands";
import { createEnvironmentCommand, runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import { appAtomRegistry } from "../../state/atom-registry";
import { connectionAtomRuntime } from "../../connection/runtime";
import {
  flushMobileImportedHistoryCorrelation,
  mobileImportedHistoryStorage,
} from "../../state/use-composer-drafts";
import { scopedThreadKey } from "../../lib/scopedEntities";
import { uuidv4 } from "../../lib/uuid";
import { createMobileImportedHistoryChoice, type MobileImportedHistoryChoice } from "./controller";

import { mobileImportedHistoryIdentity } from "./identity";

export const mobileImportedHistoryCommands = createImportedHistoryCommands(connectionAtomRuntime);
const identityCommand = createEnvironmentCommand(connectionAtomRuntime, {
  label: "mobile:imported-history:identity",
  execute: mobileImportedHistoryIdentity,
});
const controllers = new Map<string, MobileImportedHistoryChoice>();
export function getMobileImportedHistoryChoice(
  environmentId: EnvironmentId,
  threadId: ThreadId,
): MobileImportedHistoryChoice {
  const key = scopedThreadKey(environmentId, threadId);
  const existing = controllers.get(key);
  if (existing !== undefined) return existing;
  const choice = createMobileImportedHistoryChoice({
    environmentId,
    threadId,
    storage: mobileImportedHistoryStorage(environmentId, threadId),
    isCurrent: () => true,
    allocateCommandId: () => CommandId.make(uuidv4()),
    persistReadback: flushMobileImportedHistoryCorrelation,
    review: async (delivery) => {
      const result = await runAtomCommand(
        appAtomRegistry,
        mobileImportedHistoryCommands.review,
        { environmentId, input: { threadId, delivery } },
        { reportFailure: false },
      );
      return result._tag === "Success" ? result.value : null;
    },
    identity: async (command) => {
      const result = await runAtomCommand(
        appAtomRegistry,
        identityCommand,
        { environmentId, input: command },
        { reportFailure: false },
      );
      if (result._tag !== "Success") throw new Error("Imported history identity is unavailable.");
      return result.value;
    },
    start: async (command) => {
      const result = await runAtomCommand(
        appAtomRegistry,
        mobileImportedHistoryCommands.start,
        { environmentId, input: command },
        { reportFailure: false },
      );
      return result._tag === "Success" ? result.value : null;
    },
    observe: async (input) => {
      const result = await runAtomCommand(
        appAtomRegistry,
        mobileImportedHistoryCommands.observe,
        { environmentId, input },
        { reportFailure: false },
      );
      return result._tag === "Success" ? result.value : null;
    },
  });
  controllers.set(key, choice);
  return choice;
}
