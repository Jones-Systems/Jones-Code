import { createImportedHistoryCommands } from "@t3tools/client-runtime/jones/imported-history/commands";
import { connectionAtomRuntime } from "../../connection/runtime";

export const importedHistoryCommands = createImportedHistoryCommands(connectionAtomRuntime);
