import { createJonesUpdateAtoms } from "@t3tools/client-runtime/jones/updates";
import { connectionAtomRuntime } from "../../connection/runtime";
export const jonesUpdates = createJonesUpdateAtoms(connectionAtomRuntime);
