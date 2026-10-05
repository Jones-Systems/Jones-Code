import { createJonesUpdateAtoms } from "@t3tools/client-runtime/state/jonesUpdates";
import { connectionAtomRuntime } from "../connection/runtime";
export const jonesUpdates = createJonesUpdateAtoms(connectionAtomRuntime);
