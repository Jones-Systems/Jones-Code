import { createLinkedThreadsQueryAtomFamily } from "@t3tools/client-runtime/jones/linked-threads";
import { connectionAtomRuntime } from "../../connection/runtime";

export const linkedThreads = createLinkedThreadsQueryAtomFamily(connectionAtomRuntime);
