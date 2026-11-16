import { expect, it, vi } from "vite-plus/test";
import * as Cause from "effect/Cause";
import { EnvironmentId } from "@t3tools/contracts";
import { RemoteAdmissionError } from "@t3tools/client-runtime/jones/remote-admission";
import { remoteAdmissionRequest } from "./runtime";
import { createAdmissionStore } from "./store";
import { admissionSnapshot } from "./testFixtures";

const run = vi.hoisted(() => vi.fn());
vi.mock("@t3tools/client-runtime/state/runtime", () => ({
  createRuntimeCommand: () => ({}),
  runAtomCommand: run,
}));
vi.mock("../../connection/runtime", () => ({ connectionAtomRuntime: {} }));
vi.mock("../../rpc/atomRegistry", () => ({ appAtomRegistry: {} }));

it.each(["unauthorized", "unsupported", "transient", "other"] as const)(
  "carries a bounded %s command failure into admission state and clears it on recovery",
  async (kind) => {
    run.mockReset().mockResolvedValue({
      _tag: "Failure",
      cause: Cause.fail(new RemoteAdmissionError(kind)),
    });
    const store = createAdmissionStore((input) =>
      remoteAdmissionRequest(EnvironmentId.make(`runtime-${kind}`), input),
    );
    await store.refresh();
    expect(store.getSnapshot()).toMatchObject({ loadFailed: true, loadFailure: kind });
    run.mockResolvedValue({ _tag: "Success", value: admissionSnapshot() });
    await store.refresh();
    expect(store.getSnapshot()).toMatchObject({ loadFailed: false, loadFailure: null });
    expect(store.getSnapshot().snapshot).toEqual(admissionSnapshot());
  },
);
