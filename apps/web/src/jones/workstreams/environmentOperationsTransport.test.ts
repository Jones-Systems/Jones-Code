import { describe, expect, it, vi } from "vite-plus/test";
import {
  environmentOperationsTransport,
  type EnvironmentWorkstreamsRequest,
} from "./environmentWorkstreamsRequest";
import { WORKSTREAM_OPERATIONS_CONTRACT } from "@t3tools/contracts/jones/workstreamOperations";

describe("owning environment operations transport", () => {
  it("routes all three operations through the selected environment's authenticated client", async () => {
    const request = vi.fn(
      async (..._args: Parameters<EnvironmentWorkstreamsRequest>): Promise<never> => {
        throw new Error("request recorded");
      },
    );
    const transport = environmentOperationsTransport("linked-environment", request);
    await expect(
      transport.submit({
        request: {
          contract: WORKSTREAM_OPERATIONS_CONTRACT,
          operation_id: "operation-00000001",
          origin: { kind: "app" },
          kind: "set_primary",
          thread: { environment_id: "linked-environment", thread_id: "thread" },
          target: { kind: "unassigned", registry_id: "registry-1", owner_id: "owner" },
          expected: { kind: "any" },
        },
        wait_ms: 3000,
      }),
    ).rejects.toThrow("request recorded");
    await expect(transport.read({ open: true })).rejects.toThrow("request recorded");
    await expect(transport.setup()).rejects.toThrow("request recorded");
    expect(request.mock.calls.map(([environment, method]) => [environment, method])).toEqual([
      ["linked-environment", "POST"],
      ["linked-environment", "POST"],
      ["linked-environment", "GET"],
    ]);
  });
  it("propagates a capable server error without a legacy submission", async () => {
    const refusal = { code: "operation_id_conflict" };
    const request = vi.fn(
      async (..._args: Parameters<EnvironmentWorkstreamsRequest>): Promise<never> => {
        throw refusal;
      },
    );
    await expect(
      environmentOperationsTransport("linked", request).read({ open: true }),
    ).rejects.toBe(refusal);
    expect(request).toHaveBeenCalledTimes(1);
  });
});
