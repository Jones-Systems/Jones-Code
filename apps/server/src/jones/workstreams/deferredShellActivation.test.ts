import { describe, expect, it } from "vite-plus/test";
import { makeDeferredShellWorkstreamActivation } from "./deferredShellActivation.ts";

const activationEnvironment = (): NodeJS.ProcessEnv => ({
  T3_WORKSTREAM_CONTROL_PLANE_URL: "https://synthetic.invalid",
  T3_WORKSTREAM_OWNER_ID: "owner",
  T3_WORKSTREAM_PRINCIPAL_ID: "principal",
  T3_WORKSTREAM_KEY_ID: "key",
  T3_WORKSTREAM_SIGNING_SECRET: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
  T3_WORKSTREAM_AUTHORIZATION_REVISION: "1",
});
describe("private cold-GUI activation", () => {
  it("refreshes an already-constructed transport on late discovery and freezes the admitted selection", () => {
    const environment: NodeJS.ProcessEnv = {};
    const configured = makeDeferredShellWorkstreamActivation(
      { state: "disabled" },
      () => environment,
    );
    expect(configured.activationState).toBe("disabled");
    expect(configured.binding.ownerId).toBe("disabled");
    Object.assign(environment, activationEnvironment());
    expect(configured.registrationContext.activationState).toBe("enabled");
    expect(configured.binding.ownerId).toBe("owner");
    expect(typeof configured.transport.readCommandReceipt).toBe("function");
    environment.T3_WORKSTREAM_OWNER_ID = "changed";
    expect(configured.binding.ownerId).toBe("owner");
  });
  it("does not enable incomplete or explicitly disabled inherited activation", () => {
    const environment = activationEnvironment();
    environment.T3_WORKSTREAM_OWNER_ID = "";
    const configured = makeDeferredShellWorkstreamActivation({ state: "disabled" }, () => ({}));
    expect(configured.activationState).toBe("disabled");
    const invalid = makeDeferredShellWorkstreamActivation({ state: "invalid" }, () => environment);
    expect(invalid.activationState).toBe("invalid");
    expect(invalid.transport.readCommandReceipt).toBeUndefined();
  });
});
