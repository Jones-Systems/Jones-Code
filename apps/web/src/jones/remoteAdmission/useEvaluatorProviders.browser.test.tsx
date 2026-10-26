import { RegistryProvider, useAtomValue } from "@effect/atom-react";
import { EnvironmentId } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
import { render } from "../../../test/render";
import { serverEnvironment } from "../../state/server";
import { useEvaluatorProviders } from "./useEvaluatorProviders";

vi.mock("../../state/server", async () => {
  const Atom = await import("effect/unstable/reactivity/Atom");
  const Stream = await import("effect/Stream");
  const projection = Atom.make(
    Stream.concat(
      Stream.make({ source: "live", config: { providers: [] } }),
      Stream.never,
    ),
  );
  return { serverEnvironment: { configProjection: () => projection } };
});

const environmentId = EnvironmentId.make("evaluator-open-config-stream");

function ProvidersProbe() {
  const providers = useEvaluatorProviders(environmentId);
  const projection = useAtomValue(serverEnvironment.configProjection({ environmentId, input: {} }));
  return (
    <output>
      {providers === null ? "Unknown providers" : `${providers.length} providers`}
      {projection.waiting ? " · subscription open" : ""}
    </output>
  );
}

it("uses live provider data while the configuration subscription remains open", async () => {
  await using screen = await render(
    <RegistryProvider>
      <ProvidersProbe />
    </RegistryProvider>,
  );
  await expect.element(screen.getByText("0 providers · subscription open")).toBeVisible();
});
