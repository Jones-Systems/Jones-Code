import { useState } from "react";
import { createRoot } from "react-dom/client";
import { EnvironmentId } from "@t3tools/contracts";
import "../../../index.css";
import { APP_SOURCE_SHA, APP_VERSION } from "../../../branding";
import { ComposerBannerStack } from "../../../components/chat/ComposerBannerStack";
import { useClientUpdateBanners } from "../clientUpdateBanner";

function Fixture() {
  const [selected, setSelected] = useState("Laptop");
  const [automatic, setAutomatic] = useState(false);
  const [scenario, setScenario] = useState("different");
  const source =
    scenario === "equal"
      ? APP_SOURCE_SHA
      : scenario === "changed"
        ? "b".repeat(39) + "c"
        : "b".repeat(40);
  const environments = ["Laptop", "Remote"].map((label) => ({
    environmentId: EnvironmentId.make(`client-notice-fixture-${label}`),
    label,
    serverConfig: {
      environment: {
        environmentId: EnvironmentId.make(`client-notice-fixture-${label}`),
        label,
        platform: { os: "linux", arch: "x64" } as const,
        capabilities: { repositoryIdentity: true },
        serverVersion: scenario === "newer" ? "9.0.0" : APP_VERSION,
        ...(source
          ? {
              jonesSource: {
                repository: "Jones-Systems/Jones-Code" as const,
                sha: source,
                tree: source,
              },
            }
          : {}),
      },
    },
  }));
  const banners = useClientUpdateBanners({
    activeEnvironment: environments.find((environment) => environment.label === selected),
    automaticEnvironment: automatic,
    eligibleEnvironments: environments,
  });
  return (
    <main className="mx-auto max-w-3xl space-y-8 p-8 text-foreground">
      <h1 className="text-xl font-semibold">Client version notice fixture</h1>
      <p>Synthetic environment descriptors; production notice hook and composer banner stack.</p>
      <p>
        Client {APP_VERSION}, source {APP_SOURCE_SHA ?? "unknown"}
      </p>
      <section className="flex flex-wrap gap-3" aria-label="Fixture inputs">
        <label>
          Environment{" "}
          <select value={selected} onChange={(event) => setSelected(event.target.value)}>
            <option>Laptop</option>
            <option>Remote</option>
          </select>
        </label>
        <label>
          <input
            type="checkbox"
            checked={automatic}
            onChange={(event) => setAutomatic(event.target.checked)}
          />{" "}
          Automatic
        </label>
        <label>
          Pair{" "}
          <select value={scenario} onChange={(event) => setScenario(event.target.value)}>
            <option value="different">Different preview sources</option>
            <option value="changed">Changed full source, same abbreviation</option>
            <option value="equal">Equal sources</option>
            <option value="newer">Newer stable server</option>
          </select>
        </label>
      </section>
      <output aria-label="Visible notice count">Visible notices: {banners.length}</output>
      <section aria-label="Production composer notices" className="pt-12">
        <ComposerBannerStack items={banners} />
        <div className="rounded-b-lg border p-4 text-muted-foreground">Synthetic composer</div>
      </section>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<Fixture />);
