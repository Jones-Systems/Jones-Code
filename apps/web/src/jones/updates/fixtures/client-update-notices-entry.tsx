import { useState } from "react";
import { createRoot } from "react-dom/client";
import { EnvironmentId } from "@t3tools/contracts";
import "../../../index.css";
import { APP_BUILD_STARTED_AT, APP_SOURCE_SHA, APP_VERSION } from "../../../branding";
import { ComposerBannerStack } from "../../../components/chat/ComposerBannerStack";
import { useClientUpdateBanners, type ClientUpdateBannerInput } from "../clientUpdateBanner";
import { jonesBuildLabel } from "../buildVersion";
import { useSyntheticExactSource } from "./exact-source-fixture";

function Conversation({
  name,
  selected,
  ...input
}: ClientUpdateBannerInput & { readonly name: string; readonly selected: boolean }) {
  const banners = useClientUpdateBanners(input);
  return (
    <section hidden={!selected} aria-label={`Conversation ${name}`}>
      <output aria-label="Visible notice count">Visible notices: {banners.length}</output>
      <section aria-label="Production composer notices" className="pt-12">
        <ComposerBannerStack items={banners} />
        <div className="rounded-b-lg border p-4 text-muted-foreground">
          Synthetic conversation {name}
        </div>
      </section>
    </section>
  );
}

function Fixture() {
  const [conversation, setConversation] = useState("A");
  const [selected, setSelected] = useState("Laptop");
  const [automatic, setAutomatic] = useState(false);
  const [scenario, setScenario] = useState("older");
  const source =
    scenario === "equal"
      ? APP_SOURCE_SHA
      : scenario === "changed"
        ? "b".repeat(39) + "c"
        : "b".repeat(40);
  const serverBuildStartedAt =
    scenario === "identity" || scenario === "equal"
      ? APP_BUILD_STARTED_AT
      : scenario === "newer" || scenario === "stable"
        ? "2026-10-31T00:43:00Z"
        : "2026-10-28T15:26:00Z";
  const environments = ["Laptop", "Remote"].map((label) => ({
    environmentId: EnvironmentId.make(`client-notice-fixture-${label}`),
    label,
    serverConfig: {
      environment: {
        environmentId: EnvironmentId.make(`client-notice-fixture-${label}`),
        label,
        platform: { os: "linux", arch: "x64" } as const,
        capabilities: { repositoryIdentity: true },
        serverVersion:
          scenario === "stable"
            ? "9.0.0"
            : scenario === "newer"
              ? "0.1.2-preview.20261030.40652017133.1"
              : scenario === "identity"
                ? APP_VERSION
                : "0.1.2-preview.20261028.40652016025.1",
        ...(serverBuildStartedAt ? { jonesBuildStartedAt: serverBuildStartedAt } : {}),
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
  // This page has no connection providers; actions run against the synthetic exact-source host.
  const { renderExactSourceAction } = useSyntheticExactSource();
  return (
    <main className="mx-auto max-w-3xl space-y-8 p-8 text-foreground">
      <h1 className="text-xl font-semibold">Client version notice fixture</h1>
      <p>Synthetic environment descriptors; production notice hook and composer banner stack.</p>
      <p>
        Client build{" "}
        {
          jonesBuildLabel(
            { version: APP_VERSION, sourceSha: APP_SOURCE_SHA, startedAt: APP_BUILD_STARTED_AT },
            "day",
          ).text
        }
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
            <option value="older">Older preview server</option>
            <option value="newer">Newer preview server</option>
            <option value="identity">Same version, different sources</option>
            <option value="changed">Changed full source, same abbreviation</option>
            <option value="equal">Equal sources</option>
            <option value="stable">Newer stable server</option>
          </select>
        </label>
      </section>
      <nav className="flex gap-3" aria-label="Conversations">
        {["A", "B"].map((name) => (
          <button
            key={name}
            type="button"
            aria-pressed={conversation === name}
            onClick={() => setConversation(name)}
          >
            Conversation {name}
          </button>
        ))}
      </nav>
      {["A", "B"].map((name) => (
        <Conversation
          key={name}
          name={name}
          selected={conversation === name}
          activeEnvironment={environments.find((environment) => environment.label === selected)}
          automaticEnvironment={automatic}
          eligibleEnvironments={environments}
          renderExactSourceAction={renderExactSourceAction}
        />
      ))}
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<Fixture />);
