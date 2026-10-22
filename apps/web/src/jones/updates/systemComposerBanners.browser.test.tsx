import { EnvironmentId } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";
import type { EnvironmentPresentation } from "../../state/environments";
import { useSystemComposerBanners } from "../../components/chat/useSystemComposerBanners";

const branding = vi.hoisted(() => ({
  APP_VERSION: "0.0.45-preview.20261010.1",
  APP_SOURCE_SHA: "a".repeat(40),
  APP_BUILD_STARTED_AT: "2026-10-20T21:13:43Z",
}));
vi.mock("../../branding", () => branding);
vi.mock("./ProductionExactSourceUpdateAction", () => ({
  ProductionExactSourceUpdateAction: () => null,
}));
vi.mock("../../components/chat/useAutoBalanceUpdateBanner", () => ({
  useAutoBalanceUpdateBanner: () => null,
}));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => vi.fn() }));
vi.mock("../../state/server", async (original) => {
  const actual = await original<typeof import("../../state/server")>();
  const { Atom } = await import("effect/reactivity");
  const idle = Atom.make({ status: "idle" as const });
  return {
    ...actual,
    serverEnvironment: { ...actual.serverEnvironment, updateStateAtom: () => idle },
  };
});

function environment(id: string, sha = "b".repeat(40)) {
  return {
    environmentId: EnvironmentId.make(id),
    label: id,
    serverConfig: {
      environment: {
        environmentId: EnvironmentId.make(id),
        label: id,
        platform: { os: "linux", arch: "x64" } as const,
        capabilities: { repositoryIdentity: true },
        serverVersion: branding.APP_VERSION,
        jonesSource: { repository: "Jones-Systems/Jones-Code" as const, sha, tree: sha },
      },
    },
  };
}
function Harness({
  selected,
  eligible = [],
  automatic = false,
}: {
  selected: ReturnType<typeof environment> | null;
  eligible?: ReturnType<typeof environment>[];
  automatic?: boolean;
}) {
  const items = useSystemComposerBanners({
    activeEnvironment: null,
    clientUpdateEnvironment: selected,
    activeEnvironmentUnavailable: false,
    activeEnvironmentUnavailableState: null,
    activeThreadEnvironmentId: null,
    serverConfig: null,
    hasMultipleRegisteredEnvironments: true,
    automaticEnvironment: automatic,
    // These fixtures only provide the source observations consumed by client notices.
    autoUpdateEnvironments: eligible as unknown as EnvironmentPresentation[],
  });
  return (
    <>
      {items.map((item) => (
        <section key={item.id}>
          {item.title}
          {item.description}
          <button onClick={item.onDismiss}>{item.dismissLabel}</button>
        </section>
      ))}
    </>
  );
}
function isolateDismissals(): Disposable {
  const key = "t3code:version-mismatch-dismissals:v1";
  const previous = localStorage.getItem(key);
  localStorage.removeItem(key);
  return {
    [Symbol.dispose]() {
      if (previous === null) localStorage.removeItem(key);
      else localStorage.setItem(key, previous);
    },
  };
}

it("shows the local draft's source notice through the extracted system hook and dismisses only that source pair", async () => {
  using _dismissals = isolateDismissals();
  await using screen = await render(<Harness selected={environment("Local draft")} />);
  expect(screen.container.textContent).toContain("Local draft: Different preview sources");
  expect(screen.container.textContent).toContain("aaaaaaaaaaaa");
  expect(screen.container.textContent).toContain("bbbbbbbbbbbb");
  await act(() => screen.container.querySelector<HTMLButtonElement>("button")!.click());
  expect(screen.container.textContent).toBe("");
  await screen.rerender(<Harness selected={environment("Local draft", "c".repeat(40))} />);
  expect(screen.container.textContent).toContain("cccccccccccc");
});

it("shows every eligible automatic environment while excluding the manual selection and equal sources", async () => {
  using _dismissals = isolateDismissals();
  await using screen = await render(
    <Harness
      selected={environment("Manual")}
      eligible={[environment("Eligible"), environment("Equal", branding.APP_SOURCE_SHA)]}
      automatic
    />,
  );
  expect(screen.container.textContent).toContain("Eligible: Different preview sources");
  expect(screen.container.textContent).not.toContain("Manual");
  expect(screen.container.textContent).not.toContain("Equal");
});
