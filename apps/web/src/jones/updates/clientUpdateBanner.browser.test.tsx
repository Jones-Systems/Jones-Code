import { act, render } from "../../../test/render";
import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";
import { useClientUpdateBanners, type ClientUpdateBannerInput } from "./clientUpdateBanner";

const branding = vi.hoisted(() => ({
  APP_VERSION: "0.0.45-preview.20261010.1",
  APP_SOURCE_SHA: "a".repeat(40),
  APP_BUILD_STARTED_AT: "2026-10-20T21:13:43Z",
}));
vi.mock("../../branding", () => branding);
// The production action binds live connection atoms; its controller is tested on its own.
vi.mock("./ProductionExactSourceUpdateAction", () => ({
  ProductionExactSourceUpdateAction: () => null,
}));

const environment = (id: string, sha = "b".repeat(40), version = branding.APP_VERSION) => ({
  environmentId: EnvironmentId.make(id),
  label: id,
  serverConfig: {
    environment: {
      environmentId: EnvironmentId.make(id),
      label: id,
      platform: { os: "linux", arch: "x64" } as const,
      capabilities: { repositoryIdentity: true },
      serverVersion: version,
      jonesBuildStartedAt: "2026-10-20T21:13:47Z",
      jonesSource: { repository: "Jones-Systems/Jones-Code" as const, sha, tree: sha },
    },
  },
});
function Harness(props: ClientUpdateBannerInput) {
  const items = useClientUpdateBanners(props);
  return (
    <>
      {items.map((item) => (
        <section key={item.id}>
          {item.title}
          {item.description}
          {item.actions}
          <button onClick={item.onDismiss}>{item.dismissLabel}</button>
        </section>
      ))}
    </>
  );
}
const bannerUi = (props: ClientUpdateBannerInput) => <Harness {...props} />;

function cleanDismissals(): Disposable {
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

describe("client notice environment lifecycle", () => {
  it("selects active or every eligible automatic environment", async () => {
    using _dismissals = cleanDismissals();
    const active = environment("Active");
    const eligible = [environment("Eligible"), environment("Equal", "a".repeat(40))];
    await using screen = await render(
      bannerUi({
        activeEnvironment: active,
        automaticEnvironment: false,
        eligibleEnvironments: eligible,
      }),
    );
    expect(screen.container.innerHTML).toContain("Active");
    expect(screen.container.innerHTML).not.toContain("Eligible");
    expect(screen.container.innerHTML).toContain("Client");
    expect(screen.container.innerHTML).toContain("Server");
    expect(screen.container.innerHTML).toContain("aaaaaaaaaaaa");
    expect(screen.container.innerHTML).toContain("1020·1713:43");
    expect(screen.container.innerHTML).toContain("1020·1713:47");
    expect(
      [...screen.container.querySelectorAll("button")].map((button) => button.textContent),
    ).toEqual(["Dismiss client version notice"]);
    await screen.rerender(
      bannerUi({
        activeEnvironment: active,
        automaticEnvironment: true,
        eligibleEnvironments: eligible,
      }),
    );
    expect(screen.container.innerHTML).toContain("Eligible");
    expect(screen.container.innerHTML).not.toContain("Active");
    expect(screen.container.innerHTML).not.toContain("Equal");
    await screen.rerender(
      bannerUi({
        activeEnvironment: null,
        automaticEnvironment: false,
        eligibleEnvironments: eligible,
      }),
    );
    expect(screen.container.firstChild).toBeNull();
  });

  it("targets this app's own full source, and only for a preview source difference", async () => {
    using _dismissals = cleanDismissals();
    const targets: unknown[] = [];
    const props = (active: ReturnType<typeof environment>): ClientUpdateBannerInput => ({
      activeEnvironment: active,
      automaticEnvironment: false,
      eligibleEnvironments: [],
      renderExactSourceAction: (target) => {
        targets.push(target);
        return <button>{`Download ${target.targetSource.slice(0, 7)}`}</button>;
      },
    });
    await using screen = await render(bannerUi(props(environment("Mini", "b".repeat(40)))));
    expect(targets.length).toBeGreaterThan(0);
    expect(targets.at(-1)).toEqual({
      environmentId: "Mini",
      label: "Mini",
      targetSource: branding.APP_SOURCE_SHA,
    });
    expect(
      [...screen.container.querySelectorAll("button")].map((button) => button.textContent),
    ).toEqual([
      "Download aaaaaaa",
      "Dismiss client version notice",
    ]);
    // A newer stable server and equal sources never offer a download.
    targets.length = 0;
    for (const quiet of [
      environment("Stable", "c".repeat(40), "0.0.46"),
      environment("Equal", branding.APP_SOURCE_SHA),
    ])
      await screen.rerender(bannerUi(props(quiet)));
    expect(targets).toEqual([]);
    expect(screen.container.innerHTML).not.toContain("Download");
  });

  it("dismisses a notice together with its action and leaves other notices' actions", async () => {
    using _dismissals = cleanDismissals();
    const props = (ids: string[]): ClientUpdateBannerInput => ({
      activeEnvironment: null,
      automaticEnvironment: true,
      eligibleEnvironments: ids.map((id) => environment(id)),
      renderExactSourceAction: (target) => <span>{`action ${target.label}`}</span>,
    });
    await using screen = await render(bannerUi(props(["One", "Two"])));
    expect(screen.container.innerHTML).toContain("action One");
    expect(screen.container.innerHTML).toContain("action Two");
    await act(() => screen.container.querySelector("button")!.click());
    expect(screen.container.innerHTML).not.toContain("action One");
    expect(screen.container.innerHTML).toContain("action Two");
  });

  it("dismisses exact environment and full source pair across switches and remounts", async () => {
    using _dismissals = cleanDismissals();
    const props = (id: string, sha = "b".repeat(40)) => ({
      activeEnvironment: environment(id, sha),
      automaticEnvironment: false,
      eligibleEnvironments: [],
    });
    await using screen = await render(bannerUi(props("One")));
    await act(() => screen.container.querySelector("button")!.click());
    expect(screen.container.firstChild).toBeNull();
    await screen.rerender(bannerUi(props("Two")));
    expect(screen.container.innerHTML).toContain("Two");
    await screen.rerender(bannerUi(props("One")));
    expect(screen.container.firstChild).toBeNull();
    await screen.unmount();
    await using remounted = await render(bannerUi(props("One")));
    expect(remounted.container.firstChild).toBeNull();
    await remounted.rerender(bannerUi(props("One", "b".repeat(39) + "c")));
    expect(remounted.container.innerHTML).toContain("One");
    await remounted.rerender(bannerUi(props("One", "a".repeat(40))));
    expect(remounted.container.firstChild).toBeNull();
  });

  it("keeps automatic dismissals independent and offers client guidance for a newer stable server", async () => {
    using _dismissals = cleanDismissals();
    const first = environment("One");
    const second = environment("Two", "c".repeat(40), "0.0.46");
    const props = {
      activeEnvironment: null,
      automaticEnvironment: true,
      eligibleEnvironments: [first, second],
    };
    await using screen = await render(bannerUi(props));
    expect(screen.container.querySelectorAll("section")).toHaveLength(2);
    await act(() => screen.container.querySelector("button")!.click());
    expect(screen.container.querySelectorAll("section")).toHaveLength(1);
    expect(screen.container.innerHTML).toContain("Two");
    expect(screen.container.innerHTML).toContain("Update this app");
    await screen.rerender(bannerUi({ ...props, eligibleEnvironments: [first] }));
    expect(screen.container.firstChild).toBeNull();
    await screen.rerender(
      bannerUi({ ...props, eligibleEnvironments: [environment("One", "d".repeat(40))] }),
    );
    expect(screen.container.innerHTML).toContain("One");
  });
});
