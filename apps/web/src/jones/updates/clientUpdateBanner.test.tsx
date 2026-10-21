// @vitest-environment jsdom
import { act } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { EnvironmentId } from "@t3tools/contracts";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
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

let tree: ReactTestRenderer | undefined;
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
function render(props: ClientUpdateBannerInput) {
  act(() => {
    if (tree) tree.update(<Harness {...props} />);
    else tree = create(<Harness {...props} />);
  });
}
const output = () => JSON.stringify(tree!.toJSON());
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  window.localStorage.clear();
});
afterEach(() => {
  act(() => tree?.unmount());
  tree = undefined;
  vi.unstubAllGlobals();
});

describe("client notice environment lifecycle", () => {
  it("selects active or every eligible automatic environment", () => {
    const active = environment("Active");
    const eligible = [environment("Eligible"), environment("Equal", "a".repeat(40))];
    render({
      activeEnvironment: active,
      automaticEnvironment: false,
      eligibleEnvironments: eligible,
    });
    expect(output()).toContain("Active");
    expect(output()).not.toContain("Eligible");
    expect(output()).toContain("Client");
    expect(output()).toContain("Server");
    expect(output()).toContain("aaaaaaaaaaaa");
    expect(output()).toContain("1020·1713:43");
    expect(output()).toContain("1020·1713:47");
    expect(tree!.root.findAllByType("button").map((b) => b.children.join(""))).toEqual([
      "Dismiss client version notice",
    ]);
    render({
      activeEnvironment: active,
      automaticEnvironment: true,
      eligibleEnvironments: eligible,
    });
    expect(output()).toContain("Eligible");
    expect(output()).not.toContain("Active");
    expect(output()).not.toContain("Equal");
    render({
      activeEnvironment: null,
      automaticEnvironment: false,
      eligibleEnvironments: eligible,
    });
    expect(tree!.toJSON()).toBeNull();
  });

  it("targets this app's own full source, and only for a preview source difference", () => {
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
    render(props(environment("Mini", "b".repeat(40))));
    expect(targets.length).toBeGreaterThan(0);
    expect(targets.at(-1)).toEqual({
      environmentId: "Mini",
      label: "Mini",
      targetSource: branding.APP_SOURCE_SHA,
    });
    expect(tree!.root.findAllByType("button").map((b) => b.children.join(""))).toEqual([
      "Download aaaaaaa",
      "Dismiss client version notice",
    ]);
    // A newer stable server, equal sources and a missing source never offer a download.
    targets.length = 0;
    for (const quiet of [
      environment("Stable", "c".repeat(40), "0.0.46"),
      environment("Equal", branding.APP_SOURCE_SHA),
    ])
      render(props(quiet));
    expect(targets).toEqual([]);
    expect(output()).not.toContain("Download");
  });

  it("dismisses a notice together with its action and leaves other notices' actions", () => {
    const props = (ids: string[]): ClientUpdateBannerInput => ({
      activeEnvironment: null,
      automaticEnvironment: true,
      eligibleEnvironments: ids.map((id) => environment(id)),
      renderExactSourceAction: (target) => <span>{`action ${target.label}`}</span>,
    });
    render(props(["One", "Two"]));
    expect(output()).toContain("action One");
    expect(output()).toContain("action Two");
    act(() => tree!.root.findAllByType("button")[0]!.props.onClick());
    expect(output()).not.toContain("action One");
    expect(output()).toContain("action Two");
  });

  it("dismisses exact environment and full source pair across switches and remounts", () => {
    const props = (id: string, sha = "b".repeat(40)) => ({
      activeEnvironment: environment(id, sha),
      automaticEnvironment: false,
      eligibleEnvironments: [],
    });
    render(props("One"));
    act(() => tree!.root.findByType("button").props.onClick());
    expect(tree!.toJSON()).toBeNull();
    render(props("Two"));
    expect(output()).toContain("Two");
    render(props("One"));
    expect(tree!.toJSON()).toBeNull();
    act(() => tree!.unmount());
    tree = undefined;
    render(props("One"));
    expect(tree!.toJSON()).toBeNull();
    render(props("One", "b".repeat(39) + "c"));
    expect(output()).toContain("One");
    render(props("One", "a".repeat(40)));
    expect(tree!.toJSON()).toBeNull();
  });

  it("keeps automatic dismissals independent and offers client guidance for a newer stable server", () => {
    const first = environment("One");
    const second = environment("Two", "c".repeat(40), "0.0.46");
    const props = {
      activeEnvironment: null,
      automaticEnvironment: true,
      eligibleEnvironments: [first, second],
    };
    render(props);
    expect(tree!.root.findAllByType("section")).toHaveLength(2);
    act(() => tree!.root.findAllByType("button")[0]!.props.onClick());
    expect(tree!.root.findAllByType("section")).toHaveLength(1);
    expect(output()).toContain("Two");
    expect(output()).toContain("Update this app");
    render({ ...props, eligibleEnvironments: [first] });
    expect(tree!.toJSON()).toBeNull();
    render({ ...props, eligibleEnvironments: [environment("One", "d".repeat(40))] });
    expect(output()).toContain("One");
  });
});
