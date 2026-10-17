import { act } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { EnvironmentId } from "@t3tools/contracts";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { useClientUpdateBanners, type ClientUpdateBannerInput } from "./clientUpdateBanner";

const branding = vi.hoisted(() => ({
  APP_VERSION: "0.0.45-preview.20261010.1",
  APP_SOURCE_SHA: "a".repeat(40),
}));
vi.mock("../../branding", () => branding);

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
  const values = new Map<string, string>();
  vi.stubGlobal("window", {
    localStorage: {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => values.set(key, value),
    },
  });
});
afterEach(() => {
  act(() => tree?.unmount());
  tree = undefined;
  vi.unstubAllGlobals();
});

describe("client notice environment lifecycle", () => {
  it("selects active or every eligible automatic environment and never mounts update actions", () => {
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
