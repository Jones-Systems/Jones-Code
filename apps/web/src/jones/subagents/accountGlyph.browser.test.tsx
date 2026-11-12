import { RegistryContext } from "@effect/atom-react";
import {
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ServerProvider,
} from "@t3tools/contracts";
import { Atom, AtomRegistry } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";
import { render } from "../../../test/render";
import { EnvironmentSubagentAvatar } from "../../components/chat/V2LifecycleRow";

const state = vi.hoisted(() => ({
  providers: new Map<string, Atom.Atom<ReadonlyArray<ServerProvider>>>(),
  empty: null as unknown as Atom.Atom<ReadonlyArray<ServerProvider>>,
  settings: null as unknown as Atom.Atom<null>,
}));
vi.mock(import("../../state/server"), async (original) => ({
  ...(await original()),
  serverEnvironment: {
    ...(await original()).serverEnvironment,
    providersValueAtom: (id: string) => state.providers.get(id) ?? state.empty,
    settingsValueAtom: () => state.settings,
  },
}));

it("renders the owning environment account and retains the fallback when a remote account is unknown", async () => {
  const local = EnvironmentId.make("account-glyph-local");
  const remote = EnvironmentId.make("account-glyph-remote");
  const instanceId = ProviderInstanceId.make("same-id-on-both-machines");
  const driver = ProviderDriverKind.make("codex");
  const provider = (displayName: string): ServerProvider => ({
    instanceId,
    driver,
    displayName,
    enabled: true,
    installed: true,
    version: null,
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-11-04T00:00:00Z",
    models: [],
    slashCommands: [],
    skills: [],
  });
  state.empty = Atom.make([]);
  state.settings = Atom.make(null);
  state.providers = new Map([
    [local, Atom.make([provider("Codex Malcolm")])],
    [remote, Atom.make([provider("Codex Jace")])],
  ]);
  const registry = AtomRegistry.make();
  try {
    await using screen = await render(
      <RegistryContext value={registry}>
        <div data-case="local">
          <EnvironmentSubagentAvatar
            environmentId={local}
            providerInstanceId={instanceId}
            driver={driver}
          />
        </div>
        <div data-case="remote">
          <EnvironmentSubagentAvatar
            environmentId={local}
            providerInstanceId={instanceId}
            driver={ProviderDriverKind.make("remote")}
            remote={{
              environmentId: remote,
              threadId: ThreadId.make("remote-child"),
              label: "Remote child",
            }}
          />
        </div>
        <div data-case="unknown">
          <EnvironmentSubagentAvatar
            environmentId={local}
            providerInstanceId={instanceId}
            driver={ProviderDriverKind.make("remote")}
            remote={{
              environmentId: EnvironmentId.make("unknown-remote"),
              threadId: ThreadId.make("unknown-child"),
              label: "Unknown child",
            }}
          />
        </div>
      </RegistryContext>,
      { router: false },
    );
    expect(
      screen.container.querySelector('[data-case="local"] [data-provider-shortcut-label] defs')
        ?.textContent,
    ).toBe("CM");
    expect(
      screen.container.querySelector('[data-case="remote"] [data-provider-shortcut-label] defs')
        ?.textContent,
    ).toBe("CJ");
    expect(
      screen.container.querySelector('[data-case="unknown"] [data-provider-shortcut-label]'),
    ).toBeNull();
  } finally {
    registry.dispose();
  }
});
