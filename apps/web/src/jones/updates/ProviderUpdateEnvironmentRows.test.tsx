import type { Dispatch, ReactElement, SetStateAction } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import {
  AuthProvidersManageScope,
  type AuthEnvironmentScope,
  type EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";

import { buildLocalEnvironmentUpdateGroups } from "~/components/ProviderUpdateLaunchNotification.logic";
import type {
  LocalEnvironmentUpdateGroup,
  ProviderUpdateCandidate,
  ProviderUpdateRowStatus,
} from "~/components/ProviderUpdateLaunchNotification.logic";

const testState = vi.hoisted(() => ({
  groups: [] as LocalEnvironmentUpdateGroup[],
  permittedEnvironmentIds: new Set<EnvironmentId>(),
  updateProvider: vi.fn(),
}));

const hooks = vi.hoisted(() => {
  let cursor = 0;
  let slots: unknown[] = [];

  const nextIndex = () => cursor++;

  return {
    beginRender() {
      cursor = 0;
    },
    reset() {
      cursor = 0;
      slots = [];
    },
    useCallback<T>(callback: T): T {
      nextIndex();
      return callback;
    },
    useMemo<T>(factory: () => T): T {
      nextIndex();
      return factory();
    },
    useMemoCache(size: number): unknown[] {
      const index = nextIndex();
      if (!slots[index]) {
        slots[index] = Array.from({ length: size }, () => Symbol.for("react.memo_cache_sentinel"));
      }
      return slots[index] as unknown[];
    },
    useRef<T>(initialValue: T): { current: T } {
      const index = nextIndex();
      if (!slots[index]) {
        slots[index] = { current: initialValue };
      }
      return slots[index] as { current: T };
    },
    useState<T>(initialValue: T | (() => T)): [T, Dispatch<SetStateAction<T>>] {
      const index = nextIndex();
      if (index >= slots.length) {
        slots[index] =
          typeof initialValue === "function" ? (initialValue as () => T)() : initialValue;
      }
      const setValue: Dispatch<SetStateAction<T>> = (nextValue) => {
        const previous = slots[index] as T;
        slots[index] =
          typeof nextValue === "function" ? (nextValue as (value: T) => T)(previous) : nextValue;
      };
      return [slots[index] as T, setValue];
    },
  };
});

vi.mock("react", async (importOriginal) => {
  const actual = await importOriginal<typeof import("react")>();
  return {
    ...actual,
    useCallback: hooks.useCallback,
    useMemo: hooks.useMemo,
    useRef: hooks.useRef,
    useState: hooks.useState,
  };
});

vi.mock("react/compiler-runtime", () => ({
  c: hooks.useMemoCache,
}));

vi.mock("~/state/server", () => ({
  serverEnvironment: { updateProvider: Symbol("updateProvider") },
}));

vi.mock("~/state/session", () => ({
  readEnvironmentScope: (environmentId: EnvironmentId, scope: AuthEnvironmentScope) =>
    scope === AuthProvidersManageScope && testState.permittedEnvironmentIds.has(environmentId),
  useEnvironmentScope: (environmentId: EnvironmentId | null, scope: AuthEnvironmentScope) =>
    environmentId !== null &&
    scope === AuthProvidersManageScope &&
    testState.permittedEnvironmentIds.has(environmentId),
}));

vi.mock("~/state/use-atom-command", () => ({
  useAtomCommand: () => testState.updateProvider,
}));

vi.mock("~/components/ProviderUpdateLaunchNotification.environments", () => ({
  useLocalEnvironmentUpdateGroups: () => ({
    groups: testState.groups,
    isAnySettling: false,
  }),
}));

import { ProviderUpdateEnvironmentRows } from "~/components/ProviderUpdateEnvironmentRows";

const environmentId = "env-wsl" as EnvironmentId;

function provider(updateStatus?: "succeeded"): ServerProvider {
  const result: ServerProvider = {
    instanceId: ProviderInstanceId.make("codex-wsl"),
    driver: ProviderDriverKind.make("codex"),
    enabled: true,
    installed: true,
    version: updateStatus ? "1.1.0" : "1.0.0",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-06-26T12:00:00.000Z",
    models: [],
    slashCommands: [],
    skills: [],
    versionAdvisory: {
      status: updateStatus ? "current" : "behind_latest",
      currentVersion: updateStatus ? "1.1.0" : "1.0.0",
      latestVersion: "1.1.0",
      updateCommand: "npm install -g @openai/codex@latest",
      canUpdate: true,
      checkedAt: "2026-06-26T12:00:00.000Z",
      message: updateStatus ? "Up to date." : "Update available.",
    },
  };

  return updateStatus
    ? {
        ...result,
        updateState: {
          status: updateStatus,
          startedAt: "2026-06-26T12:00:00.000Z",
          finishedAt: "2026-06-26T12:00:01.000Z",
          message: "Provider updated.",
          output: null,
        },
      }
    : result;
}

type RowElement = ReactElement<{
  readonly status: ProviderUpdateRowStatus;
  readonly onUpdate: () => void;
}>;

async function flushPromises(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

describe("ProviderUpdateEnvironmentRows", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    hooks.reset();
    testState.permittedEnvironmentIds.clear();
    testState.permittedEnvironmentIds.add(environmentId);
    testState.updateProvider.mockReset();
    const candidate = provider() as ProviderUpdateCandidate;
    testState.groups = [
      {
        environmentId,
        label: "WSL",
        isPrimary: false,
        isSettling: false,
        candidates: [candidate],
        providers: [candidate],
      },
    ];
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it.each(["npm install -g @openai/codex@latest", "bun add -g @openai/codex@latest"])(
    "dispatches and completes every environment instance with sibling command %s",
    async (updateCommand) => {
      const secondEnvironmentId = "env-primary" as EnvironmentId;
      const personal = { ...provider(), instanceId: ProviderInstanceId.make("personal") };
      const work = {
        ...provider(),
        instanceId: ProviderInstanceId.make("work"),
        versionAdvisory: { ...provider().versionAdvisory!, updateCommand },
      };
      testState.permittedEnvironmentIds.add(secondEnvironmentId);
      testState.groups = buildLocalEnvironmentUpdateGroups(
        [environmentId, secondEnvironmentId].map((id) => ({
          environmentId: id,
          label: id,
          isPrimary: id === secondEnvironmentId,
          connectionState: "ready",
          providers: [personal, work, personal],
        })),
      ).groups;
      testState.updateProvider.mockImplementation(async () =>
        AsyncResult.success({
          providers: [personal, work].map((snapshot) => ({
            ...provider("succeeded"),
            instanceId: snapshot.instanceId,
          })),
        }),
      );
      const renderRows = () => {
        hooks.beginRender();
        return (
          ProviderUpdateEnvironmentRows({}) as ReactElement<{
            readonly children: RowElement[];
          }>
        ).props.children;
      };

      for (const row of renderRows()) row.props.onUpdate();
      await flushPromises();

      expect(
        testState.updateProvider.mock.calls.map(([request]) => [
          request.environmentId,
          request.input.instanceId,
        ]),
      ).toEqual([
        [environmentId, "personal"],
        [environmentId, "work"],
        [secondEnvironmentId, "personal"],
        [secondEnvironmentId, "work"],
      ]);
      expect(renderRows().map((row) => row.props.status.kind)).toEqual(["success", "success"]);
    },
  );
});
