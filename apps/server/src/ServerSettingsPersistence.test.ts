import { assert, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Cause from "effect/Cause";
import { decodePersistedServerSettings } from "./ServerSettingsPersistence.ts";

const projectId = ProjectId.make("project:persisted-settings");

it("treats missing raw settings as trusted defaults without persisted provider flags", () => {
  const result = decodePersistedServerSettings(undefined);
  assert.strictEqual(result.settings, DEFAULT_SERVER_SETTINGS);
  assert.deepEqual(result.persisted, {});
  assert.isTrue(result.settingsFileTrusted);
  assert.isUndefined(result.cause);
  assert.isFalse(result.settings.continueThreadsAfterServerUpdate);
});

it("decodes sparse settings without inventing explicit optional-provider flags", () => {
  const result = decodePersistedServerSettings("{}");
  assert.deepEqual(result.settings, DEFAULT_SERVER_SETTINGS);
  assert.deepEqual(result.persisted, {});
  assert.isTrue(result.settingsFileTrusted);
  assert.isUndefined(result.cause);
});

it("accepts comments and trailing commas in both persisted decoders", () => {
  const result = decodePersistedServerSettings(`{
    // Preserve explicit flags separately from decoded defaults.
    "continueThreadsAfterServerUpdate": true,
    "providers": { "cursor": { "enabled": false, }, },
  }`);
  assert.isTrue(result.settingsFileTrusted);
  assert.isUndefined(result.cause);
  assert.isTrue(result.settings.continueThreadsAfterServerUpdate);
  assert.deepEqual(result.persisted, { providers: { cursor: { enabled: false } } });
});

it("migrates saved token delivery globally and per project without discarding preferences", () => {
  const result = decodePersistedServerSettings(
    JSON.stringify({
      responseStreamingMode: "token",
      continueThreadsAfterServerUpdate: true,
      projectSettingsOverrides: {
        [projectId]: { responseStreamingMode: "token", continueThreadsAfterServerUpdate: false },
      },
    }),
  );
  assert.isTrue(result.settingsFileTrusted);
  assert.isUndefined(result.cause);
  assert.equal(result.settings.responseStreamingMode, "paragraph");
  assert.isTrue(result.settings.continueThreadsAfterServerUpdate);
  assert.deepEqual(result.settings.projectSettingsOverrides[projectId], {
    responseStreamingMode: "paragraph",
    continueThreadsAfterServerUpdate: false,
  });
});

it("falls back the whole settings value while retaining independently valid provider flags", () => {
  const result = decodePersistedServerSettings(
    JSON.stringify({
      addProjectBaseDirectory: 42,
      continueThreadsAfterServerUpdate: true,
      projectSettingsOverrides: { [projectId]: { continueThreadsAfterServerUpdate: true } },
      providers: { cursor: { enabled: false }, grok: { enabled: true } },
    }),
  );
  assert.strictEqual(result.settings, DEFAULT_SERVER_SETTINGS);
  assert.isFalse(result.settingsFileTrusted);
  assert.isDefined(result.cause);
  assert.deepEqual(result.persisted, {
    providers: { cursor: { enabled: false }, grok: { enabled: true } },
  });
  assert.isFalse(
    resolveProjectSettings(result.settings, projectId).settings.continueThreadsAfterServerUpdate,
  );
});

it("rejects invalid optional-provider flags instead of salvaging continuation opt-in", () => {
  const result = decodePersistedServerSettings(
    JSON.stringify({
      continueThreadsAfterServerUpdate: true,
      providers: { cursor: { enabled: "false" } },
    }),
  );
  assert.strictEqual(result.settings, DEFAULT_SERVER_SETTINGS);
  assert.deepEqual(result.persisted, {});
  assert.isFalse(result.settingsFileTrusted);
  assert.isDefined(result.cause);
});

it("retains a decode cause for malformed or empty files", () => {
  for (const raw of ["", "{invalid json"]) {
    const result = decodePersistedServerSettings(raw);
    assert.strictEqual(result.settings, DEFAULT_SERVER_SETTINGS);
    assert.deepEqual(result.persisted, {});
    assert.isFalse(result.settingsFileTrusted);
    assert.isDefined(result.cause);
  }
});

it("reports the full-settings decode failure when both decoders fail", () => {
  const result = decodePersistedServerSettings(
    JSON.stringify({
      addProjectBaseDirectory: 42,
      providers: { cursor: { enabled: "false" } },
    }),
  );
  assert.isFalse(result.settingsFileTrusted);
  assert.isDefined(result.cause);
  assert.include(Cause.pretty(result.cause!), "addProjectBaseDirectory");
});

it("preserves global continuation preferences and project overrides in both directions", () => {
  for (const global of [false, true]) {
    for (const project of [undefined, false, true]) {
      const result = decodePersistedServerSettings(
        JSON.stringify({
          continueThreadsAfterServerUpdate: global,
          projectSettingsOverrides: {
            [projectId]: project === undefined ? {} : { continueThreadsAfterServerUpdate: project },
          },
        }),
      );
      assert.isTrue(result.settingsFileTrusted);
      assert.isUndefined(result.cause);
      assert.equal(
        resolveProjectSettings(result.settings, projectId).settings
          .continueThreadsAfterServerUpdate,
        project ?? global,
      );
    }
  }
});

it("leaves enabled-flag normalization to the live loader", () => {
  const result = decodePersistedServerSettings(
    JSON.stringify({
      providerInstances: {
        grok: { driver: "grok", enabled: true, config: { enabled: false } },
      },
    }),
  );
  assert.isTrue(result.settingsFileTrusted);
  assert.deepEqual(result.settings.providerInstances[ProviderInstanceId.make("grok")], {
    driver: ProviderDriverKind.make("grok"),
    enabled: true,
    config: { enabled: false },
  });
});
