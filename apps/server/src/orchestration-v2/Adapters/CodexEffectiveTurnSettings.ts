import { ProviderDriverKind, type ModelSelection, type ProviderInstanceId, type ServerProviderModel } from "@t3tools/contracts";
import {
  codexModelFamily,
  getConfiguredReasoningEffort,
  getProviderOptionCurrentValue,
  normalizeModelSlug,
} from "@t3tools/shared/model";
import * as CodexSchema from "effect-codex-app-server/schema";
import * as Schema from "effect/Schema";

import { getCodexServiceTierOptionValue } from "../../codexModelOptions.ts";

export interface CodexEffectiveTurnSettings {
  readonly model: string;
  readonly effort?: CodexSchema.V2TurnStartParams__ReasoningEffort;
  readonly serviceTier?: string;
}

export type CodexEffectiveTurnSettingsResult =
  | { readonly type: "resolved"; readonly settings: CodexEffectiveTurnSettings }
  | { readonly type: "rejected"; readonly reason: "instance_mismatch" | "invalid_native_settings" };

interface CodexTurnSelectionInput {
  readonly instanceId: ProviderInstanceId;
  readonly selection: ModelSelection;
}

const codexDriver = ProviderDriverKind.make("codex");

const isNativeTurnSettings = Schema.is(
  Schema.Struct({
    model: CodexSchema.V2TurnStartParams.fields.model,
    effort: CodexSchema.V2TurnStartParams.fields.effort,
    serviceTier: CodexSchema.V2TurnStartParams.fields.serviceTier,
  }),
);

export function codexTurnNeedsModelCatalog(input: CodexTurnSelectionInput): boolean {
  return (
    input.selection.instanceId === input.instanceId &&
    !input.selection.options?.some((option) => option.id === "reasoningEffort")
  );
}

export function resolveCodexEffectiveTurnSettings(
  input: CodexTurnSelectionInput & {
    readonly models?: ReadonlyArray<ServerProviderModel>;
    readonly managed: boolean;
    readonly configuredDefaultModelSelection?: {
      readonly modelSelection: ModelSelection;
      readonly driver: ProviderDriverKind;
    };
  },
): CodexEffectiveTurnSettingsResult {
  if (input.selection.instanceId !== input.instanceId) {
    return { type: "rejected", reason: "instance_mismatch" };
  }

  const explicitEffort = input.selection.options?.find((option) => option.id === "reasoningEffort");
  // Defaults are dispatch-only. Explicit choices read neither the catalog nor
  // the current configured default, and neither default mutates the selection.
  let effort = explicitEffort?.value;
  if (explicitEffort === undefined) {
    const models = input.models;
    const canonicalModel = normalizeModelSlug(codexModelFamily(input.selection.model), codexDriver);
    const model = models?.find((model) => model.slug === input.selection.model) ??
      models?.find((model) => [model.slug, ...(model.aliases ?? [])].some(
        (slug) => normalizeModelSlug(codexModelFamily(slug), codexDriver) === canonicalModel,
      ));
    const configured = input.configuredDefaultModelSelection;
    effort = getConfiguredReasoningEffort({
      modelSelection: input.selection,
      driverKind: codexDriver,
      capabilities: model?.capabilities ?? undefined,
      defaultModelSelection: configured?.modelSelection,
      defaultDriverKind: configured?.driver,
    }) ?? getProviderOptionCurrentValue(
      model?.capabilities?.optionDescriptors?.find(
        (option) => option.id === "reasoningEffort" && option.type === "select",
      ),
    );
  }
  const serviceTier = input.managed ? undefined : getCodexServiceTierOptionValue(input.selection);
  const settings = {
    model: input.selection.model,
    ...(effort === undefined ? {} : { effort }),
    ...(serviceTier === undefined ? {} : { serviceTier }),
  };
  if (!isNativeTurnSettings(settings)) {
    return { type: "rejected", reason: "invalid_native_settings" };
  }
  return { type: "resolved", settings };
}
