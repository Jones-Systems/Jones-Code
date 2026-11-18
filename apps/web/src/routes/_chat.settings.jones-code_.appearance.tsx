import { createFileRoute } from "@tanstack/react-router";
import { JonesCodeAppearanceSettings } from "../jones/settings/JonesCodeAppearanceSettings";

export const Route = createFileRoute("/_chat/settings/jones-code_/appearance")({
  component: JonesCodeAppearanceSettings,
});
