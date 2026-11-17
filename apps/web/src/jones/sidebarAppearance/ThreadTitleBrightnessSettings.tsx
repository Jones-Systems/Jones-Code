import { SettingsRow, SettingsSection } from "../../components/settings/settingsLayout";
import { sidebarAppearanceScope } from "./model";
import { ThreadTitleBrightnessSlider } from "./ThreadTitleBrightnessSlider";
import { useSidebarAppearanceStore } from "./useSidebarAppearanceStore";
import "./sidebarAppearance.cards.css";

export function ThreadTitleBrightnessSettings() {
  const store = useSidebarAppearanceStore();
  const original = sidebarAppearanceScope({ ...store.stored, threadTitleBrightnessBoost: 0 });
  const adjusted = sidebarAppearanceScope(store.stored);
  return (
    <SettingsSection title="Sidebar">
      <SettingsRow
        title="Sidebar thread title brightness"
        description="Increases sidebar title contrast. Compare the same read thread before and after your adjustment."
      >
        <ThreadTitleBrightnessSlider
          value={store.stored.threadTitleBrightnessBoost}
          onChange={(threadTitleBrightnessBoost) => store.update({ threadTitleBrightnessBoost })}
        />
        <div className="mt-3 grid grid-cols-2 gap-2" aria-label="Thread title previews">
          {(
            [
              ["Original", original],
              ["Adjusted", adjusted],
            ] as const
          ).map(([label, scope]) => (
            <div key={label} {...scope.attributes} style={scope.variables}>
              <p className="mb-1 text-xs text-muted-foreground">{label}</p>
              <div data-jones-sidebar-card className="rounded-md bg-sidebar px-2 py-1.5">
                <span
                  data-jones-thread-title="receded"
                  data-jones-thread-title-branch="card"
                  className="text-sm font-normal text-foreground/75"
                >
                  Read thread
                </span>
              </div>
            </div>
          ))}
        </div>
        <p role="status">
          {store.saveFailed ? "Couldn't save; showing the saved appearance." : ""}
        </p>
      </SettingsRow>
    </SettingsSection>
  );
}
