import {
  SettingsPageContainer,
  useSettingsSearchTarget,
} from "../../components/settings/settingsLayout";
import { ThreadBoardAppearanceSettings } from "../sidebarAppearance/ThreadBoardAppearanceSettings";
import { ThreadTitleBrightnessSettings } from "../sidebarAppearance/ThreadTitleBrightnessSettings";
import { WorkButtonSettings } from "../workMode/WorkButtonSettings";

const TITLE_BRIGHTNESS_ANCHOR = "sidebar-thread-title-brightness";

function SidebarThreadTitleAppearanceSettings() {
  const titleBrightnessTarget = useSettingsSearchTarget<HTMLDivElement>(TITLE_BRIGHTNESS_ANCHOR);
  return (
    <div id={TITLE_BRIGHTNESS_ANCHOR} ref={titleBrightnessTarget} tabIndex={-1}>
      <ThreadTitleBrightnessSettings />
    </div>
  );
}

export function JonesCodeAppearanceSettings() {
  return (
    <SettingsPageContainer>
      <ThreadBoardAppearanceSettings />
      <SidebarThreadTitleAppearanceSettings />
      <WorkButtonSettings />
    </SettingsPageContainer>
  );
}
