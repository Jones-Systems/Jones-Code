import {
  BookOpenIcon,
  ChartNoAxesColumnIcon,
  ListTodoIcon,
  MicIcon,
  SettingsIcon,
} from "lucide-react";
import { useLocation, useNavigate } from "@tanstack/react-router";
import { memo } from "react";
import { usePullRequestsSupported } from "../../state/environments";
import {
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from "../../components/ui/sidebar";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { PullRequestGlyph } from "../../components/pullRequest/pullRequestIcons";
import { readPullRequestListPreferences } from "../../components/pullRequest/pullRequestListPreferences";
import { useNavigateToMainApp } from "../../components/sidebar/mainAppLocation";
import { SidebarUpdatePill } from "../../components/sidebar/SidebarUpdatePill";

const destinations = [
  {
    path: "/settings",
    label: "Settings",
    icon: SettingsIcon,
    description: "App and environment settings",
  },
  {
    path: "/pull-requests",
    label: "Pull Requests",
    icon: PullRequestGlyph.pullRequest,
    description: "Browse and manage pull requests",
  },
  {
    path: "/usage",
    label: "Usage",
    icon: ChartNoAxesColumnIcon,
    description: "Provider usage and token accounting",
  },
  {
    path: "/work-queue",
    label: "Submitted work",
    icon: ListTodoIcon,
    description: "Work already submitted for routing and delivery",
  },
  {
    path: "/voice-review",
    label: "Queue",
    icon: MicIcon,
    description: "Review, edit, or pause voice prompts before release",
  },
  {
    path: "/conversations",
    label: "Conversation Library",
    icon: BookOpenIcon,
    description: "Browse imported ChatGPT conversations",
  },
] as const;

export const JonesSidebarUtilityMenu = memo(function JonesSidebarUtilityMenu() {
  const navigate = useNavigate();
  const returnToConversation = useNavigateToMainApp();
  const pathname = useLocation({ select: (location) => location.pathname });
  const { isMobile, setOpenMobile } = useSidebar();
  const pullRequestsSupported = usePullRequestsSupported();

  return (
    <SidebarMenu className="flex-row items-center">
      {destinations.map(({ path, label, icon: Icon, description }) => {
        if (path === "/pull-requests" && !pullRequestsSupported) return null;
        const active =
          pathname === path ||
          (path === "/settings" &&
            (pathname.startsWith("/settings/") || pathname.startsWith("/projects/")));
        return (
          <SidebarMenuItem key={path} className="shrink-0">
            <Tooltip>
              <TooltipTrigger
                render={
                  <SidebarMenuButton
                    aria-label={label}
                    aria-current={active ? "page" : undefined}
                    isActive={active}
                    size="icon"
                    onClick={() => {
                      if (isMobile) setOpenMobile(false);
                      if (active) {
                        void returnToConversation();
                        return;
                      }
                      if (path === "/pull-requests") {
                        void navigate({ to: path, search: readPullRequestListPreferences() });
                      } else {
                        void navigate({ to: path });
                      }
                    }}
                  >
                    <Icon />
                  </SidebarMenuButton>
                }
              />
              <TooltipPopup side="top">
                {active
                  ? `${label} · Click again to return to your conversation`
                  : `${label} · ${description}`}
              </TooltipPopup>
            </Tooltip>
          </SidebarMenuItem>
        );
      })}
      <SidebarUpdatePill />
    </SidebarMenu>
  );
});
