import { useEffect, useState, type CSSProperties } from "react";
import { NonSortableThreadRow } from "../../components/sidebar/SidebarThreadRow";
import { SidebarAppearanceOverride } from "../sidebarAppearance/SidebarAppearanceContext";
import {
  DEFAULT_SIDEBAR_APPEARANCE,
  THREAD_TITLE_BRIGHTNESS_BOOST,
  sidebarAppearanceScope,
} from "../sidebarAppearance/model";
import { threadBoardAppearanceScope } from "../sidebarAppearance/threadBoardAppearance";
import { ThreadTitleBrightnessSlider } from "../sidebarAppearance/ThreadTitleBrightnessSlider";
import { ReviewActions, ReviewChoice, ReviewSurface, ReviewWindow } from "./ReviewSurface";
import { formatDesignReviewSource, type DesignReviewSource } from "./reviewSource";
import {
  REVIEW_THEME_OPTIONS,
  applyReviewTheme,
  followReviewHostTheme,
  type ReviewTheme,
} from "./reviewTheme";
import {
  THREAD_CARD_SCENARIOS,
  syntheticThreadCard,
  type ThreadCardScenario,
} from "./syntheticThreadCard";

const DEFAULT_BOOST = THREAD_TITLE_BRIGHTNESS_BOOST.default;
const COMPONENTS = ["NonSortableThreadRow", "ThreadTitleBrightnessSlider"] as const;
const BOARD_SCOPE = threadBoardAppearanceScope({});

function SidebarSample({
  boost,
  scenario,
  pane,
  label,
  nowMs,
}: {
  readonly boost: number;
  readonly scenario: ThreadCardScenario;
  readonly pane: string;
  readonly label: string;
  readonly nowMs: number;
}) {
  const appearance = { ...DEFAULT_SIDEBAR_APPEARANCE, threadTitleBrightnessBoost: boost };
  const scope = sidebarAppearanceScope(appearance);
  return (
    <SidebarAppearanceOverride value={appearance}>
      <section
        aria-label={label}
        className="surface-grain min-w-0 bg-sidebar p-2 text-sidebar-foreground"
        {...scope.attributes}
        {...BOARD_SCOPE.attributes}
        style={{ ...scope.variables, ...BOARD_SCOPE.variables } as CSSProperties}
      >
        <div
          data-sidebar="sidebar"
          data-slot="sidebar-inner"
          className="mx-auto flex w-full max-w-64 flex-col gap-1"
        >
          <p className="flex items-center justify-between gap-2 px-1 text-xs text-muted-foreground">
            <span>{label}</span>
            <output className="font-medium text-foreground tabular-nums">{boost}%</output>
          </p>
          <div data-jones-thread-board>
            <ul data-personal-thread-list aria-label="Workstream threads">
              <li data-personal-thread className="relative">
                <ul>
                  <NonSortableThreadRow {...syntheticThreadCard(scenario, pane, nowMs)} />
                </ul>
              </li>
            </ul>
          </div>
        </div>
      </section>
    </SidebarAppearanceOverride>
  );
}

export function ThreadCardBrightnessReview({
  nowMs,
  source,
}: {
  readonly nowMs: number;
  readonly source: DesignReviewSource;
}) {
  const [boost, setBoost] = useState<number>(DEFAULT_BOOST);
  const [scenario, setScenario] = useState<ThreadCardScenario>("read");
  const [theme, setTheme] = useState<ReviewTheme>("app");
  const changeTheme = (next: ReviewTheme) => {
    applyReviewTheme(next);
    setTheme(next);
  };

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Follow the inline viewer's palette without resetting the review.
  useEffect(() => followReviewHostTheme(() => theme), [theme]);

  const scenarioLabel = THREAD_CARD_SCENARIOS.find((option) => option.value === scenario)!.label;
  const themeLabel = REVIEW_THEME_OPTIONS.find((option) => option.value === theme)!.label;
  return (
    <ReviewSurface
      title="Thread title brightness"
      summary="Drag from 0% toward 100%. The adjustable card changes as you drag."
      source={source}
      components={COMPONENTS}
    >
      <section aria-label="Preview controls" data-design-review-chrome className="grid gap-2">
        <ReviewChoice
          label="Card state"
          options={THREAD_CARD_SCENARIOS}
          value={scenario}
          onChange={setScenario}
        />
        <ReviewChoice<ReviewTheme>
          label="Theme"
          options={REVIEW_THEME_OPTIONS}
          value={theme}
          onChange={changeTheme}
        />
        <div className="max-w-md">
          <ThreadTitleBrightnessSlider value={boost} onChange={setBoost} />
        </div>
      </section>

      <ReviewWindow caption={`Workstream sidebar · ${scenarioLabel} card`}>
        <div className="grid gap-px bg-border min-[420px]:grid-cols-2">
          <SidebarSample
            label="Default"
            boost={DEFAULT_BOOST}
            scenario={scenario}
            pane="baseline"
            nowMs={nowMs}
          />
          <SidebarSample
            label="Adjustable"
            boost={boost}
            scenario={scenario}
            pane="candidate"
            nowMs={nowMs}
          />
        </div>
      </ReviewWindow>

      <ReviewActions
        resetLabel={`Reset to ${DEFAULT_BOOST}%`}
        onReset={() => setBoost(DEFAULT_BOOST)}
        onRestart={() => {
          setBoost(DEFAULT_BOOST);
          setScenario("read");
          changeTheme("app");
        }}
        copyKey={`${scenario}:${theme}:${boost}`}
        copyText={() =>
          [
            "Thread title brightness review",
            `Setting: threadTitleBrightnessBoost = ${boost}`,
            `Baseline: ${DEFAULT_BOOST}%`,
            `Checked on: ${scenarioLabel} card · ${themeLabel}`,
            `Source: ${formatDesignReviewSource(source)}`,
          ].join("\n")
        }
      />
    </ReviewSurface>
  );
}
