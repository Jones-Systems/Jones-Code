import {
  useId,
  useLayoutEffect,
  useRef,
  useState,
  type CSSProperties,
  type RefObject,
} from "react";
import { NonSortableThreadRow } from "../../components/sidebar/SidebarThreadRow";
import { cn } from "../../lib/utils";
import { SidebarAppearanceOverride } from "../sidebarAppearance/SidebarAppearanceContext";
import {
  DEFAULT_SIDEBAR_APPEARANCE,
  SIDEBAR_APPEARANCE_SCOPE_ATTRIBUTE_NAMES,
  SIDEBAR_APPEARANCE_SCOPE_VARIABLE_NAMES,
  THREAD_TITLE_BRIGHTNESS_BOOST,
  sidebarAppearanceScope,
  type SidebarAppearance,
} from "../sidebarAppearance/model";
import { threadBoardAppearanceScope } from "../sidebarAppearance/threadBoardAppearance";
import { ThreadTitleBrightnessSlider } from "../sidebarAppearance/ThreadTitleBrightnessSlider";
import {
  contrastLevel,
  formatContrast,
  measureTextContrast,
  measureTextContrastAcross,
} from "./contrast";
import {
  ReviewActions,
  ReviewChoice,
  ReviewComparison,
  ReviewPane,
  ReviewSurface,
} from "./ReviewSurface";
import { formatDesignReviewSource, type DesignReviewSource } from "./reviewSource";
import { REVIEW_THEME_OPTIONS, applyReviewTheme, type ReviewTheme } from "./reviewTheme";
import {
  THREAD_CARD_SCENARIOS,
  syntheticThreadCard,
  type ThreadCardScenario,
} from "./syntheticThreadCard";

const DEFAULT_BOOST = THREAD_TITLE_BRIGHTNESS_BOOST.default;
const SWEEP_VALUES = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100] as const;
const COMPONENTS = ["NonSortableThreadRow", "ThreadTitleBrightnessSlider"] as const;
/** The live sidebar also scopes the stored thread board settings; a review uses their defaults. */
const BOARD_SCOPE = threadBoardAppearanceScope({});
const TITLE_SELECTOR = "[data-jones-thread-title]";

function withBoost(boost: number): SidebarAppearance {
  return { ...DEFAULT_SIDEBAR_APPEARANCE, threadTitleBrightnessBoost: boost };
}

/** Restyles a rendered scope for one boost with the same attributes the product scope sets. */
function applyBoostScope(scope: HTMLElement, boost: number): void {
  const next = sidebarAppearanceScope(withBoost(boost));
  for (const name of SIDEBAR_APPEARANCE_SCOPE_ATTRIBUTE_NAMES) {
    const value = next.attributes[name];
    if (value === undefined) scope.removeAttribute(name);
    else scope.setAttribute(name, value);
  }
  for (const name of SIDEBAR_APPEARANCE_SCOPE_VARIABLE_NAMES) {
    const value = next.variables[name];
    if (value === undefined) scope.style.removeProperty(name);
    else scope.style.setProperty(name, value);
  }
}

function titleIn(scope: RefObject<HTMLElement | null>): Element | null {
  return scope.current?.querySelector(TITLE_SELECTOR) ?? null;
}

function describeContrast(ratio: number | null): string {
  const level = contrastLevel(ratio);
  return level ? `${formatContrast(ratio)} ${level}` : formatContrast(ratio);
}

/** The same synthetic card inside the scope and Workstream list markup the live sidebar uses. */
function SidebarCard({
  appearance,
  scenario,
  pane,
  nowMs,
  scopeRef,
}: {
  readonly appearance: SidebarAppearance;
  readonly scenario: ThreadCardScenario;
  readonly pane: string;
  readonly nowMs: number;
  readonly scopeRef: RefObject<HTMLDivElement | null>;
}) {
  const scope = sidebarAppearanceScope(appearance);
  return (
    <SidebarAppearanceOverride value={appearance}>
      <div
        ref={scopeRef}
        {...scope.attributes}
        {...BOARD_SCOPE.attributes}
        style={{ ...scope.variables, ...BOARD_SCOPE.variables }}
      >
        <div
          data-sidebar="sidebar"
          data-slot="sidebar-inner"
          className="w-full max-w-(--sidebar-width) rounded-md bg-sidebar surface-grain p-2 text-sidebar-foreground"
          style={{ "--sidebar-width": "16rem" } as CSSProperties}
        >
          <div data-jones-thread-board aria-label="Workstreams">
            <ul data-personal-thread-list aria-label="Synthetic threads">
              <li data-personal-thread className="relative">
                <ul>
                  <NonSortableThreadRow {...syntheticThreadCard(scenario, pane, nowMs)} />
                </ul>
              </li>
            </ul>
          </div>
        </div>
      </div>
    </SidebarAppearanceOverride>
  );
}

interface ReviewProps {
  readonly initialTheme: ReviewTheme;
  readonly nowMs: number;
  readonly source: DesignReviewSource;
}

/** Restart remounts the session, so every choice returns to its starting value. */
export function ThreadCardBrightnessReview(props: ReviewProps) {
  const [run, setRun] = useState(0);
  return (
    <ThreadCardBrightnessSession
      key={run}
      {...props}
      onRestart={() => {
        applyReviewTheme(props.initialTheme);
        setRun((value) => value + 1);
      }}
    />
  );
}

function ThreadCardBrightnessSession(props: ReviewProps & { readonly onRestart: () => void }) {
  const [boost, setBoost] = useState<number>(DEFAULT_BOOST);
  const [scenario, setScenario] = useState<ThreadCardScenario>("read");
  const [theme, setTheme] = useState<ReviewTheme>(props.initialTheme);
  const baselineScope = useRef<HTMLDivElement>(null);
  const candidateScope = useRef<HTMLDivElement>(null);
  const readouts = useRef<HTMLDivElement>(null);
  const sweepId = useId();
  const scenarioLabel = THREAD_CARD_SCENARIOS.find((option) => option.value === scenario)!.label;
  const themeLabel = REVIEW_THEME_OPTIONS.find((option) => option.value === theme)!.label;

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Measures the rendered product cards after each change and writes the ratios into their readout nodes.
  useLayoutEffect(() => {
    const container = readouts.current;
    const baselineTitle = titleIn(baselineScope);
    const scopeElement = baselineScope.current;
    if (container === null || baselineTitle === null || scopeElement === null) return;
    const write = (selector: string, text: string) => {
      const output = container.querySelector(selector);
      if (output) output.textContent = text;
    };
    write('[data-contrast="baseline"]', describeContrast(measureTextContrast(baselineTitle)));
    const candidateTitle = titleIn(candidateScope);
    write(
      '[data-contrast="candidate"]',
      describeContrast(candidateTitle === null ? null : measureTextContrast(candidateTitle)),
    );
    const sweep = measureTextContrastAcross(
      baselineTitle,
      SWEEP_VALUES,
      (value) => applyBoostScope(scopeElement, value),
      () => applyBoostScope(scopeElement, DEFAULT_BOOST),
    );
    for (const { value, ratio } of sweep) {
      write(`[data-contrast-sweep="${value}"]`, formatContrast(ratio));
    }
    container.dataset.measuredFor = `${scenario}:${theme}:${boost}`;
  }, [boost, scenario, theme]);

  const copyText = () => {
    const chosen = titleIn(candidateScope);
    const standard = titleIn(baselineScope);
    const chosenRatio = formatContrast(chosen === null ? null : measureTextContrast(chosen));
    const standardRatio = formatContrast(standard === null ? null : measureTextContrast(standard));
    return [
      "Thread title brightness review",
      boost === DEFAULT_BOOST
        ? `Choice: keep the current default (${DEFAULT_BOOST}%)`
        : `Choice: ${boost}% (current default ${DEFAULT_BOOST}%)`,
      `Setting: threadTitleBrightnessBoost = ${boost}`,
      `Checked on: ${scenarioLabel} card, ${themeLabel} theme`,
      `Title contrast: ${chosenRatio} at ${boost}%, ${standardRatio} at the ${DEFAULT_BOOST}% default`,
      `Source: ${formatDesignReviewSource(props.source)}`,
      `Components: ${COMPONENTS.join(", ")}`,
    ].join("\n");
  };

  return (
    <ReviewSurface
      title="Thread title brightness"
      summary={`The same synthetic Workstream card, drawn by the sidebar's own components. The left card stays at the current default (${DEFAULT_BOOST}%); only title brightness changes on the right. 0% keeps the original title colours.`}
      source={props.source}
      components={COMPONENTS}
    >
      <section aria-label="Scenario" className="flex flex-wrap gap-x-6 gap-y-3">
        <ReviewChoice
          label="Card state"
          options={THREAD_CARD_SCENARIOS}
          value={scenario}
          onChange={setScenario}
        />
        <ReviewChoice
          label="Theme"
          options={REVIEW_THEME_OPTIONS}
          value={theme}
          onChange={(next) => {
            applyReviewTheme(next);
            setTheme(next);
          }}
        />
      </section>

      <div ref={readouts} className="contents">
        <ReviewComparison label="Baseline and adjusted cards">
          <ReviewPane label="Current default" detail={`${DEFAULT_BOOST}%`}>
            <SidebarCard
              appearance={DEFAULT_SIDEBAR_APPEARANCE}
              scenario={scenario}
              pane="baseline"
              nowMs={props.nowMs}
              scopeRef={baselineScope}
            />
            <p className="text-xs text-muted-foreground">
              Title contrast <output data-contrast="baseline" className="tabular-nums" />
            </p>
          </ReviewPane>
          <ReviewPane label="Your adjustment" detail={`${boost}%`}>
            <SidebarCard
              appearance={withBoost(boost)}
              scenario={scenario}
              pane="candidate"
              nowMs={props.nowMs}
              scopeRef={candidateScope}
            />
            <p className="text-xs text-muted-foreground">
              Title contrast <output data-contrast="candidate" className="tabular-nums" />
            </p>
          </ReviewPane>
        </ReviewComparison>

        <section aria-label="Adjustment" className="rounded-lg border border-border/60 p-3">
          <ThreadTitleBrightnessSlider value={boost} onChange={setBoost} />
        </section>

        <section aria-labelledby={sweepId} className="flex flex-col gap-2">
          <h2 id={sweepId} className="text-xs font-medium text-muted-foreground">
            Title contrast at each value ({scenarioLabel} card, {themeLabel} theme)
          </h2>
          <ol className="grid grid-cols-4 gap-1 sm:grid-cols-6">
            {SWEEP_VALUES.map((value) => (
              <li
                key={value}
                aria-current={value === boost ? "true" : undefined}
                className={cn(
                  "flex flex-col rounded-md border border-border/60 px-2 py-1 text-xs tabular-nums",
                  value === boost && "border-primary bg-accent font-medium",
                )}
              >
                <span>
                  {value}%{value === DEFAULT_BOOST ? " default" : ""}
                </span>
                <output data-contrast-sweep={value} className="text-muted-foreground" />
              </li>
            ))}
          </ol>
        </section>
      </div>

      <ReviewActions
        resetLabel={`Reset to ${DEFAULT_BOOST}%`}
        onReset={() => setBoost(DEFAULT_BOOST)}
        onRestart={props.onRestart}
        copyKey={`${scenario}:${theme}:${boost}`}
        copyText={copyText}
      />
    </ReviewSurface>
  );
}
