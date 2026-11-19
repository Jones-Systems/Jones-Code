import * as Schema from "effect/Schema";
import { useState, type CSSProperties } from "react";
import { SettingsSection } from "../../components/settings/settingsLayout";
import { Button } from "../../components/ui/button";
import { Input } from "../../components/ui/input";
import { useCopyToClipboard } from "../../hooks/useCopyToClipboard";
import {
  PersonalSidebarGroupHeader,
  WorkstreamRailToggle,
} from "../workstreams/PersonalSidebarGroupHeader";
import type { PersonalStatusFilter } from "../workstreams/personalStatusFilter";
import { sidebarAppearanceScope, type SidebarAppearance } from "./model";
import { SidebarAppearanceOverride } from "./SidebarAppearanceContext";
import {
  THREAD_BOARD_MEASUREMENTS,
  ThreadBoardColor,
  threadBoardAppearanceScope,
  type ThreadBoardAppearance,
  type ThreadBoardColorKey,
  type ThreadBoardMeasurement,
} from "./threadBoardAppearance";
import { useSidebarAppearanceStore } from "./useSidebarAppearanceStore";
import { useThreadBoardAppearanceControls } from "./useThreadBoardAppearance";
import "./sidebarAppearance.cards.css";
import "./sidebarAppearance.workstreams.css";

const measurementGroups = [
  {
    title: "Thread cards",
    fields: [
      ["cardScale", "Card size", "slider"],
      ["cardPadding", "Card padding"],
      ["cardGap", "Spacing between cards"],
      ["cardTitleSize", "Thread title size"],
      ["cardTitleWeight", "Thread title weight", "slider"],
      ["cardMetaSize", "Project and metadata text size"],
      ["cardBorderWidth", "Card border width"],
      ["cardBorderOpacity", "Card border opacity", "slider"],
    ],
  },
  {
    title: "Workstream sections",
    fields: [
      ["groupBorderWidth", "Section border width"],
      ["groupBorderOpacity", "Section border opacity", "slider"],
      ["tintOpacity", "Workstream tint opacity", "slider"],
      ["tintBrightness", "Workstream tint brightness", "slider"],
    ],
  },
  {
    title: "Workstream headers and filters",
    fields: [
      ["headerPadding", "Default vertical header padding"],
      ["headerPaddingTop", "Workstream top padding"],
      ["headerPaddingBottom", "Workstream bottom padding"],
      ["headerPaddingInline", "Horizontal header padding"],
      ["headerTitleSize", "Workstream title size"],
      ["headerTitleWeight", "Workstream title weight", "slider"],
      ["headerGap", "Header title and filter spacing"],
      ["countSize", "Failed and total count size"],
      ["countGap", "Failed and total count spacing"],
      ["countOffsetX", "Count offset left (−) / right (+)"],
      ["countOffsetY", "Count offset up (−) / down (+)"],
      ["filterSize", "Status filter text size"],
      ["filterGap", "Status filter spacing"],
      ["filterOffsetX", "Filter pill offset left (−) / right (+)"],
      ["filterOffsetY", "Filter pill offset up (−) / down (+)"],
    ],
  },
] as const satisfies ReadonlyArray<{
  title: string;
  fields: ReadonlyArray<readonly [ThreadBoardMeasurement, string, "slider"?]>;
}>;

function measurementDefault(
  key: ThreadBoardMeasurement,
  sidebar: SidebarAppearance,
  board: ThreadBoardAppearance,
) {
  if (key === "cardScale") return Math.round(sidebar.cardScale * 100);
  if (key === "cardPadding") return Math.round(8 * sidebar.cardScale);
  if (key === "cardTitleSize") return Math.round(14 * sidebar.cardScale);
  if (key === "cardMetaSize") return Math.round(12 * sidebar.cardScale);
  const legacyPadding = { tight: 3, default: 4, roomy: 6 }[sidebar.workstreamSpacing];
  if (key === "headerPadding") return legacyPadding;
  if (key === "headerPaddingTop" || key === "headerPaddingBottom") {
    return board.headerPadding ?? legacyPadding;
  }
  if (key === "headerTitleWeight") {
    return { regular: 400, medium: 500, default: 600 }[sidebar.workstreamTitleWeight];
  }
  return THREAD_BOARD_MEASUREMENTS[key].initial;
}

function MeasurementControl({
  measurement,
  label,
  slider,
  value,
  initial,
  onChange,
}: {
  measurement: ThreadBoardMeasurement;
  label: string;
  slider: boolean;
  value: number | undefined;
  initial: number;
  onChange: (value: number | undefined) => void;
}) {
  const { min, max, unit } = THREAD_BOARD_MEASUREMENTS[measurement];
  const shown = value ?? initial;
  const id = `thread-board-${measurement}`;
  return (
    <div className="min-w-0 space-y-1.5">
      <label htmlFor={id} className="block text-sm font-medium">
        {label}
      </label>
      <div className="flex min-w-0 items-center gap-2">
        {slider ? (
          <>
            <input
              id={id}
              type="range"
              className="min-w-0 flex-1 cursor-pointer accent-primary"
              value={shown}
              min={min}
              max={max}
              step={unit === "" ? 10 : 1}
              aria-valuetext={`${shown}${unit}`}
              onChange={(event) => onChange(event.currentTarget.valueAsNumber)}
            />
            <span aria-hidden="true" className="min-w-9 text-right text-xs tabular-nums">
              {shown}
              {unit}
            </span>
          </>
        ) : (
          <>
            <div className="w-20 shrink-0">
              <Input
                key={shown}
                id={id}
                type="number"
                size="sm"
                min={min}
                max={max}
                step={1}
                defaultValue={shown}
                onKeyDown={(event) => {
                  if (event.key === "Enter") event.currentTarget.blur();
                  if (event.key === "Escape") {
                    event.currentTarget.value = String(shown);
                    event.currentTarget.blur();
                  }
                }}
                onBlur={(event) => {
                  const input = event.currentTarget;
                  if (input.validity.valid && Number.isInteger(input.valueAsNumber)) {
                    if (input.valueAsNumber !== shown) onChange(input.valueAsNumber);
                  } else {
                    input.value = String(shown);
                  }
                }}
              />
            </div>
            <span className="text-xs text-muted-foreground">{unit}</span>
          </>
        )}
        <div className="w-12 shrink-0">
          {value !== undefined ? (
            <Button
              size="xs"
              variant="ghost"
              aria-label={`Reset ${label.toLowerCase()}`}
              onClick={() => onChange(undefined)}
            >
              Reset
            </Button>
          ) : null}
        </div>
      </div>
    </div>
  );
}

const isColor = Schema.is(ThreadBoardColor);

function ColorControl({
  field,
  title,
  value,
  onChange,
}: {
  field: ThreadBoardColorKey;
  title: string;
  value: string | undefined;
  onChange: (value: string | undefined) => void;
}) {
  const id = `thread-board-${field}`;
  return (
    <div className="min-w-0 space-y-1.5">
      <label htmlFor={id} className="block text-sm font-medium">
        {title}
      </label>
      <div className="flex flex-wrap items-center gap-2">
        <input
          type="color"
          aria-label={`Pick ${title.toLowerCase()}`}
          className="h-8 w-10 cursor-pointer rounded border border-border bg-transparent"
          value={value ?? "#283244"}
          onChange={(event) => onChange(event.currentTarget.value)}
        />
        <div className="w-26">
          <Input
            key={value ?? "theme"}
            id={id}
            size="sm"
            placeholder={field === "unassignedBackground" ? "Board color" : "Theme"}
            defaultValue={value ?? ""}
            spellCheck={false}
            onKeyDown={(event) => {
              if (event.key === "Enter") event.currentTarget.blur();
            }}
            onBlur={(event) => {
              const input = event.currentTarget;
              const text = input.value.trim();
              if (text === "") {
                if (value !== undefined) onChange(undefined);
              } else if (isColor(text)) {
                onChange(text.toLowerCase());
              } else {
                input.value = value ?? "";
              }
            }}
          />
        </div>
        {value !== undefined ? (
          <Button
            size="xs"
            variant="ghost"
            aria-label={`Reset ${title.toLowerCase()}`}
            onClick={() => onChange(undefined)}
          >
            Reset
          </Button>
        ) : null}
      </div>
    </div>
  );
}

function ThreadBoardPreview({
  board,
  sidebar,
}: {
  board: ThreadBoardAppearance;
  sidebar: SidebarAppearance;
}) {
  const [collapsed, setCollapsed] = useState(false);
  const [filter, setFilter] = useState<PersonalStatusFilter | null>(null);
  const showStatus = (status: "done" | "failed") =>
    !collapsed &&
    (filter === null ||
      filter === "all" ||
      filter === status ||
      (filter.startsWith("hide-") && filter !== `hide-${status}`));
  const sidebarScope = sidebarAppearanceScope(sidebar);
  const boardScope = threadBoardAppearanceScope(board);
  const card = (title: string, metadata: string) => (
    <div data-jones-sidebar-card>
      <div data-jones-sidebar-card-surface className="rounded-lg">
        <div
          data-jones-sidebar-card-content
          className="flex h-[4.875rem] flex-col justify-center gap-1 px-2.5 py-1.5"
        >
          <span data-jones-sidebar-card-topline className="truncate text-xs text-secondary-label">
            {metadata}
          </span>
          <span
            data-jones-thread-title="receded"
            data-jones-thread-title-branch="card"
            className="truncate text-sm text-foreground/75"
          >
            {title}
          </span>
        </div>
      </div>
    </div>
  );
  return (
    <SidebarAppearanceOverride value={sidebar}>
      <div
        {...sidebarScope.attributes}
        {...boardScope.attributes}
        style={
          {
            "--jones-board-background": "var(--sidebar)",
            ...sidebarScope.variables,
            ...boardScope.variables,
          } as CSSProperties
        }
        data-jones-thread-board
        className="max-w-80 space-y-2 rounded-lg bg-sidebar p-2 text-sidebar-foreground"
      >
        <section
          data-jones-board-section="workstream"
          className="relative rounded-lg"
          style={
            {
              "--jones-board-section-accent": "#38bda9",
              "--jones-board-section-rail": "#38bda9",
            } as CSSProperties
          }
        >
          <PersonalSidebarGroupHeader
            name="CI & Test Efficiency"
            collapsed={collapsed}
            onToggle={() => setCollapsed((value) => !value)}
            summary={{ total: 2, running: 0, attention: 0, failed: 1, done: 1 }}
            statusFilter={filter}
            onStatusFilterChange={setFilter}
          />
          {board.clickRailToCollapse ? (
            <WorkstreamRailToggle
              name="CI & Test Efficiency"
              collapsed={collapsed}
              onToggle={() => setCollapsed((value) => !value)}
            />
          ) : null}
          {showStatus("done") ? card("Assess CI performance", "Jones Code · Done") : null}
          {showStatus("failed") ? card("Run build verification", "Jones Code · Failed") : null}
        </section>
        <section data-jones-board-section="unassigned" className="rounded-lg">
          <div
            data-jones-board-unassigned-header
            className="flex items-center justify-between px-2 py-1 text-sm"
          >
            <span>Unassigned</span>
            <span data-jones-board-count>1 thread</span>
          </div>
          {card("Follow up on build results", "Jones Code · Laptop")}
        </section>
      </div>
    </SidebarAppearanceOverride>
  );
}

export function ThreadBoardAppearanceSettings() {
  const { appearance, set, reset, saveFailed } = useThreadBoardAppearanceControls();
  const { stored: sidebar } = useSidebarAppearanceStore();
  const [message, setMessage] = useState("");
  const { copyToClipboard, isCopied } = useCopyToClipboard({
    target: "thread board appearance settings",
    onCopy: () => setMessage("Appearance settings copied."),
    onError: () => setMessage("Couldn’t copy appearance settings."),
  });

  return (
    <SettingsSection id="thread-board-appearance" title="Thread board" variant="plain">
      <div className="space-y-5 rounded-xl border border-border bg-card p-4">
        <div className="space-y-2">
          <p className="text-sm text-muted-foreground">
            Changes apply immediately and are saved in this browser profile. Right-click a
            Workstream and choose Change color to set its individual color.
          </p>
          <div className="flex flex-wrap gap-2">
            <Button
              size="sm"
              variant="outline"
              onClick={() =>
                copyToClipboard(
                  JSON.stringify({ version: 1, board: appearance, sidebar }, null, 2),
                  undefined,
                )
              }
            >
              {isCopied ? "Copied" : "Copy appearance settings"}
            </Button>
            <Button
              size="sm"
              variant="ghost"
              onClick={() => {
                reset();
                setMessage("");
              }}
              disabled={Object.keys(appearance).length === 0}
            >
              Reset board settings
            </Button>
          </div>
          <p role="status" className="text-xs text-muted-foreground">
            {saveFailed ? "Couldn’t save appearance settings." : message}
          </p>
        </div>
        <div className="grid items-start gap-5 xl:grid-cols-[minmax(0,1fr)_minmax(0,18rem)]">
          <aside
            className="space-y-1.5 xl:sticky xl:top-4 xl:col-start-2 xl:row-start-1"
            aria-label="Thread board preview"
            role="group"
          >
            <p className="text-xs font-medium text-muted-foreground">Preview</p>
            <ThreadBoardPreview board={appearance} sidebar={sidebar} />
          </aside>
          <div className="min-w-0 space-y-5 xl:col-start-1 xl:row-start-1">
            {measurementGroups.map(({ title, fields }) => (
              <fieldset key={title} className="min-w-0 border-t border-border pb-1">
                <legend className="pr-2 text-sm font-semibold">{title}</legend>
                <div className="grid gap-x-6 gap-y-4 pt-3 sm:grid-cols-2">
                  {fields.map(([measurement, label, control]) => (
                    <MeasurementControl
                      key={measurement}
                      measurement={measurement}
                      label={label}
                      slider={control === "slider"}
                      value={appearance[measurement]}
                      initial={measurementDefault(measurement, sidebar, appearance)}
                      onChange={(value) => set(measurement, value)}
                    />
                  ))}
                  {title === "Thread cards" ? (
                    <ColorControl
                      field="cardBorderColor"
                      title="Card border color"
                      value={appearance.cardBorderColor}
                      onChange={(value) => set("cardBorderColor", value)}
                    />
                  ) : null}
                  {title === "Workstream sections" ? (
                    <>
                      <div className="space-y-1.5">
                        <p className="text-sm font-medium">Workstream color bar</p>
                        <Button
                          size="sm"
                          variant="outline"
                          aria-pressed={appearance.clickRailToCollapse === true}
                          onClick={() =>
                            set(
                              "clickRailToCollapse",
                              appearance.clickRailToCollapse ? undefined : true,
                            )
                          }
                        >
                          Click color bar to collapse or expand
                        </Button>
                      </div>
                      <ColorControl
                        field="background"
                        title="Board background"
                        value={appearance.background}
                        onChange={(value) => set("background", value)}
                      />
                      <ColorControl
                        field="unassignedBackground"
                        title="Unassigned background"
                        value={appearance.unassignedBackground}
                        onChange={(value) => set("unassignedBackground", value)}
                      />
                    </>
                  ) : null}
                  {title === "Workstream headers and filters" ? (
                    <div className="space-y-1.5">
                      <p className="text-sm font-medium">Filter spacing mode</p>
                      <Button
                        size="sm"
                        variant="outline"
                        aria-describedby="thread-board-filter-spacing-help"
                        aria-pressed={appearance.filterSpacing === "fixed"}
                        onClick={() =>
                          set(
                            "filterSpacing",
                            appearance.filterSpacing === "fixed" ? undefined : "fixed",
                          )
                        }
                      >
                        Keep filter spacing fixed
                      </Button>
                      <p
                        id="thread-board-filter-spacing-help"
                        className="text-xs text-muted-foreground"
                      >
                        Adaptive spacing can tighten to make room for long Workstream names.
                      </p>
                    </div>
                  ) : null}
                </div>
              </fieldset>
            ))}
          </div>
        </div>
      </div>
    </SettingsSection>
  );
}
