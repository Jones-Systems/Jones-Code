import { isReasoningSelect } from "@t3tools/client-runtime/model-traits";
import { useLayoutEffect, useState } from "react";
import { ZapIcon } from "lucide-react";
import { getProviderOptionCurrentValue } from "@t3tools/shared/model";
import { UltrafastIcon } from "../../components/Icons";
import { ComposerControl } from "../../components/chat/ComposerControl";
import {
  useTraitsSelection,
  type TraitsMenuContentProps,
  type TraitsPersistence,
} from "../../components/chat/TraitsPicker";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { observeResize } from "~/lib/observeResize";
import { cn } from "~/lib/utils";
import { DOUBLE_CLICK_WINDOW_MS, useTimedClick } from "../useTimedClick";

const QUICK_EFFORTS = [
  {
    value: "medium",
    label: "M",
    name: "Medium",
    alternate: { value: "low", label: "L", name: "Low" },
  },
  { value: "high", label: "H", name: "High", alternate: null },
  {
    value: "xhigh",
    label: "E",
    name: "Extra high",
    alternate: { value: "max", label: "Max", name: "Max" },
  },
] as const;

export function ComposerQuickControls(
  props: TraitsMenuContentProps & TraitsPersistence & { hidden?: boolean },
) {
  const traits = useTraitsSelection(props);
  const descriptor = traits.selectDescriptors.find(isReasoningSelect) ?? null;
  const fast = traits.fastModeDescriptor;
  const tier = traits.selectDescriptors.find(
    (option) =>
      option.id === "serviceTier" &&
      option.options.some(({ id }) => id === "priority") &&
      option.options.some(({ id }) => id === "default"),
  );
  const efforts = QUICK_EFFORTS.filter(({ value }) =>
    descriptor?.options.some((option) => option.id === value),
  );
  const [effortElement, setEffortElement] = useState<HTMLDivElement | null>(null);
  const [showEfforts, setShowEfforts] = useState(false);
  const timedClick = useTimedClick(
    JSON.stringify([
      props.threadRef?.environmentId,
      props.threadRef?.threadId,
      "draftId" in props ? props.draftId : undefined,
      props.instanceId,
      props.provider,
      props.model,
      descriptor?.id,
    ]),
  );

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Own the measured shortcut fit and font/resize subscriptions; no fixed composer breakpoint.
  useLayoutEffect(() => {
    const effort = effortElement;
    const root = effort?.parentElement;
    const controls = root?.closest<HTMLElement>(
      "[data-chat-composer-controls], [data-chat-composer-resting-controls]",
    );
    if (!root || !effort || !controls) return;
    const host = controls.hasAttribute("data-chat-composer-resting-controls")
      ? controls.parentElement
      : controls;
    if (!host) return;
    const measure = () => {
      const hostStyle = getComputedStyle(host);
      const available =
        host.clientWidth -
        (Number.parseFloat(hostStyle.paddingInlineStart) || 0) -
        (Number.parseFloat(hostStyle.paddingInlineEnd) || 0);
      const siblings = [...controls.children].filter(
        (element): element is HTMLElement =>
          element instanceof HTMLElement &&
          element !== root &&
          getComputedStyle(element).position !== "absolute",
      );
      const occupied = siblings.reduce((width, element) => {
        const label = element.querySelector<HTMLElement>("[data-chat-provider-model-picker-label]");
        const maxLabel = label ? Number.parseFloat(getComputedStyle(label).maxWidth) : Number.NaN;
        const naturalLabel = label
          ? Math.min(label.scrollWidth, Number.isFinite(maxLabel) ? maxLabel : Infinity)
          : 0;
        const style = getComputedStyle(element);
        return (
          width +
          element.getBoundingClientRect().width +
          Math.max(0, naturalLabel - (label?.clientWidth ?? 0)) +
          (Number.parseFloat(style.marginInlineStart) || 0) +
          (Number.parseFloat(style.marginInlineEnd) || 0)
        );
      }, 0);
      const fastButton = root.querySelector<HTMLElement>("[data-composer-quick-fast]");
      const quickWidth =
        effort.getBoundingClientRect().width +
        (fastButton ? fastButton.getBoundingClientRect().width + 8 : 0);
      const gap = Number.parseFloat(getComputedStyle(controls).columnGap) || 0;
      // Reserve the following picker and mode controls before promoting the entire effort group.
      setShowEfforts(
        (visible) => occupied + siblings.length * gap + quickWidth + (visible ? 4 : 5) <= available,
      );
    };
    measure();
    const stop = observeResize(
      [host, controls, root, effort, ...controls.children].filter(
        (element): element is HTMLElement => element instanceof HTMLElement,
      ),
      measure,
    );
    document.fonts.addEventListener("loadingdone", measure);
    return () => {
      stop();
      document.fonts.removeEventListener("loadingdone", measure);
    };
  }, [effortElement]);

  if (traits.modelIsUnavailable || (!fast && !tier && efforts.length === 0)) return null;
  const selectedTier = getProviderOptionCurrentValue(tier);
  const ultrafast = fast ? null : tier?.options.find(({ id }) => id === "ultrafast");
  const ultrafastOn = !!ultrafast && selectedTier === ultrafast.id;
  const fastOn = fast ? fast.currentValue === true : selectedTier === "priority" || ultrafastOn;
  const selected = traits.ultrathinkPromptControlled
    ? "ultrathink"
    : getProviderOptionCurrentValue(descriptor);
  const visible = showEfforts && !props.hidden;
  return (
    <div className="relative flex shrink-0 items-center gap-2" data-composer-quick-controls>
      {fast || tier ? (
        <Tooltip>
          <TooltipTrigger
            render={
              <ComposerControl
                data-composer-quick-fast
                aria-label={ultrafastOn ? "Ultrafast mode" : "Fast mode"}
                aria-pressed={fastOn}
                disabled={props.hidden}
                className="w-6 px-0 aria-pressed:bg-transparent aria-pressed:hover:bg-accent"
                onClick={(event) => {
                  if (props.hidden) return;
                  const toggleFast = () => {
                    if (fast) traits.changeBoolean(fast.id, !fastOn);
                    else if (tier) traits.changeSelect(tier, fastOn ? "default" : "priority");
                  };
                  timedClick(
                    event,
                    toggleFast,
                    tier && ultrafast ? () => traits.changeSelect(tier, ultrafast.id) : undefined,
                  );
                }}
              />
            }
          >
            {ultrafastOn ? (
              <UltrafastIcon aria-hidden="true" className="size-4" />
            ) : (
              <ZapIcon aria-hidden="true" className={cn("size-4", fastOn && "fill-current")} />
            )}
          </TooltipTrigger>
          <TooltipPopup>
            {ultrafastOn ? "Ultrafast mode: on" : `Fast mode: ${fastOn ? "on" : "off"}`}
            {ultrafast ? `. Double-click within ${DOUBLE_CLICK_WINDOW_MS} ms for Ultrafast.` : null}
          </TooltipPopup>
        </Tooltip>
      ) : null}
      {efforts.length > 0 ? (
        <div
          ref={setEffortElement}
          role="group"
          aria-label="Quick reasoning effort"
          data-composer-quick-effort
          aria-hidden={!visible || undefined}
          inert={!visible || undefined}
          className={cn(
            "flex w-max shrink-0 items-center gap-2",
            !visible && "invisible absolute left-0 pointer-events-none",
          )}
        >
          {efforts.map(({ value, label, name, alternate }) => {
            const supportedAlternate =
              alternate && descriptor?.options.some(({ id }) => id === alternate.value)
                ? alternate
                : null;
            const activeAlternate =
              supportedAlternate?.value === selected ? supportedAlternate : null;
            const disabled =
              !descriptor ||
              (traits.ultrathinkInBodyText && descriptor.id === traits.primarySelectDescriptor?.id);
            const description = disabled
              ? 'Remove "ultrathink" from the prompt text to change reasoning.'
              : supportedAlternate
                ? `Click for ${name} reasoning. Double-click within ${DOUBLE_CLICK_WINDOW_MS} ms for ${supportedAlternate.name}.`
                : `${name} reasoning`;
            return (
              <Tooltip key={value}>
                <TooltipTrigger
                  render={
                    <ComposerControl
                      aria-label={`${activeAlternate?.name ?? name} reasoning`}
                      aria-pressed={selected === value || !!activeAlternate}
                      disabled={disabled || !visible}
                      className="w-6 px-0 font-normal text-foreground/55 aria-pressed:bg-black/15 aria-pressed:text-foreground/90 aria-pressed:hover:bg-black/20"
                      onClick={(event) => {
                        if (!visible || disabled) return;
                        timedClick(
                          event,
                          () => traits.changeSelect(descriptor, value),
                          supportedAlternate
                            ? () => traits.changeSelect(descriptor, supportedAlternate.value)
                            : undefined,
                        );
                      }}
                    />
                  }
                >
                  {activeAlternate ? (
                    <span className="text-xs">{activeAlternate.label}</span>
                  ) : (
                    label
                  )}
                </TooltipTrigger>
                <TooltipPopup>{description}</TooltipPopup>
              </Tooltip>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}
