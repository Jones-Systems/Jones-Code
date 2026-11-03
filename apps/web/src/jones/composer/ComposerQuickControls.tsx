import { useLayoutEffect, useState } from "react";
import { ZapIcon } from "lucide-react";
import { getProviderOptionCurrentValue } from "@t3tools/shared/model";
import { ComposerControl } from "../../components/chat/ComposerControl";
import {
  useTraitsSelection,
  type TraitsMenuContentProps,
  type TraitsPersistence,
} from "../../components/chat/TraitsPicker";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { observeResize } from "~/lib/observeResize";
import { cn } from "~/lib/utils";

const QUICK_EFFORTS = [
  { value: "medium", label: "M", name: "Medium" },
  { value: "high", label: "H", name: "High" },
  { value: "xhigh", label: "E", name: "Extra high" },
] as const;

export function ComposerQuickControls(
  props: TraitsMenuContentProps & TraitsPersistence & { hidden?: boolean },
) {
  const traits = useTraitsSelection(props);
  const descriptor = traits.primarySelectDescriptor;
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

  // oxlint-disable-next-line t3code/react-effect-budget -- Own the measured shortcut fit and font/resize subscriptions; no fixed composer breakpoint.
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
      const right =
        host.getBoundingClientRect().left +
        host.clientWidth -
        (Number.parseFloat(hostStyle.paddingInlineEnd) || 0);
      const fastButton = root.querySelector<HTMLElement>("[data-composer-quick-fast]");
      const start = fastButton
        ? fastButton.getBoundingClientRect().right + 8
        : root.getBoundingClientRect().left;
      // Retain the entire group until only 4px remains, with 1px promotion slack.
      setShowEfforts(
        (visible) => start + effort.getBoundingClientRect().width + (visible ? 4 : 5) <= right,
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
  const fastOn = fast
    ? fast.currentValue === true
    : getProviderOptionCurrentValue(tier) === "priority";
  const selected = traits.ultrathinkPromptControlled
    ? "ultrathink"
    : getProviderOptionCurrentValue(
        descriptor,
        traits.modelSelection,
        traits.reportedModelSelection,
      );
  const visible = showEfforts && !props.hidden;
  return (
    <div className="relative flex shrink-0 items-center gap-2" data-composer-quick-controls>
      {fast || tier ? (
        <Tooltip>
          <TooltipTrigger
            render={
              <ComposerControl
                data-composer-quick-fast
                aria-label="Fast mode"
                aria-pressed={fastOn}
                disabled={props.hidden}
                className="w-6 px-0 aria-pressed:bg-transparent aria-pressed:hover:bg-accent"
                onClick={() => {
                  if (props.hidden) return;
                  if (fast) traits.updateOption({ id: fast.id, value: !fastOn });
                  else if (tier) traits.handleSelectChange(tier, fastOn ? "default" : "priority");
                }}
              />
            }
          >
            <ZapIcon className={cn("size-4", fastOn && "fill-current")} />
          </TooltipTrigger>
          <TooltipPopup>Fast mode: {fastOn ? "on" : "off"}</TooltipPopup>
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
          {efforts.map(({ value, label, name }) => {
            const disabled = !descriptor || traits.isSelectChangeDisabled(descriptor, value);
            const description = traits.ultrathinkInBodyText
              ? 'Remove "ultrathink" from the prompt text to change reasoning.'
              : `${name} reasoning`;
            return (
              <Tooltip key={value}>
                <TooltipTrigger
                  render={
                    <ComposerControl
                      aria-label={`${name} reasoning`}
                      aria-pressed={selected === value}
                      disabled={disabled || !visible}
                      className="w-6 px-0 font-normal text-foreground/55 aria-pressed:bg-black/15 aria-pressed:text-foreground/90 aria-pressed:hover:bg-black/20"
                      onClick={() => {
                        if (visible && !disabled && descriptor)
                          traits.handleSelectChange(descriptor, value);
                      }}
                    />
                  }
                >
                  {label}
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
