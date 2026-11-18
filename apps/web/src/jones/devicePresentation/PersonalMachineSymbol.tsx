import { BrainIcon, HouseIcon, BotIcon, type LucideProps } from "lucide-react";
import { useId, type FunctionComponent } from "react";
import type { EnvironmentMachineKind } from "@t3tools/contracts";
import { resolveMachineIdentity } from "@t3tools/contracts/jones/devicePresentation";
import { useSidebarAppearancePresentation } from "../sidebarAppearance/SidebarAppearanceContext";
import type { SidebarAppearance } from "../sidebarAppearance/model";

const HOUSE_BODY =
  "M3 10a2 2 0 0 1 .709-1.528l7-6a2 2 0 0 1 2.582 0l7 6A2 2 0 0 1 21 10v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z";
const BRAIN_HALVES = [
  {
    side: "left",
    body: "M10.2 3.6C8.7 2.7 6.3 3 5.5 5.1 3.6 5.4 2.4 7.4 3.1 9.4 1.9 10.9 2 13.6 3.5 14.8 3.3 17.3 5.2 19.4 7.5 19 8.4 20.5 9.6 20.9 10.2 20.4Z",
    grooves: ["M10.2 12C8.4 12 7 10.9 6.6 9.3", "M3.5 14.8C5.1 14.6 6.5 15.4 7.1 16.9"],
  },
  {
    side: "right",
    body: "M13.8 3.6C15.3 2.7 17.7 3 18.5 5.1 20.4 5.4 21.6 7.4 20.9 9.4 22.1 10.9 22 13.6 20.5 14.8 20.7 17.3 18.8 19.4 16.5 19 15.6 20.5 14.4 20.9 13.8 20.4Z",
    grooves: ["M13.8 12C15.6 12 17 10.9 17.4 9.3", "M20.5 14.8C18.9 14.6 17.5 15.4 16.9 16.9"],
  },
] as const;

function SymbolSvg({ size = 24, color = "currentColor", ...props }: LucideProps) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width={size}
      height={size}
      viewBox="0 0 24 24"
      color={color}
      fill="none"
      stroke={color}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      {...props}
    />
  );
}

function FilledHouseSymbol(props: LucideProps) {
  const maskId = `${useId()}-house-door`;
  return (
    <SymbolSvg {...props}>
      <defs>
        <mask id={maskId} maskUnits="userSpaceOnUse" x={-2} y={-2} width={28} height={28}>
          {/* oxlint-disable-next-line shadcn/no-raw-colors -- Luminance-mask coverage must be white, independent of the visual theme. */}
          <rect x={-2} y={-2} width={28} height={28} fill="white" stroke="none" />
          {/* oxlint-disable-next-line shadcn/no-raw-colors -- Black removes the door from the luminance mask; it is not painted ink. */}
          <path d="M10 23v-9.5a.5.5 0 0 1 .5-.5h3a.5.5 0 0 1 .5.5V23z" fill="black" stroke="none" />
        </mask>
      </defs>
      <path
        d={HOUSE_BODY}
        fill="currentColor"
        stroke="currentColor"
        strokeWidth={2}
        mask={`url(#${maskId})`}
      />
    </SymbolSvg>
  );
}

function BrainSymbol({
  variant,
  ...props
}: LucideProps & { variant: Exclude<SidebarAppearance["vpsSymbol"], "outline"> }) {
  const id = useId();
  return (
    <SymbolSvg {...props}>
      <defs>
        {BRAIN_HALVES.map(({ side, grooves }) => (
          <mask
            key={side}
            id={`${id}-${side}`}
            maskUnits="userSpaceOnUse"
            x={-2}
            y={-2}
            width={28}
            height={28}
          >
            {/* oxlint-disable-next-line shadcn/no-raw-colors -- Luminance-mask coverage must be white, independent of the visual theme. */}
            <rect x={-2} y={-2} width={28} height={28} fill="white" stroke="none" />
            {grooves.map((d) => (
              <path
                key={d}
                d={d}
                fill="none"
                stroke="var(--color-black, #000)"
                strokeWidth={1.7}
                strokeLinecap="round"
              />
            ))}
          </mask>
        ))}
      </defs>
      {BRAIN_HALVES.map(({ side, body }) => {
        const solid = variant === "filled" || variant === side;
        return (
          <path
            key={side}
            d={body}
            fill={solid ? "currentColor" : "none"}
            stroke="currentColor"
            strokeWidth={1.75}
            mask={solid ? `url(#${id}-${side})` : undefined}
          />
        );
      })}
    </SymbolSvg>
  );
}

function AppleSymbol(props: LucideProps) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="currentColor"
      aria-hidden="true"
      {...props}
    >
      <path d="M15.8 2c.2 1.8-1.2 3.8-3.1 4 .1-1.8 1.3-3.6 3.1-4ZM19.7 16.7c-.6 1.4-1 2.1-1.8 3.2-.9 1.2-2.1 2.7-3.6 2-1.4-.6-2.6-.6-4.1 0-1.5.6-2.6-.7-3.5-1.9C4.2 16.4 3 11.1 5.8 8.4c1.5-1.4 3.5-1.8 5.4-1.1 1 .4 1.5.4 2.6 0 1.7-.6 3.9-.3 5.2 1.1-3.4 1.9-2.8 6.4.7 8.3Z" />
    </svg>
  );
}

export function PersonalMachineSymbol({
  label,
  kind,
  fallback: Fallback,
  ...props
}: LucideProps & {
  label?: string | null | undefined;
  kind: EnvironmentMachineKind;
  fallback: FunctionComponent<LucideProps>;
}) {
  const appearance = useSidebarAppearancePresentation();
  const identity = resolveMachineIdentity(label, kind).identity;
  if (identity === "laptop" && label != null && appearance?.laptopSymbol === "filled") {
    return <FilledHouseSymbol {...props} />;
  }
  if (identity === "vps" && appearance && appearance.vpsSymbol !== "outline") {
    return <BrainSymbol variant={appearance.vpsSymbol} {...props} />;
  }
  const Icon = {
    vps: BrainIcon,
    test: BotIcon,
    laptop: HouseIcon,
    mini: AppleSymbol,
    unknown: Fallback,
  }[identity];
  return <Icon {...props} />;
}
