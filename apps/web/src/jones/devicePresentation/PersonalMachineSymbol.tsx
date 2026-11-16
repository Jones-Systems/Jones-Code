import { BrainIcon, HouseIcon, BotIcon, type LucideProps } from "lucide-react";
import type { FunctionComponent } from "react";
import type { EnvironmentMachineKind } from "@t3tools/contracts";
import { resolveMachineIdentity } from "@t3tools/contracts/jones/devicePresentation";

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
  const identity = resolveMachineIdentity(label, kind).identity;
  const Icon = {
    vps: BrainIcon,
    test: BotIcon,
    laptop: HouseIcon,
    mini: AppleSymbol,
    unknown: Fallback,
  }[identity];
  return <Icon {...props} />;
}
