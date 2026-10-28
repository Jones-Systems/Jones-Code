import type { LucideProps } from "lucide-react";
import type { EnvironmentMachineKind } from "@t3tools/contracts";
import { EnvironmentMachineIcon } from "../../components/EnvironmentMachineIcon";
import { resolveMachineIdentity } from "./model";

export function MachineIcon({
  label,
  kind,
  ...props
}: LucideProps & {
  label?: string | null | undefined;
  kind?: EnvironmentMachineKind | null | undefined;
}) {
  const machine = resolveMachineIdentity(label, kind);
  const emoji = { vps: "🧠", test: "🤖", laptop: "🏡", mini: "🍎", unknown: null }[
    machine.identity
  ];
  return emoji ? (
    <span
      aria-hidden="true"
      className="inline-flex size-4 shrink-0 items-center justify-center text-base leading-none"
    >
      {emoji}
    </span>
  ) : (
    <EnvironmentMachineIcon kind={machine.kind} {...props} />
  );
}
