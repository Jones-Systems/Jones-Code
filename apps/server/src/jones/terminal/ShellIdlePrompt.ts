import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

/** Markers are ephemeral, stripped before terminal output/history, and never replayed. */
export class ShellIdlePrompt {
  atPrompt = false;
  revision = 0;
  private pending = "";
  readonly ready: string;
  readonly busy: string;
  private readonly markers: RegExp;

  constructor(token: string) {
    this.ready = `\x1b]1337;JonesIdle=${token}:ready\x07`;
    this.busy = `\x1b]1337;JonesIdle=${token}:busy\x07`;
    const escaped = `\x1b]1337;JonesIdle=${token}:`.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    this.markers = new RegExp(`${escaped}(ready|busy)\x07`, "g");
  }

  input(): void {
    this.atPrompt = false;
    this.revision++;
  }

  output(chunk: string): string {
    let data = this.pending + chunk;
    this.pending = "";
    // Retain only a possible split marker suffix, bounded by the marker length.
    for (let length = Math.min(data.length, this.ready.length - 1); length > 0; length--) {
      const tail = data.slice(-length);
      if (
        (length < this.ready.length && this.ready.startsWith(tail)) ||
        (length < this.busy.length && this.busy.startsWith(tail))
      ) {
        this.pending = tail;
        data = data.slice(0, -length);
        break;
      }
    }
    // A single scan avoids quadratic work when a shell emits many prompt markers.
    return data.replace(this.markers, (_marker, state: string) => {
      this.atPrompt = state === "ready";
      this.revision++;
      return "";
    });
  }
}

/** Child-local startup hooks preserve the user's files; unsupported shells never expire. */
export const makeShellIdlePromptIntegration = Effect.fn("terminal.makeShellIdlePromptIntegration")(
  function* () {
    const crypto = yield* Crypto.Crypto;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "jones-shell-idle-" });
    // Zsh has no environment-only precmd/preexec hook. A private startup directory
    // sources the ordinary files in order, then restores ZDOTDIR before the prompt.
    const sourceOriginal = (file: string) => `
__jones_idle_dotdir=$HOME
if [[ $JONES_IDLE_DOTDIR_SET == 1 ]]; then
  __jones_idle_dotdir=$JONES_IDLE_DOTDIR
  ZDOTDIR="$JONES_IDLE_DOTDIR"
else
  unset ZDOTDIR
fi
if [[ -r "$__jones_idle_dotdir/${file}" ]]; then
  source "$__jones_idle_dotdir/${file}"
fi
# Re-enter our next startup hook only if the user's file kept its original
# directory and startup processing enabled. Its own source calls see the real path.
if [[ -o interactive && -o rcs ]]; then
  if [[ $JONES_IDLE_DOTDIR_SET == 1 && \${+ZDOTDIR} == 1 && $ZDOTDIR == $JONES_IDLE_DOTDIR ]] ||
     [[ $JONES_IDLE_DOTDIR_SET == 0 && \${+ZDOTDIR} == 0 ]]; then
    ZDOTDIR="$JONES_IDLE_WRAPPER"
  fi
fi
`;
    yield* fs.writeFileString(path.join(directory, ".zshenv"), sourceOriginal(".zshenv"));
    yield* fs.writeFileString(path.join(directory, ".zprofile"), sourceOriginal(".zprofile"));
    yield* fs.writeFileString(
      path.join(directory, ".zshrc"),
      sourceOriginal(".zshrc") +
        `
if [[ $ZDOTDIR == $JONES_IDLE_WRAPPER ]]; then
  if [[ $JONES_IDLE_DOTDIR_SET == 1 ]]; then
    ZDOTDIR="$JONES_IDLE_DOTDIR"
  else
    unset ZDOTDIR
  fi
fi
function __jones_idle_ready() {
  local __jones_idle_status=$?
  if (( \${preexec_functions[(Ie)__jones_idle_busy]} )); then
    builtin printf '%s' "$JONES_IDLE_READY"
  fi
  return $__jones_idle_status
}
function __jones_idle_busy() { builtin printf '%s' "$JONES_IDLE_BUSY"; }
precmd_functions+=(__jones_idle_ready)
preexec_functions+=(__jones_idle_busy)
`,
    );

    return {
      prepare: Effect.fn("terminal.prepareShellIdlePrompt")(function* (
        shellName: string | null,
        env: NodeJS.ProcessEnv,
      ) {
        if (shellName !== "bash" && shellName !== "zsh") return { env, tracker: undefined };
        const token = yield* crypto.randomUUIDv4();
        const tracker = new ShellIdlePrompt(token);
        if (shellName === "bash") {
          // PS0 is emitted immediately before execution, including shell builtins.
          // Bash <4.4 or startup files removing PS0 cannot provide proof: no expiry.
          const previous = env.PROMPT_COMMAND;
          const prompt =
            `${previous ? `${previous}\n` : ""}__jones_idle_status=$?; ` +
            `if (( BASH_VERSINFO[0] > 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] >= 4) )) ` +
            `&& [[ $PS0 == *'${tracker.busy}'* ]]; then builtin printf '%s' '${tracker.ready}'; fi; ` +
            `(exit "$__jones_idle_status")`;
          return {
            tracker,
            env: { ...env, PS0: `${env.PS0 ?? ""}${tracker.busy}`, PROMPT_COMMAND: prompt },
          };
        }
        return {
          tracker,
          env: {
            ...env,
            ZDOTDIR: directory,
            JONES_IDLE_WRAPPER: directory,
            JONES_IDLE_DOTDIR: env.ZDOTDIR ?? "",
            JONES_IDLE_DOTDIR_SET: env.ZDOTDIR === undefined ? "0" : "1",
            JONES_IDLE_READY: tracker.ready,
            JONES_IDLE_BUSY: tracker.busy,
          },
        };
      }),
    };
  },
);
