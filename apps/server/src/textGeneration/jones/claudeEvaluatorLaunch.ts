import { parseCliArgs, tokenizeCliArgs } from "@t3tools/shared/cliArgs";

type Arity = "boolean" | "required" | "optional" | "variadic";

// These task/UX options are replaced by the evaluator's own tool and prompt policy.
// Account, environment and unrecognized configuration options must fail closed.
const strippedFlags: Readonly<Record<string, Arity>> = {
  chrome: "boolean",
  "no-chrome": "boolean",
  "mcp-config": "variadic",
  "plugin-dir": "variadic",
  agents: "required",
  "add-dir": "variadic",
  tools: "variadic",
  allowedTools: "variadic",
  "allowed-tools": "variadic",
  disallowedTools: "variadic",
  "disallowed-tools": "variadic",
  "permission-mode": "required",
  "permission-prompt-tool": "required",
  "dangerously-skip-permissions": "boolean",
  "allow-dangerously-skip-permissions": "boolean",
  "disable-slash-commands": "boolean",
  "strict-mcp-config": "boolean",
  "system-prompt": "required",
  "system-prompt-file": "required",
  "append-system-prompt": "required",
  "append-system-prompt-file": "required",
  print: "optional",
  debug: "optional",
  "debug-file": "required",
  verbose: "boolean",
  "output-format": "required",
  "input-format": "required",
  "json-schema": "required",
  "include-partial-messages": "boolean",
  "replay-user-messages": "boolean",
  "thinking-display": "required",
  model: "required",
  "fallback-model": "required",
};

type ClaudeEvaluatorLaunch =
  | { readonly qualified: true; readonly args: ReadonlyArray<string> }
  | { readonly qualified: false; readonly detail: string };

function hasUnclosedQuote(input: string): boolean {
  let quote: "'" | '"' | undefined;
  for (let index = 0; index < input.length; index++) {
    const char = input[index];
    if (char === "\\") {
      const next = input[index + 1] ?? "";
      if (
        (quote === '"' && ['"', "\\", "$", "`"].includes(next)) ||
        (quote === undefined && /[\s'"]/.test(next))
      ) {
        index++;
      }
    } else if (char === quote) {
      quote = undefined;
    } else if (quote === undefined && (char === "'" || char === '"')) {
      quote = char;
    }
  }
  return quote !== undefined;
}

export function classifyClaudeEvaluatorLaunchArgs(launchArgs: string): ClaudeEvaluatorLaunch {
  if (hasUnclosedQuote(launchArgs)) {
    return {
      qualified: false,
      detail: "Claude remote request evaluation cannot use malformed launch arguments.",
    };
  }
  const tokens = tokenizeCliArgs(launchArgs);
  const args: Array<string> = [];
  let settingSourcesSeen = false;
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index]!;
    const normalized =
      token === "-p"
        ? "--print"
        : token === "-d"
          ? "--debug"
          : token === "-v"
            ? "--verbose"
            : token;
    const equalIndex = normalized.indexOf("=");
    const flag = equalIndex < 0 ? normalized : normalized.slice(0, equalIndex);
    const name = flag.slice(2);
    const unsupported = (): ClaudeEvaluatorLaunch => ({
      qualified: false,
      detail: /^--[a-zA-Z][a-zA-Z0-9-]{0,63}$/.test(flag)
        ? `Claude remote request evaluation does not support the configured ${flag} launch flag.`
        : "Claude remote request evaluation does not support positional or malformed launch arguments.",
    });
    if (!flag.startsWith("--")) return unsupported();
    const arity =
      name === "setting-sources"
        ? "required"
        : Object.hasOwn(strippedFlags, name)
          ? strippedFlags[name]
          : undefined;
    if (arity === undefined) return unsupported();
    const values: Array<string> = equalIndex < 0 ? [] : [normalized.slice(equalIndex + 1)];
    if (arity === "boolean") {
      if (values.length > 0) return unsupported();
    } else {
      while (
        index + 1 < tokens.length &&
        !tokens[index + 1]!.startsWith("-") &&
        (arity === "variadic" || values.length === 0)
      ) {
        values.push(tokens[++index]!);
      }
      if ((arity === "required" || arity === "variadic") && values.length === 0) {
        return unsupported();
      }
    }
    const parsed = parseCliArgs([flag, ...values], {
      booleanFlags: arity === "boolean" ? [name] : [],
    });
    if (name === "setting-sources") {
      const value = parsed.flags[name];
      if (
        settingSourcesSeen ||
        typeof value !== "string" ||
        (value !== "" && !/^(user|project|local)(,(user|project|local))*$/.test(value)) ||
        (value !== "" && new Set(value.split(",")).size !== value.split(",").length)
      ) {
        return unsupported();
      }
      settingSourcesSeen = true;
      args.push("--setting-sources", value);
    }
  }
  return { qualified: true, args };
}
