import * as NodeModule from "node:module";
import {
  hostPath,
  hostFileSystem,
  readBounded,
  digest,
  canonical,
  contained,
  object,
} from "./seed-files.ts";
type JsonObject = Record<string, unknown>;
const EXPO_MANIFEST_HASH = "b64e612beee02fbbf0aa883e9d3d767a2213abb5093d5bd04a2aef87868fd2ba";
const EXPO_STORE_PATH =
  "node_modules/.pnpm/expo@58.0.14_277a83826bb26ba15256633fefeb0bc2/node_modules/expo";
export interface ConfigurationProject {
  directory: string;
  configFile?: string;
}
export interface ConfigurationInput {
  identity: string;
  options: JsonObject;
  origins: Record<string, string>;
  validOptions: boolean;
  hasReferences: boolean;
}
// Validate JSONC shape and duplicate decoded keys, never resolve or merge it.
// Unsupported inheritance must not silently enable retained cache reuse.
function configObject(bytes: Buffer, filename: string): JsonObject {
  const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes).replace(/^\uFEFF/, "");
  let cursor = 0;
  const invalid = (): never => {
    throw new Error(`Unsupported or ambiguous JSONC config at offset ${cursor}: ${filename}`);
  };
  const whitespace = () => {
    for (;;) {
      while (/[ \t\r\n]/.test(text[cursor] ?? "x")) cursor++;
      if (text.startsWith("//", cursor)) {
        const end = text.indexOf("\n", cursor + 2);
        cursor = end < 0 ? text.length : end + 1;
      } else if (text.startsWith("/*", cursor)) {
        const end = text.indexOf("*/", cursor + 2);
        if (end < 0) invalid();
        cursor = end + 2;
      } else return;
    }
  };
  const string = (): string => {
    if (text[cursor] !== '"') invalid();
    const start = cursor++;
    while (cursor < text.length) {
      const char = text[cursor++];
      if (char === "\\") cursor++;
      else if (char === '"') {
        try {
          return JSON.parse(text.slice(start, cursor)) as string;
        } catch {
          invalid();
        }
      }
    }
    return invalid();
  };
  const value = (depth: number): unknown => {
    if (depth > 64) invalid();
    whitespace();
    const char = text[cursor];
    if (char === '"') return string();
    if (char === "{" || char === "[") {
      cursor++;
      const end = char === "{" ? "}" : "]";
      const record: JsonObject = Object.create(null);
      const list: unknown[] = [];
      const keys = new Set<string>();
      whitespace();
      while (text[cursor] !== end) {
        if (char === "{") {
          const key = string();
          if (depth === 0 && key === "extends" && keys.has(key)) invalid();
          keys.add(key);
          whitespace();
          if (text[cursor++] !== ":") invalid();
          record[key] = value(depth + 1);
        } else list.push(value(depth + 1));
        whitespace();
        if (text[cursor] === end) break;
        if (text[cursor++] !== ",") invalid();
        whitespace();
      }
      cursor++;
      return char === "{" ? record : list;
    }
    const token = /^(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/.exec(
      text.slice(cursor),
    );
    if (!token) return invalid();
    cursor += token[0].length;
    return JSON.parse(token[0]) as unknown;
  };
  const parsed = value(0);
  whitespace();
  if (cursor !== text.length || !object(parsed)) return invalid();
  return parsed;
}

export async function configurationSnapshots(
  root: string,
  projects: ConfigurationProject[],
): Promise<Map<string, ConfigurationInput>> {
  const files = new Map<string, { config: JsonObject; hash: string }>();
  const read = async (filename: string) => {
    const cached = files.get(filename);
    if (cached) return cached;
    if (
      !contained(root, filename) ||
      (await hostFileSystem((fs) => fs.realPath(filename))) !== filename
    ) {
      throw new Error(`Unsupported config path or symlink escape: ${filename}`);
    }
    const bytes = await readBounded(filename, 1024 * 1024);
    if (!bytes) throw new Error(`Missing or oversized config input (1 MiB limit): ${filename}`);
    const input = { config: configObject(bytes, filename), hash: digest(bytes) };
    files.set(filename, input);
    return input;
  };
  const identities = new Map<string, ConfigurationInput>();
  for (const project of projects) {
    const filename = project.configFile ?? hostPath().join(project.directory, "tsconfig.json");
    const input = await read(filename);
    const extendsValue = input.config.extends;
    if (!Object.hasOwn(input.config, "extends")) {
      identities.set(project.directory, {
        identity: digest(canonical({ filename, projectHash: input.hash })),
        ...effectiveOptions(input.config, filename),
      });
      continue;
    }
    if (typeof extendsValue !== "string")
      throw new Error(`Unsupported nonliteral or missing extends: ${filename}`);
    let base: string;
    let binding: unknown = null;
    if (
      extendsValue === "expo/tsconfig.base.json" &&
      project.directory === hostPath().join(root, "apps", "mobile")
    ) {
      const packageBinding = hostPath().join(project.directory, "node_modules", "expo");
      const packageDirectory = await hostFileSystem((fs) => fs.realPath(packageBinding));
      if (packageDirectory !== hostPath().join(root, EXPO_STORE_PATH)) {
        throw new Error(`Unsupported external Expo config binding: ${packageBinding}`);
      }
      const require = NodeModule.createRequire(hostPath().join(project.directory, "package.json"));
      const manifest = hostPath().join(packageDirectory, "package.json");
      base = hostPath().join(packageDirectory, "tsconfig.base.json");
      if (
        (await hostFileSystem((fs) => fs.realPath(require.resolve("expo/package.json")))) !==
          manifest ||
        (await hostFileSystem((fs) => fs.realPath(require.resolve(extendsValue)))) !== base
      ) {
        throw new Error(`Ambiguous Expo config binding: ${packageBinding}`);
      }
      const packageInput = await read(manifest);
      if (packageInput.config.name !== "expo" || packageInput.hash !== EXPO_MANIFEST_HASH)
        throw new Error(
          `Unqualified Expo package identity: ${manifest}; use --fresh until requalified.`,
        );
      binding = {
        path: packageBinding,
        resolved: packageDirectory,
        manifest,
        manifestHash: packageInput.hash,
      };
    } else {
      base = hostPath().join(root, "tsconfig.base.json");
      if (
        !extendsValue.startsWith(".") ||
        hostPath().isAbsolute(extendsValue) ||
        hostPath().resolve(hostPath().dirname(filename), extendsValue) !== base
      ) {
        throw new Error(
          `Unsupported config inheritance in ${filename}: only the worktree tsconfig.base.json or mobile Expo base is recognized.`,
        );
      }
    }
    const baseInput = await read(base);
    if (Object.hasOwn(baseInput.config, "extends"))
      throw new Error(`Unsupported nonterminal config base: ${base}`);
    identities.set(project.directory, {
      identity: digest(
        canonical({ filename, projectHash: input.hash, base, baseHash: baseInput.hash, binding }),
      ),
      ...effectiveOptions(input.config, filename, baseInput.config, base),
    });
  }
  return identities;
}

// Only the already-admitted terminal base and project can supply these options.
// Raw bytes, including plugins/overrides, remain the retention identity above.
function effectiveOptions(
  project: JsonObject,
  filename: string,
  base: JsonObject = {},
  baseFile = filename,
) {
  const options: JsonObject = {};
  const origins: Record<string, string> = {};
  let validOptions = true;
  let hasReferences = false;
  for (const [config, origin] of [
    [base, baseFile],
    [project, filename],
  ] as const) {
    if (config.compilerOptions !== undefined && !object(config.compilerOptions))
      validOptions = false;
    if (
      config.references !== undefined &&
      (!Array.isArray(config.references) || config.references.length)
    )
      hasReferences = true;
    for (const [key, value] of Object.entries(
      object(config.compilerOptions) ? config.compilerOptions : {},
    )) {
      options[key] = value;
      origins[key] = origin;
    }
  }
  return { options, origins, validOptions, hasReferences };
}
export async function configurationInputs(
  root: string,
  projects: ConfigurationProject[],
): Promise<Map<string, string>> {
  return new Map(
    [...(await configurationSnapshots(root, projects))].map(([key, value]) => [
      key,
      value.identity,
    ]),
  );
}
