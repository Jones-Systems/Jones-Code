// @effect-diagnostics nodeBuiltinImport:off -- Runs synchronously in boot before profile configuration.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import {
  holdJonesNativeWriterFence,
  holdJonesDesktopOwner,
  asJonesNativeStartupRefusal,
  JonesNativeStartupRefusal,
  type JonesNativeWriterAdmission,
} from "@t3tools/shared/jones/nativeWriterFence";
import { isCompanionPackage } from "@t3tools/shared/jones/previewCompanionProduct";
import { holdJonesColdStartup } from "./jonesColdBackendCustody.ts";
import { jonesNativeHelperSource } from "./jonesNativeHelperSource.ts";
import { resolveDesktopUserDataOverride } from "../../app/DesktopUserDataOverride.ts";

let resolveDesktopOwner: (() => JonesNativeWriterAdmission) | undefined;

/** Called only by primary backend configuration, after Clerk forwards secondary launches. */
export function currentJonesDesktopOwner(): (() => JonesNativeWriterAdmission) | undefined {
  return resolveDesktopOwner;
}

type StartupCommand = (
  command: string,
  args: readonly string[],
  options: NodeChildProcess.SpawnSyncOptionsWithStringEncoding,
) => Pick<NodeChildProcess.SpawnSyncReturns<string>, "status" | "signal" | "error" | "stdout">;

function materializeHelper(home: string): string {
  const root = NodePath.join(home, "runtime", "jones-updates");
  const owner = NodeFS.lstatSync(root);
  if (
    !owner.isDirectory() ||
    owner.isSymbolicLink() ||
    owner.uid !== process.getuid?.() ||
    NodeFS.realpathSync(root) !== root
  )
    throw new Error("Native updater directory ownership is unknown.");
  const hash = NodeCrypto.createHash("sha256").update(jonesNativeHelperSource).digest("hex");
  const helper = NodePath.join(root, "activate-v1-" + hash + ".py");
  try {
    const fd = NodeFS.openSync(helper, "wx", 0o700);
    try {
      NodeFS.writeFileSync(fd, jonesNativeHelperSource, "utf8");
      NodeFS.fsyncSync(fd);
    } finally {
      NodeFS.closeSync(fd);
    }
    const directory = NodeFS.openSync(root, "r");
    try {
      NodeFS.fsyncSync(directory);
    } finally {
      NodeFS.closeSync(directory);
    }
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code !== "EEXIST") throw cause;
  }
  const fd = NodeFS.openSync(helper, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    const info = NodeFS.fstatSync(fd);
    if (
      !info.isFile() ||
      info.nlink !== 1 ||
      info.uid !== process.getuid?.() ||
      (info.mode & 0o777) !== 0o700 ||
      info.size !== Buffer.byteLength(jonesNativeHelperSource) ||
      NodeFS.readFileSync(fd, "utf8") !== jonesNativeHelperSource
    )
      throw new Error("The embedded native helper differs; startup held.");
  } finally {
    NodeFS.closeSync(fd);
  }
  return helper;
}

/** C1 remains the only journal writer and takes its own nonblocking activation lock. */
export function settleInterruptedJonesActivation(
  input: {
    readonly home: string;
    readonly transactionId: string;
    readonly env: Record<string, string | undefined>;
  },
  run: StartupCommand = (command, args, options) =>
    NodeChildProcess.spawnSync(command, [...args], options),
): void {
  if (
    !/^[a-f0-9]{64}$/.test(input.transactionId) ||
    input.env.T3CODE_JONES_TRIAL_DESCRIPTOR !== undefined ||
    input.env.T3CODE_JONES_COLD_TRIAL_DESCRIPTOR !== undefined ||
    input.env.T3CODE_JONES_COLD_CHANNEL_FD !== undefined
  )
    throw new Error("Native startup settlement ownership is unknown.");
  const helper = materializeHelper(input.home);
  const result = run(
    "/usr/bin/python3",
    [
      helper,
      "--manifest",
      NodePath.join(input.home, "runtime", "jones-active-install.json"),
      "--settle-interrupted-activation",
      input.transactionId,
    ],
    { env: input.env, encoding: "utf8", timeout: 10000, maxBuffer: 65536, windowsHide: true },
  );
  if (
    result.error !== undefined ||
    result.signal !== null ||
    result.status !== 0 ||
    typeof result.stdout !== "string"
  )
    throw new Error("Native startup settlement did not complete.", { cause: result.error });
  const decoded: unknown = JSON.parse(result.stdout);
  if (decoded === null || typeof decoded !== "object" || Array.isArray(decoded))
    throw new Error("Native startup settlement returned invalid evidence.");
  const value = decoded as Record<string, unknown>;
  if (
    value.protocol !== 1 ||
    !["settled", "refused", "uncertain"].includes(String(value.status)) ||
    Object.keys(value).some((key) => !["protocol", "status", "reason"].includes(key)) ||
    ("reason" in value && typeof value.reason !== "string")
  )
    throw new Error("Native startup settlement returned invalid evidence.");
  if (value.status !== "settled")
    throw new Error(
      "Native startup settlement " +
        String(value.status) +
        (typeof value.reason === "string" ? ": " + value.reason.slice(0, 512) : "."),
    );
}

/** A successful settlement permits one fresh fence check, never a retry loop. */
export function recoverJonesNativeStartup(
  admit: () => JonesNativeWriterAdmission | undefined,
  settle: (transactionId: string) => void,
): JonesNativeWriterAdmission | undefined {
  try {
    return admit();
  } catch (cause) {
    const refusal = asJonesNativeStartupRefusal(cause);
    if (refusal.code !== "transaction-settleable" || refusal.transactionId === undefined)
      throw refusal;
    try {
      settle(refusal.transactionId);
    } catch (failure) {
      const detail = failure instanceof Error ? failure.message : "Unknown settlement failure";
      throw new JonesNativeStartupRefusal(refusal.code, detail, refusal.transactionId, {
        cause: failure,
      });
    }
  }
  return admit();
}

export function reportJonesNativeStartupRefusal(
  cause: unknown,
  output: {
    readonly showErrorBox: (title: string, content: string) => void;
    readonly exit: (code: number) => void;
    readonly writeStderr?: (line: string) => void;
  },
): void {
  const refusal = asJonesNativeStartupRefusal(cause);
  try {
    output.showErrorBox(
      "Jones Code could not start safely",
      "Startup refused (" +
        refusal.code +
        ")." +
        (refusal.transactionId === undefined ? "" : "\nTransaction: " + refusal.transactionId) +
        "\nRecovery help: https://github.com/Jones-Systems/Jones-Code/blob/main/docs/user/updating.md#if-an-update-fails",
    );
  } catch {
    /* Reporting must not keep an inadmissible writer alive. */
  }
  try {
    const line =
      "Jones Code refused native startup: " +
      refusal.message.slice(0, 512).replace(/[\x00-\x1f\x7f]/g, " ") +
      "\n";
    if (output.writeStderr === undefined) NodeFS.writeSync(2, line);
    else output.writeStderr(line);
  } catch {
    /* A closed stderr must not prevent process-lifetime lease release. */
  }
  output.exit(1);
}

export function holdJonesDesktopNativeWriterFence(input: {
  readonly platform: string;
  readonly env: Record<string, string | undefined>;
  readonly homeDirectory: string;
  readonly appDataDirectory: string;
  readonly version: string;
  readonly metadata: unknown;
}): void {
  resolveDesktopOwner = undefined;
  holdJonesColdStartup(input.env);
  if (input.platform !== "darwin" || isCompanionPackage(input.metadata)) return;
  const explicitHome = input.env.T3CODE_HOME?.trim();
  const home = explicitHome || NodePath.join(input.homeDirectory, ".t3");
  const development = Boolean(input.env.VITE_DEV_SERVER_URL?.trim());
  const legacy = NodePath.join(input.appDataDirectory, "T3 Code (Dev)");
  const profile =
    resolveDesktopUserDataOverride(input.env.T3CODE_DESKTOP_USER_DATA_DIR, NodePath) ??
    (development && NodeFS.existsSync(legacy)
      ? legacy
      : NodePath.join(input.appDataDirectory, development ? "t3code-dev" : "t3code-v2"));
  const writerInput = {
    home,
    databasePath: NodePath.join(
      home,
      development && !explicitHome ? "dev" : "userdata",
      "statev2.sqlite",
    ),
    profile,
    descriptorPath: input.env.T3CODE_JONES_TRIAL_DESCRIPTOR,
    version: input.version,
    buildMetadata: input.metadata,
  };
  const admission = recoverJonesNativeStartup(
    () => holdJonesNativeWriterFence(writerInput),
    (transactionId) => settleInterruptedJonesActivation({ home, transactionId, env: input.env }),
  );
  if (admission === undefined && writerInput.descriptorPath === undefined) return;
  let owner: JonesNativeWriterAdmission | undefined;
  resolveDesktopOwner = () => {
    if (owner === undefined) {
      const ordinary = holdJonesNativeWriterFence({ ...writerInput, descriptorPath: undefined });
      if (ordinary === undefined)
        throw new Error("Desktop ownership requires ordinary native admission.");
      owner = holdJonesDesktopOwner(ordinary);
    }
    owner.assertCurrent();
    return owner;
  };
}
