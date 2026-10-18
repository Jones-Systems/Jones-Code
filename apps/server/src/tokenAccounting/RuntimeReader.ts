import { TokenAccountingReportId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { makeProcessReader } from "./ProcessReader.ts";
import { ProcessReaderConfiguration } from "./ProcessReaderConfig.ts";
import { unconfiguredReader, type TokenAccountingReaderPort } from "./Reader.ts";

const decodeConfiguration = Schema.decodeUnknownSync(ProcessReaderConfiguration);
const decodeReportId = Schema.decodeUnknownSync(TokenAccountingReportId);

function unverifiedReader(reportId: string | undefined): TokenAccountingReaderPort {
  let configuredReportId: string | null = null;
  try {
    configuredReportId = reportId?.length === 64 ? decodeReportId(reportId) : null;
  } catch {
    configuredReportId = null;
  }
  return {
    checkBinding: Effect.succeed({
      status: "unconfigured",
      reason: "host_binding_unverified",
      configuredReportId,
    }),
    readSummary: () =>
      Effect.succeed({ status: "unconfigured", reason: "host_binding_unverified" }),
  };
}

/** Startup configuration constructs a reader; its checkBinding separately verifies enrollment. */
export function makeRuntimeReader(
  environment: Readonly<Record<string, string | undefined>>,
  readerFactory: (
    configuration: ProcessReaderConfiguration,
  ) => TokenAccountingReaderPort = makeProcessReader,
): TokenAccountingReaderPort {
  const bindingPath = environment.T3_TOKEN_ACCOUNTING_BINDING_PATH;
  const bindingSha256 = environment.T3_TOKEN_ACCOUNTING_BINDING_SHA256;
  const reportId = environment.T3_TOKEN_ACCOUNTING_REPORT_ID;
  if (bindingPath === undefined && bindingSha256 === undefined && reportId === undefined) {
    return unconfiguredReader;
  }
  try {
    const configuration = decodeConfiguration({ bindingPath, bindingSha256, reportId });
    // Startup pins must remain exactly 64 characters independently of schema decoding.
    if (configuration.bindingSha256.length !== 64 || configuration.reportId.length !== 64) {
      return unverifiedReader(reportId);
    }
    return readerFactory(Object.freeze(configuration));
  } catch {
    return unverifiedReader(reportId);
  }
}
