"use strict";

const fs = require("node:fs");
const path = require("node:path");
const F = require("./files.cjs");

function artifact(file) {
  F.absolute(file);
  return { path: file, sha256: F.hashFile(file) };
}

function inventory(root) {
  F.absolute(root);
  F.insist(
    fs.lstatSync(root).isDirectory() && fs.realpathSync(root) === root,
    "Runtime root must be a canonical directory",
  );
  const files = [];
  function walk(directory) {
    for (const name of fs.readdirSync(directory).sort()) {
      const file = path.join(directory, name);
      const stat = fs.lstatSync(file);
      if (stat.isDirectory()) walk(file);
      else {
        F.insist(
          stat.isFile() && !stat.isSymbolicLink(),
          "Runtime payload contains a symlink or special file",
        );
        files.push({ path: path.relative(root, file), sha256: F.hashFile(file) });
      }
    }
  }
  walk(root);
  files.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
  return { root, files };
}

function candidatePlan(input, placeholderPath) {
  F.insist(
    input && typeof input === "object" && !Array.isArray(input),
    "Candidate plan must be an object",
  );
  F.insist(
    input.qualification === undefined,
    "Qualification must be supplied separately at finalize",
  );
  // The placeholder satisfies structural validation only; it is never persisted or verified as evidence.
  const plan = F.validatePlan({
    ...input,
    qualification: { path: placeholderPath, sha256: "0".repeat(64) },
  });
  delete plan.qualification;
  return plan;
}

function verifyInputs(plan) {
  for (const side of ["source", "target"]) {
    F.verifyPayload(plan[side]);
    F.verifyArtifact(plan[side].config);
  }
  F.verifyRuntimeState(plan);
  if (plan.service.fragment) F.verifyArtifact(plan.service.fragment);
  for (const dropIn of plan.service.dropIns) F.verifyArtifact(dropIn);
}

function writeNew(file, value, plan) {
  F.absolute(file);
  const directory = path.dirname(file);
  F.insist(
    fs.realpathSync(directory) === directory && fs.lstatSync(directory).isDirectory(),
    "Output parent must be an existing canonical directory",
  );
  for (const root of [plan.home, plan.source.payload.root, plan.target.payload.root]) {
    F.insist(
      file !== root && !file.startsWith(`${root}/`),
      "Output must be outside state home and runtime payloads",
    );
  }
  F.insist(
    file !== plan.service.configPath,
    "Output must not be the installed service configuration",
  );
  const bytes = `${JSON.stringify(value, null, 2)}\n`;
  const fd = fs.openSync(file, "wx", 0o600);
  try {
    fs.writeFileSync(fd, bytes);
    fs.fsyncSync(fd);
  } catch (error) {
    fs.closeSync(fd);
    fs.unlinkSync(file);
    throw error;
  }
  fs.closeSync(fd);
  F.syncDirectory(directory);
  return F.sha256(bytes);
}

function prepare(requestPath, outputPath) {
  F.absolute(requestPath);
  const input = F.readJson(requestPath);
  F.insist(
    input && typeof input === "object" && !Array.isArray(input),
    "Request must be an object",
  );
  F.insist(
    input.qualification === undefined,
    "Qualification must be supplied separately at finalize",
  );
  const plan = structuredClone(input);
  for (const side of ["source", "target"]) {
    F.insist(
      plan[side] && plan[side].payload && plan[side].payload.files === undefined,
      "Request payload must contain a root without a precomputed inventory",
    );
    plan[side].config = artifact(plan[side].config);
    plan[side].executable = artifact(plan[side].executable);
    plan[side].payload = inventory(plan[side].payload.root);
  }
  F.insist(plan.service && typeof plan.service === "object", "Service required");
  if (plan.service.fragment !== undefined) plan.service.fragment = artifact(plan.service.fragment);
  F.insist(
    plan.service.dropIns === undefined || Array.isArray(plan.service.dropIns),
    "dropIns must be a list of absolute paths",
  );
  plan.service.dropIns = (plan.service.dropIns ?? []).map(artifact);
  if (plan.runtimeState !== undefined) {
    F.insist(
      plan.runtimeState && typeof plan.runtimeState === "object",
      "Both staged runtime-state paths required",
    );
    plan.runtimeState = {
      source: artifact(plan.runtimeState.source),
      target: artifact(plan.runtimeState.target),
    };
  }
  F.insist(
    plan.stateFiles === undefined || Array.isArray(plan.stateFiles),
    "stateFiles must be a list of relative paths",
  );
  plan.stateFiles = [...new Set([...F.STATE_FILES, ...(plan.stateFiles ?? [])])];
  const candidate = candidatePlan(plan, requestPath);
  verifyInputs(candidate);
  const bindingDigest = F.bindingDigest(candidate);
  const preparation = {
    schema: "server-swap-preparation/1",
    candidatePlan: candidate,
    bindingDigest,
  };
  const digest = writeNew(outputPath, preparation, candidate);
  return { status: "prepared", preparationPath: outputPath, digest, bindingDigest };
}

function finalize(preparationPath, qualificationPath, outputPath) {
  F.absolute(preparationPath);
  const preparation = F.readJson(preparationPath);
  F.insist(preparation?.schema === "server-swap-preparation/1", "Unsupported preparation schema");
  const candidate = candidatePlan(preparation.candidatePlan, preparationPath);
  F.insist(
    F.canonical(candidate) === F.canonical(preparation.candidatePlan),
    "Preparation candidate is not normalized",
  );
  F.insist(
    preparation.bindingDigest === F.bindingDigest(candidate),
    "Preparation binding digest mismatch",
  );
  const plan = F.validatePlan({ ...candidate, qualification: artifact(qualificationPath) });
  F.verifyQualification(plan);
  verifyInputs(plan);
  const digest = writeNew(outputPath, plan, plan);
  return {
    status: "plan-created",
    planPath: outputPath,
    digest,
    bindingDigest: preparation.bindingDigest,
  };
}

function main(argv) {
  if (argv.length === 3 && argv[0] === "prepare") return prepare(argv[1], argv[2]);
  if (argv.length === 4 && argv[0] === "finalize") return finalize(argv[1], argv[2], argv[3]);
  throw new Error(
    "Expected prepare /absolute/request.json /absolute/new-preparation.json or finalize /absolute/preparation.json /absolute/qualification.json /absolute/new-plan.json",
  );
}

module.exports = { main, prepare, finalize };
