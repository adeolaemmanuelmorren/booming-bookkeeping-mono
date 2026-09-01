import { readFileSync } from "node:fs";
import path from "node:path";

export function loadOutputContract(projectRoot, contractPath) {
  const rawContract = JSON.parse(readFileSync(contractPath, "utf8"));
  validateContractShape(rawContract);

  const dataformRoot = path.resolve(projectRoot, rawContract.dataform_root);
  const outputs = rawContract.outputs.map((output) => {
    const dataformPath = path.resolve(dataformRoot, output.dataform_definition);
    const uniqueKey = readDataformUniqueKey(dataformPath);
    const diagnosticKey = uniqueKey.length > 0
      ? uniqueKey
      : output.diagnostic_key;

    if (!diagnosticKey || diagnosticKey.length === 0) {
      throw new Error(`${output.name} needs a diagnostic key`);
    }

    return {
      ...output,
      tinybird_resource: output.tinybird_resource ?? output.name,
      unique_key: uniqueKey,
      diagnostic_key: diagnosticKey,
      dataform_path: dataformPath,
    };
  });

  return {
    ...rawContract,
    outputs,
  };
}

export function readDataformUniqueKey(dataformPath) {
  const source = readFileSync(dataformPath, "utf8");
  const configEnd = source.indexOf("}\n");
  const config = configEnd === -1 ? source : source.slice(0, configEnd + 1);
  const match = config.match(/uniqueKey\s*:\s*\[([^\]]*)\]/s);

  if (!match) {
    return [];
  }

  return [...match[1].matchAll(/["']([^"']+)["']/g)].map(
    (keyMatch) => keyMatch[1],
  );
}

export function selectOutputs(contract, selection) {
  const selectedNames = new Set(selection.outputs ?? []);
  const layer = selection.layer;

  if (selectedNames.size > 0 && layer !== null) {
    throw new Error("Use --output or --layer, not both");
  }

  if (selectedNames.size > 0) {
    const knownNames = new Set(contract.outputs.map((output) => output.name));
    const unknownNames = [...selectedNames].filter((name) => !knownNames.has(name));

    if (unknownNames.length > 0) {
      throw new Error(`Unknown output: ${unknownNames.join(", ")}`);
    }

    return contract.outputs.filter((output) => selectedNames.has(output.name));
  }

  if (layer !== null) {
    if (!Number.isInteger(layer) || layer < 1 || layer > 6) {
      throw new Error("--layer must be an integer from 1 through 6");
    }

    return contract.outputs.filter((output) => output.layer === layer);
  }

  return contract.outputs;
}

function validateContractShape(contract) {
  if (contract.version !== 1) {
    throw new Error(`Unsupported parity contract version ${contract.version}`);
  }

  if (!Array.isArray(contract.outputs) || contract.outputs.length !== 40) {
    throw new Error("The parity contract must contain exactly 40 outputs");
  }

  if (!Number.isInteger(contract.bucket_count) || contract.bucket_count < 2) {
    throw new Error("bucket_count must be an integer greater than one");
  }

  const names = contract.outputs.map((output) => output.name);
  if (new Set(names).size !== names.length) {
    throw new Error("The parity contract contains duplicate output names");
  }

  for (const output of contract.outputs) {
    validateOutput(output);
  }
}

function validateOutput(output) {
  if (!/^[a-z][a-z0-9_]*$/.test(output.name)) {
    throw new Error(`Invalid output name ${output.name}`);
  }

  if (!Number.isInteger(output.layer) || output.layer < 1 || output.layer > 6) {
    throw new Error(`${output.name} has an invalid layer`);
  }

  if (!["table", "view"].includes(output.bigquery_kind)) {
    throw new Error(`${output.name} has an invalid BigQuery kind`);
  }

  if (!output.dataform_definition) {
    throw new Error(`${output.name} has no Dataform definition`);
  }

  const digestShards = output.digest_shards ?? 1;
  if (!Number.isInteger(digestShards) || digestShards < 1 || digestShards > 64) {
    throw new Error(`${output.name} has an invalid digest shard count`);
  }

  if (digestShards > 1 && !/^[a-z][a-z0-9_]*$/.test(output.digest_shard_key ?? "")) {
    throw new Error(`${output.name} needs a valid digest shard key`);
  }

  const shardStrategy = output.digest_shard_strategy ?? "hash";
  if (!["hash", "hex_range"].includes(shardStrategy)) {
    throw new Error(`${output.name} has an invalid digest shard strategy`);
  }

  if (shardStrategy === "hex_range" && ![1, 2, 4, 8, 16].includes(digestShards)) {
    throw new Error(`${output.name} hex range shards must divide 16`);
  }

  for (const [fieldPath, policy] of Object.entries(output.array_policies ?? {})) {
    if (!fieldPath || !["ordered", "set"].includes(policy)) {
      throw new Error(`${output.name} has an invalid array policy`);
    }
  }
}
