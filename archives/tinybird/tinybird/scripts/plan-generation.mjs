import { readFile } from "node:fs/promises";

const projectRoot = new URL("../", import.meta.url);
const planUrl = new URL("project/generation-plan.json", projectRoot);
const plan = JSON.parse(await readFile(planUrl, "utf8"));

const stageIds = plan.stages.map((stage) => stage.id);
if (new Set(stageIds).size !== stageIds.length) {
  throw new Error("Generation plan contains duplicate stage IDs.");
}

for (const stage of plan.stages) {
  if (!stage.kind || !stage.resource) {
    throw new Error(`Generation stage ${stage.id} must declare kind and resource.`);
  }
}

const completed = new Set();
const stagesById = new Map(plan.stages.map((stage) => [stage.id, stage]));
const remaining = new Map(stagesById);
const ordered = [];

while (remaining.size > 0) {
  const ready = [...remaining.values()].filter((stage) =>
    stage.after.every((dependency) => completed.has(dependency)),
  );

  if (ready.length === 0) {
    throw new Error("Generation plan contains a dependency cycle.");
  }

  for (const stage of ready) {
    ordered.push(stage.id);
    completed.add(stage.id);
    remaining.delete(stage.id);
  }
}

const orderedStages = ordered.map((stageId) => stagesById.get(stageId));

if (process.argv.includes("--json")) {
  console.log(JSON.stringify({
    version: plan.version,
    publicationRule: plan.publication_rule,
    stages: orderedStages,
  }, null, 2));
} else {
  console.log("Boom analytics generation plan");
  for (const [index, stage] of orderedStages.entries()) {
    console.log(`${index + 1}. ${stage.id} [${stage.kind}] -> ${stage.resource}`);
  }

  console.log("\nThis command is read-only. It does not call Tinybird or a provider API.");
}
