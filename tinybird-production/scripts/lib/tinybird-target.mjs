const validTargets = new Set(["cloud", "staging", "branch"]);

export function resolveTinybirdTarget(argumentsByName) {
  const requestedTarget = argumentsByName.target;
  const branch = argumentsByName.branch;
  const target = requestedTarget ?? (branch ? "branch" : "cloud");

  if (!validTargets.has(target)) {
    throw new Error(`--target must be cloud, staging, or branch, received ${target}`);
  }

  if (target === "cloud") {
    if (branch) {
      throw new Error("--branch cannot be used with --target=cloud");
    }

    return {
      name: "cloud",
      branch: null,
      cliArguments: ["--cloud"],
    };
  }

  if (target === "staging") {
    if (branch) {
      throw new Error("--branch cannot be used with --target=staging");
    }

    return {
      name: "staging",
      branch: null,
      cliArguments: ["--cloud", "--staging"],
    };
  }

  if (!branch) {
    throw new Error("--target=branch requires --branch=<branch-name>");
  }

  return {
    name: "branch",
    branch,
    cliArguments: ["--branch", branch],
  };
}
