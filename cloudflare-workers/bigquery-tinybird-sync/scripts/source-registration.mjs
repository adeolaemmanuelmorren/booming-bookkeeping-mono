function sourceName(source) {
  return `${source.dataset}.${source.table}`;
}

function registeredOwners(source, registrations) {
  const exactName = sourceName(source);
  const owners = registrations
    .filter(({ input }) => input === source.dataset || input === exactName)
    .map(({ sourceId }) => sourceId);

  return [...new Set(owners)];
}

export function validateSourceRegistration(
  rawContract,
  derivedContract,
  sourceRegistry,
) {
  if (!Array.isArray(sourceRegistry.sources)) {
    throw new Error("The source registry has no sources array.");
  }

  const registrations = sourceRegistry.sources.flatMap((source) => {
    if (!Array.isArray(source.inputs)) {
      throw new Error(`Source registry entry ${source.id} has no inputs array.`);
    }

    return source.inputs.map((input) => ({ sourceId: source.id, input }));
  });
  const physicalSources = [
    ...rawContract.tables.map((table) => ({
      resourceName: table.resourceName,
      source: table.source,
    })),
    ...derivedContract.exports.flatMap((exportDefinition) => (
      exportDefinition.unionSources.map((unionSource) => ({
        resourceName: exportDefinition.resourceName,
        source: unionSource.source,
      }))
    )),
  ];

  for (const physicalSource of physicalSources) {
    const owners = registeredOwners(physicalSource.source, registrations);
    const name = sourceName(physicalSource.source);

    if (owners.length === 0) {
      throw new Error(
        `${physicalSource.resourceName} reads ${name}, but no source registry entry owns that input.`,
      );
    }

    if (owners.length > 1) {
      throw new Error(
        `${physicalSource.resourceName} reads ${name}, which is owned by multiple source registry entries: ${owners.join(", ")}.`,
      );
    }
  }
}
