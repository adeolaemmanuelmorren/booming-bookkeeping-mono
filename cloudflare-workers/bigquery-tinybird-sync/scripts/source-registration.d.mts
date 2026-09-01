interface BigQuerySource {
  project: string;
  dataset: string;
  table: string;
}

interface RawContract {
  tables: Array<{
    resourceName: string;
    source: BigQuerySource;
  }>;
}

interface DerivedContract {
  exports: Array<{
    resourceName: string;
    unionSources: Array<{ source: BigQuerySource }>;
  }>;
}

interface SourceRegistry {
  sources: Array<{
    id: string;
    inputs?: string[];
  }>;
}

export function validateSourceRegistration(
  rawContract: RawContract,
  derivedContract: DerivedContract,
  sourceRegistry: SourceRegistry,
): void;
