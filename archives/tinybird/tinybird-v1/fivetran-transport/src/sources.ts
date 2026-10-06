import { TABLE_MANIFEST, type SourceTable } from "./table-manifest.generated";

export const SOURCE_NAMES = [
  "raw_activecampaign_contact",
  "raw_activecampaign_contact_tag",
  "raw_activecampaign_tags",
  "raw_stripe_charge",
  "raw_stripe_customer",
  "raw_stripe_kajabi_charge",
  "raw_stripe_kajabi_customer",
  "raw_stripe_kajabi_payment_intent",
  "raw_stripe_payment_intent",
] as const;

const tablesByName = new Map(
  TABLE_MANIFEST.map((table) => [table.resourceName, table]),
);

export const SOURCE_TABLES: readonly SourceTable[] = SOURCE_NAMES.map(
  (name) => {
    const table = tablesByName.get(name);
    if (!table) throw new Error(`Missing source table manifest: ${name}`);
    return table;
  },
);

export function landingName(resourceName: string): string {
  return `v1_fivetran_${resourceName.slice("raw_".length)}`;
}
