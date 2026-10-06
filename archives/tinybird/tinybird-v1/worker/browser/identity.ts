import { compareStrings } from "../sessions/session-engine.ts";

export interface BrowserIdentifiers {
  anonymous_id: string | null;
  user_id: string | null;
  email: string | null;
  phone: string | null;
  first_name: string | null;
  last_name: string | null;
}

/** Matches dataform/includes/phone.js, including its deliberate NANP exclusions. */
export function normalizePhone(value: string | null): string | null {
  if (value === null) return null;
  let digits = value.trim().replace(/[^0-9]/g, "");
  if (digits.length === 11 && digits.startsWith("1")) digits = digits.slice(1);
  if (!/^[2-9][0-9]{2}[2-9][0-9]{6}$/.test(digits)) return null;
  if (digits.slice(3, 6) === "555") return null;
  if (/(0000000|1111111|2222222|3333333|4444444|5555555|6666666|7777777|8888888|9999999)$/.test(digits)) return null;
  if (/^(0123456789|1234567890|234567890[0-9]|9876543210)$/.test(digits)) return null;
  return `+1${digits}`;
}

/** Dataform does not validate email syntax here. Preserve that graph behavior. */
export function canonicalEmail(value: string | null): string | null {
  const email = value?.trim().toLowerCase() || null;
  if (!email) return null;
  const [local, domain] = email.split("@");
  if (domain !== "gmail.com" && domain !== "googlemail.com") return email;
  return `${local.replace(/[+].*$/, "").replace(/\./g, "")}@gmail.com`;
}

export function evidenceKeys(identifiers: BrowserIdentifiers): string[] {
  const values = {
    anonymous_id: identifiers.anonymous_id,
    user_id: identifiers.user_id,
    email: identifiers.email,
    canonical_email: canonicalEmail(identifiers.email),
    phone: normalizePhone(identifiers.phone),
  };
  return Object.entries(values)
    .filter(([, value]) => value !== null)
    .map(([type, value]) => `${type}:${value}`)
    .sort(compareStrings);
}
