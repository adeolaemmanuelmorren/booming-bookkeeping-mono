export type JsonPrimitive = boolean | number | string | null;

export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];

export interface JsonObject {
  [key: string]: JsonValue;
}

export interface JitsuObservation {
  tenant_id: string;
  producer_id: string;
  producer_sequence: number;
  delivery_event_id: string;
  payload_hash: string;
  message_id: string;
  event_kind: string;
  observed_at: string;
  ingested_at: string;
  event_timestamp: string | null;
  anonymous_id: string | null;
  user_id: string | null;
  email: string | null;
  phone: string | null;
  first_name: string | null;
  last_name: string | null;
  customer_name: string | null;
  page_url: string | null;
  page_path: string | null;
  page_referrer: string | null;
  form_id: string | null;
  form_name: string | null;
  form_action: string | null;
  submitted_at: string | null;
  is_checkout_form: boolean | null;
  is_payment_confirmed: boolean | null;
  payment_status: string | null;
  amount: number | null;
  value: number | null;
  currency: string | null;
  product_id: string | null;
  product_name: string | null;
  products: string | null;
  utm_source: string | null;
  utm_medium: string | null;
  utm_campaign: string | null;
  utm_content: string | null;
  utm_term: string | null;
  utm_id: string | null;
  campaign_id: string | null;
  adset_id: string | null;
  ad_id: string | null;
  fbclid: string | null;
  source_fact_version: number;
  source_deleted: 0 | 1;
  fact_payload_hash: string;
  fact_payload: string;
}

export interface QueueEnvelope {
  schema_version: "jitsu_events_api_v1";
  producer_id: string;
  events: JitsuObservation[];
}

export interface WorkerEnv {
  JITSU_EVENTS_QUEUE: Queue<QueueEnvelope>;
  JITSU_WEBHOOK_TOKEN: string;
  TENANT_ID: string;
  TINYBIRD_API_URL: string;
  TINYBIRD_APPEND_TOKEN: string;
  TINYBIRD_DATASOURCE: string;
}

export type Fetcher = (
  input: Request | string | URL,
  init?: RequestInit,
) => Promise<Response>;
