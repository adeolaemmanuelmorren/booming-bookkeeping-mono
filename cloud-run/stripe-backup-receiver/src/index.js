import { createServer } from "node:http";
import { BigQuery } from "@google-cloud/bigquery";
import { handler } from "./app.js";

// ADC uses the Cloud Run service identity. No Google key leaves GCP.
const bigquery = new BigQuery({ projectId: "able-folio-499722" });
const dataset = "able-folio-499722.stripe_backup";

async function warehouse(path, body) {
  const options = { location: "US", maximumBytesBilled: "1000000000" };
  if (path === "/window") {
    const [rows] = await bigquery.query({ ...options,
      query: `SELECT CAST(MAX(window_end) AS STRING) AS windowEnd FROM \`${dataset}.completed_windows\` WHERE account = @account`,
      params: { account: body.account },
    });
    return rows[0];
  }
  if (path === "/records") {
    await bigquery.query({ ...options,
      query: `INSERT INTO \`${dataset}.records\` (account, stream, record_id, payload, observed_at, batch_id)
        SELECT @account, JSON_VALUE(record, '$.stream'), JSON_VALUE(record, '$.id'),
          JSON_QUERY(record, '$.payload'), TIMESTAMP(@observedAt), @batchId
        FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@records))) record`,
      params: { account: body.account, observedAt: body.observedAt, batchId: body.batchId, records: JSON.stringify(body.records) },
    });
    return { accepted: body.records.length };
  }
  await bigquery.query({ ...options,
    query: `INSERT INTO \`${dataset}.completed_windows\` (account, window_start, window_end, completed_at, workflow_id)
      VALUES (@account, TIMESTAMP_SECONDS(@start), TIMESTAMP_SECONDS(@end), CURRENT_TIMESTAMP(), @workflowId)`,
    params: { account: body.account, start: body.start, end: body.end, workflowId: body.workflowId },
  });
  return { completed: true };
}

createServer(handler({ token: process.env.RECEIVER_TOKEN, warehouse })).listen(Number(process.env.PORT ?? 8080));
