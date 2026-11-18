import { createServer } from "node:http";
import { BigQuery } from "@google-cloud/bigquery";
import { handler } from "./app.js";

// Google credentials stay in GCP through the Cloud Run runtime identity.
const bigquery = new BigQuery({ projectId: "able-folio-499722" });
const dataset = "able-folio-499722.activecampaign_backup";

async function warehouse(path, body) {
  const options = { location: "US", maximumBytesBilled: "1000000000" };
  if (path === "/window") {
    const [rows] = await bigquery.query({ ...options,
      query: `SELECT CAST(MAX(window_end) AS STRING) AS windowEnd FROM \`${dataset}.completed_windows\``,
    });
    return rows[0];
  }
  if (path === "/records") {
    await bigquery.query({ ...options,
      query: `INSERT INTO \`${dataset}.records\` (stream, record_id, payload, observed_at, batch_id, workflow_id)
        SELECT JSON_VALUE(record, '$.stream'), JSON_VALUE(record, '$.id'),
          JSON_QUERY(record, '$.payload'), TIMESTAMP(@observedAt), @batchId, @workflowId
        FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@records))) record`,
      params: {
        workflowId: body.workflowId, observedAt: body.observedAt,
        batchId: body.batchId, records: JSON.stringify(body.records),
      },
    });
    return { accepted: body.records.length };
  }
  // Verify the acknowledged pages before publishing the workflow to Dataform.
  // Physical retry copies are allowed; each source ID counts once.
  await bigquery.query({ ...options,
    query: `
      ASSERT (SELECT COUNT(DISTINCT record_id) FROM \`${dataset}.records\`
        WHERE observed_at >= TIMESTAMP_SECONDS(@start)
          AND workflow_id = @workflowId AND stream = 'contacts') = @contactCount
        AS 'Incomplete contact publication';
      ASSERT (SELECT COUNT(DISTINCT record_id) FROM \`${dataset}.records\`
        WHERE observed_at >= TIMESTAMP_SECONDS(@start)
          AND workflow_id = @workflowId AND stream = 'tags') = @tagCount
        AS 'Incomplete tag catalog publication';
      INSERT INTO \`${dataset}.completed_windows\`
        (window_start, window_end, completed_at, workflow_id, catalog_observed_at, contact_count, tag_count)
      SELECT TIMESTAMP_SECONDS(@start), TIMESTAMP_SECONDS(@end), CURRENT_TIMESTAMP(),
        @workflowId, TIMESTAMP(@catalogObservedAt), @contactCount, @tagCount
      FROM (SELECT 1)
      WHERE NOT EXISTS (SELECT 1 FROM \`${dataset}.completed_windows\` WHERE workflow_id = @workflowId)`,
    params: {
      start: body.start, end: body.end, workflowId: body.workflowId,
      catalogObservedAt: body.catalogObservedAt, contactCount: body.contactCount, tagCount: body.tagCount,
    },
  });
  return { completed: true };
}

createServer(handler({ token: process.env.RECEIVER_TOKEN, warehouse })).listen(Number(process.env.PORT ?? 8080));
