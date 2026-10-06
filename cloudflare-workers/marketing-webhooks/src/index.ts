/**
 * Marketing Webhook Processor
 *
 * A modular, queue-based webhook processor for marketing sources.
 * Receives webhooks via HTTP, queues them for processing, and sends
 * transformed events to Segment.
 *
 * Features:
 *   - Modular source architecture for Shopify, Webflow, and ActiveCampaign
 *   - Cloudflare Queue for reliable processing with automatic retries
 *   - Dead Letter Queue with KV persistence
 *   - API enrichment with fail-safe retries
 */

import { env } from "cloudflare:workers";
import type { MarketingWebhookEnv, QueueMessage } from "./types";
import { firstOf, generateUUID, headersToObject } from "./utils";
import { getSource, hasSource, getSourceNames, defaultExtractTopic } from "./sources";
import { processBatch } from "./consumers/processor";
import { handleDLQ, listFailedMessages, replayFailedMessage } from "./consumers/dlq-handler";
import {
	handleDebugEventQuery,
	handleDebugWebhook,
} from "./handlers/debug-webhook";

export { ReverseEtlDebugStore } from "./durable-objects/reverse-etl-debug-store";
export { ActiveCampaignSource } from "../../source-gateways/activecampaign";

// =============================================================================
// HTTP INGESTION HANDLER
// =============================================================================

function isTestEndpointsEnabled(request: Request): boolean {
	const url = new URL(request.url);
	// Always allow on localhost for local development.
	if (url.hostname === "localhost" || url.hostname === "127.0.0.1" || url.hostname === "::1") {
		return true;
	}
	const envRecord = env as unknown as MarketingWebhookEnv;
	return envRecord.ENABLE_TEST_ENDPOINTS === "true";
}

function hasTestEndpointAccess(request: Request): boolean {
	if (!isTestEndpointsEnabled(request)) {
		return false;
	}

	const url = new URL(request.url);
	if (url.hostname === "localhost" || url.hostname === "127.0.0.1" || url.hostname === "::1") {
		return true;
	}

	const envRecord = env as unknown as MarketingWebhookEnv;
	const expectedToken = envRecord.TEST_ENDPOINT_TOKEN;
	if (!expectedToken) {
		return true;
	}

	const authorization = request.headers.get("Authorization");
	return authorization === `Bearer ${expectedToken}`;
}

function buildEdgeContext(request: Request): Record<string, unknown> {
	const cf = (request as Request & { cf?: Record<string, unknown> }).cf;
	if (!cf) return {};

	const edge: Record<string, unknown> = {};
	const supportedFields = [
		"region",
		"regionCode",
		"city",
		"country",
		"continent",
		"postalCode",
		"latitude",
		"longitude",
		"timezone",
	];

	for (const field of supportedFields) {
		const value = cf[field];
		if (value === null || value === undefined || value === "") continue;
		edge[field] = value;
	}

	return edge;
}

function getRequestIp(headers: Headers): string | undefined {
	const forwardedFor = headers.get("x-forwarded-for");
	const forwardedIp = forwardedFor?.split(",")[0]?.trim();

	return firstOf(
		headers.get("cf-connecting-ip"),
		forwardedIp,
		headers.get("x-real-ip")
	) ?? undefined;
}

function buildRequestContext(request: Request): Record<string, unknown> {
	const context: Record<string, unknown> = {};
	const ip = getRequestIp(request.headers);
	const userAgent = request.headers.get("user-agent") ?? undefined;
	const edge = buildEdgeContext(request);

	if (ip) context.ip = ip;
	if (userAgent) context.userAgent = userAgent;
	if (Object.keys(edge).length > 0) context.edge = edge;

	return context;
}

/**
 * Handle incoming webhook requests
 *
 * Routes:
 * - POST /webhook/:source
 */
async function handleWebhook(request: Request): Promise<Response> {
	const url = new URL(request.url);

	// Parse route: /webhook/:source
	const match = url.pathname.match(/^\/webhook\/(\w+)$/);
	if (!match) {
		return jsonResponse(
			{ error: "Invalid webhook path. Expected: /webhook/:source" },
			400
		);
	}

	const [, source] = match;
	console.log(`[Ingestion] Incoming webhook for source: ${source}`);

	// Validate source exists
	if (!hasSource(source)) {
		return jsonResponse(
			{ error: `Unknown source: ${source}. Available: ${getSourceNames().join(", ")}` },
			400
		);
	}

	// Get the source handler
	const sourceHandler = getSource(source);
	if (!sourceHandler) {
		return jsonResponse({ error: `Source not found: ${source}` }, 500);
	}

	// Parse payload
	let payload: unknown;
	try {
		payload = sourceHandler.parsePayload
			? await sourceHandler.parsePayload(request)
			: await request.json();
	} catch {
		const error = sourceHandler.parsePayload
			? "Invalid webhook body"
			: "Invalid JSON body";
		return jsonResponse({ error }, 400);
	}

	// Determine topic
	const headers = headersToObject(request.headers);
	let topic = sourceHandler.extractTopic?.(headers, payload) ?? defaultExtractTopic(headers);

	if (!topic) {
		return jsonResponse(
			{
				error:
					"Could not determine webhook topic. Provide /webhook/:source/:topic or send source-specific headers.",
			},
			400
		);
	}
	console.log(`[Ingestion] Resolved topic: ${topic} for source: ${source}`);

	// Check if topic is supported
	if (!sourceHandler.handlers.has(topic)) {
		const supportedTopics = Array.from(sourceHandler.handlers.keys());
		return jsonResponse(
			{
				error: `Unknown topic: ${topic} for source ${source}`,
				supportedTopics,
			},
			400
		);
	}

	// Validate payload before queuing
	const handler = sourceHandler.handlers.get(topic)!;
	const validation = handler.validate(payload);
	if (!validation.valid) {
		return jsonResponse({ error: `Validation failed: ${validation.error}` }, 400);
	}

	// Build queue message
	const shopifyEventId =
		source === "shopify"
			? firstOf(
					headers["x-shopify-event-id"],
					headers["X-Shopify-Event-Id"],
					headers["X-SHOPIFY-EVENT-ID"]
				)
			: null;
	const messageId = shopifyEventId ?? generateUUID();
	const message: QueueMessage = {
		source,
		topic,
		payload,
		metadata: {
			receivedAt: new Date().toISOString(),
			messageId,
			headers,
			requestContext: buildRequestContext(request),
		},
	};

	// Queue the message for processing
	try {
		await env.WEBHOOK_QUEUE.send(message);
		console.log(`[Ingestion] Queued message ${messageId} to WEBHOOK_QUEUE`);
	} catch (error) {
		console.error("[Ingestion] Failed to queue message:", error);
		return jsonResponse({ error: "Failed to queue webhook for processing" }, 500);
	}

	return jsonResponse({
		ok: true,
		messageId,
		source,
		topic,
	});
}

/**
 * Test-only endpoint: validate + (optionally enrich) + transform and return events.
 *
 * Routes:
 * - POST /__test/transform/:source/:topic
 *
 * Notes:
 * - Disabled by default. Enable by setting ENABLE_TEST_ENDPOINTS="true".
 * - `:topic` may be provided as "orders-paid" (will be converted to "orders/paid").
 * - Query params:
 *   - enrich=true|false (default: false)
 */
async function handleTestTransform(request: Request): Promise<Response> {
	if (!hasTestEndpointAccess(request)) {
		return jsonResponse({ error: "Not found" }, 404);
	}

	const url = new URL(request.url);
	const match = url.pathname.match(/^\/__test\/transform\/(\w+)\/(.+)$/);
	if (!match) {
		return jsonResponse(
			{ error: "Invalid test path. Expected: /__test/transform/:source/:topic" },
			400
		);
	}

	const [, source, rawTopic] = match;
	const topic = rawTopic.includes("/") ? rawTopic : rawTopic.replace(/-/g, "/");

	if (!hasSource(source)) {
		return jsonResponse(
			{ error: `Unknown source: ${source}. Available: ${getSourceNames().join(", ")}` },
			400
		);
	}

	const sourceHandler = getSource(source);
	if (!sourceHandler) {
		return jsonResponse({ error: `Source not found: ${source}` }, 500);
	}

	if (!sourceHandler.handlers.has(topic)) {
		return jsonResponse(
			{
				error: `Unknown topic: ${topic} for source ${source}`,
				supportedTopics: Array.from(sourceHandler.handlers.keys()),
			},
			400
		);
	}

	let payload: unknown;
	try {
		payload = await request.json();
	} catch {
		return jsonResponse({ error: "Invalid JSON body" }, 400);
	}

	const handler = sourceHandler.handlers.get(topic)!;
	const validation = handler.validate(payload);
	if (!validation.valid) {
		return jsonResponse({ error: `Validation failed: ${validation.error}` }, 400);
	}

	const enrich = url.searchParams.get("enrich") === "true";
	let enrichment;
	if (enrich && sourceHandler.enricher) {
		try {
			enrichment = await sourceHandler.enricher.enrich(validation.data);
		} catch (error) {
			return jsonResponse(
				{ error: `Enrichment failed: ${error instanceof Error ? error.message : "Unknown error"}` },
				500
			);
		}
	}

	const events = handler.transform(validation.data, enrichment);

	return jsonResponse({
		ok: true,
		source,
		topic,
		enriched: Boolean(enrichment),
		enrichmentFailed: enrichment?.failed,
		events,
	});
}

/**
 * Handle admin/status requests
 */
async function handleAdmin(request: Request): Promise<Response> {
	const url = new URL(request.url);

	// Health check
	if (url.pathname === "/health" || url.pathname === "/") {
		return jsonResponse({
			ok: true,
			sources: getSourceNames(),
			timestamp: new Date().toISOString(),
		});
	}

	// List DLQ messages
	if (url.pathname === "/admin/dlq" && request.method === "GET") {
		const source = url.searchParams.get("source") || undefined;
		const topic = url.searchParams.get("topic") || undefined;
		const date = url.searchParams.get("date") || undefined;
		const limit = parseInt(url.searchParams.get("limit") || "50", 10);

		const messages = await listFailedMessages({ source, topic, date, limit });
		return jsonResponse({
			messages: messages.keys,
			count: messages.keys.length,
			cursor: messages.cursor,
			complete: messages.complete,
		});
	}

	// Replay a DLQ message
	if (url.pathname === "/admin/dlq/replay" && request.method === "POST") {
		const { key } = (await request.json()) as { key?: string };
		if (!key) {
			return jsonResponse({ error: "Missing 'key' in request body" }, 400);
		}

		const result = await replayFailedMessage(key);
		if (!result.success) {
			return jsonResponse({ error: result.error }, 400);
		}

		return jsonResponse({ ok: true, replayed: key });
	}

	return jsonResponse({ error: "Not found" }, 404);
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

function jsonResponse(data: unknown, status = 200): Response {
	return new Response(JSON.stringify(data), {
		status,
		headers: { "Content-Type": "application/json" },
	});
}

// =============================================================================
// WORKER EXPORT
// =============================================================================

export default {
	/**
	 * HTTP fetch handler - receives webhooks and admin requests
	 */
	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);

		// CORS preflight
		if (request.method === "OPTIONS") {
			return new Response(null, {
				headers: {
					"Access-Control-Allow-Origin": "*",
					"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
					"Access-Control-Allow-Headers":
						"Authorization, Content-Type, X-Shopify-Topic, X-Shopify-HMAC-SHA256",
				},
			});
		}

		// Reverse ETL debug endpoints only log the posted properties.
		if (url.pathname.startsWith("/webhook/debug/")) {
			return handleDebugWebhook(request, env as unknown as MarketingWebhookEnv);
		}

		// Authenticated query access for the Reverse ETL debug audit.
		if (url.pathname === "/admin/debug-events") {
			return handleDebugEventQuery(
				request,
				env as unknown as MarketingWebhookEnv
			);
		}

		// Webhook ingestion
		if (url.pathname.startsWith("/webhook/") && request.method === "POST") {
			return handleWebhook(request);
		}

		// Test endpoints (disabled unless ENABLE_TEST_ENDPOINTS="true")
		if (url.pathname.startsWith("/__test/") && request.method === "POST") {
			return handleTestTransform(request);
		}

		// Legacy single-path webhook (auto-detect from headers)
		if (url.pathname === "/webhook" && request.method === "POST") {
			// Get source and topic from headers
			const headers = headersToObject(request.headers);
			const topic = defaultExtractTopic(headers);

			if (!topic) {
				return jsonResponse(
					{ error: "Could not determine webhook topic. Use /webhook/:source/:topic path." },
					400
				);
			}

			// Assume Shopify for legacy endpoint
			const source = "shopify";

			// Re-route to main handler by constructing equivalent URL
			const newUrl = new URL(`/webhook/${source}`, url.origin);
			const newRequest = new Request(newUrl.toString(), request);
			return handleWebhook(newRequest);
		}

		// Admin endpoints
		if (url.pathname.startsWith("/admin/") || url.pathname === "/health" || url.pathname === "/") {
			return handleAdmin(request);
		}

		return jsonResponse({ error: "Not found" }, 404);
	},

	/**
	 * Queue consumer - processes webhooks from the queue
	 */
	async queue(batch: MessageBatch): Promise<void> {
		// Route based on which queue this batch came from
		const isDLQ = batch.queue === "marketing-webhooks-dlq";

		// Cast messages to our QueueMessage type
		const typedBatch = batch as MessageBatch<QueueMessage>;

		if (isDLQ) {
			// DLQ handler - persist to R2
			await handleDLQ(typedBatch);
		} else {
			// Main processor - enrich, transform, send to Segment
			await processBatch(typedBatch);
		}
	},
};
