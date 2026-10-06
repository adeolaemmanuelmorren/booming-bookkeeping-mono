import {
	CK_PATH,
	HttpError,
	JITSU_SCRIPT_PATH,
	corsifyResponse,
	errorResponse,
	getAllowedOrigin,
	getRequestTenant,
	handleCkRequest,
	handleJitsuEventRequest,
	handleJitsuScriptRequest,
	handlePreflight,
	isJitsuWorkerPath,
	resolveAnonymousIdentity,
	type Env,
} from './jitsu-proxy';
import { ConsentShard } from './consent/consent-shard';
import {
	CONSENT_BOOTSTRAP_PATH,
	CONSENT_STATE_PATH,
	handleConsentBootstrap,
	handleConsentState,
	isConsentPath,
} from './consent/routes';
import { PurchaseState } from './stripe/purchase-state';
import {
	PURCHASE_ATTEMPT_PATH,
	PURCHASE_POLL_PATH,
	handlePurchaseAttemptRequest,
	handlePurchasePollRequest,
	isStripePurchasePath,
} from './stripe/routes';

export { ConsentShard, PurchaseState };
export { StripeSource } from '../../source-gateways/stripe';

function isPublicPath(path: string): boolean {
	return isJitsuWorkerPath(path) ||
		isStripePurchasePath(path) ||
		isConsentPath(path);
}

export default {
	async fetch(request: Request, env: Env, _ctx: ExecutionContext): Promise<Response> {
		const requestOrigin = request.headers.get('Origin');
		const path = new URL(request.url).pathname;
		const tenant = getRequestTenant(request);

		if (!tenant || !isPublicPath(path)) {
			return new Response('Not Found', { status: 404 });
		}

		const allowedOrigin = getAllowedOrigin(requestOrigin, tenant);
		if (requestOrigin && !allowedOrigin) {
			return new Response('Forbidden', { status: 403 });
		}

		if (request.method === 'OPTIONS') {
			return handlePreflight(allowedOrigin);
		}

		try {
			let response: Response;

			if (path === CK_PATH) {
				if (request.method !== 'GET' && request.method !== 'POST') {
					throw new HttpError(405, 'Method not allowed');
				}

				response = await handleCkRequest(request);
				return corsifyResponse(response, allowedOrigin);
			}

			if (path === JITSU_SCRIPT_PATH) {
				if (request.method !== 'GET') {
					throw new HttpError(405, 'Method not allowed');
				}

				const identity = resolveAnonymousIdentity(request, tenant);
				response = await handleJitsuScriptRequest(env, identity);
				return corsifyResponse(response, allowedOrigin);
			}

			if (request.method !== 'POST') {
				throw new HttpError(405, 'Method not allowed');
			}

			if (path === CONSENT_BOOTSTRAP_PATH) {
				response = await handleConsentBootstrap(request, env, tenant);
				return corsifyResponse(response, allowedOrigin);
			}

			if (path === CONSENT_STATE_PATH) {
				response = await handleConsentState(request, env, tenant);
				return corsifyResponse(response, allowedOrigin);
			}

			if (path === PURCHASE_ATTEMPT_PATH) {
				response = await handlePurchaseAttemptRequest(request, env, tenant);
				return corsifyResponse(response, allowedOrigin);
			}

			if (path === PURCHASE_POLL_PATH) {
				response = await handlePurchasePollRequest(request, env, tenant);
				return corsifyResponse(response, allowedOrigin);
			}

			response = await handleJitsuEventRequest(request, env, tenant);
			return corsifyResponse(response, allowedOrigin);
		} catch (error) {
			return corsifyResponse(errorResponse(error), allowedOrigin);
		}
	},
};
