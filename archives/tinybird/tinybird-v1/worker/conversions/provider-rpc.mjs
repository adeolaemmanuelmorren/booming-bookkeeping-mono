export class ProviderReadError extends Error {
  constructor(message, options = {}) {
    super(message, { cause: options.cause });
    this.name = "ProviderReadError";
    this.provider = options.provider ?? null;
    this.status = options.status ?? null;
    this.retryable = options.retryable === true;
    this.retryAfter = options.retryAfter ?? null;
    this.code = options.code ?? "provider_read_failed";
  }
}

export async function readProviderRpc({
  provider,
  invoke,
  deadlineAtMs,
  clock = Date,
  maxCallMs = 20_000,
}) {
  requireFunction(invoke, "invoke");
  requireClock(clock);
  requirePositiveNumber(maxCallMs, "maxCallMs");

  const remainingMs = requireDeadline(deadlineAtMs) - clock.now();

  if (remainingMs <= 0) {
    throw deadlineError(provider);
  }

  const timeoutMs = Math.min(remainingMs, maxCallMs);
  let response;

  try {
    response = await withTimeout(invoke(), timeoutMs, provider);
  } catch (error) {
    if (error instanceof ProviderReadError) {
      throw error;
    }

    throw new ProviderReadError(`${provider} source read failed before a response`, {
      provider,
      retryable: true,
      code: "transport_error",
      cause: error,
    });
  }

  return requireProviderResponse(response, provider);
}

export function providerBodyOrThrow(response, provider, options = {}) {
  if (response.status >= 200 && response.status < 300) {
    return response.body;
  }

  if (options.allowNotFound === true && response.status === 404) {
    return null;
  }

  throw new ProviderReadError(`${provider} source returned HTTP ${response.status}`, {
    provider,
    status: response.status,
    retryable: response.status === 429 || response.status >= 500,
    retryAfter: response.retryAfter,
    code: "http_error",
  });
}

export function requireRecord(value, fieldName) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${fieldName} must be an object`);
  }

  return value;
}

export function requireArray(value, fieldName) {
  if (!Array.isArray(value)) {
    throw new TypeError(`${fieldName} must be an array`);
  }

  return value;
}

export function requireLimit(value) {
  if (!Number.isSafeInteger(value) || value < 1 || value > 100) {
    throw new TypeError("limit must be an integer from 1 through 100");
  }

  return value;
}

export function setParameter(parameters, name, value) {
  if (value === null || value === undefined || value === "") {
    return;
  }

  parameters[name] = String(value);
}

function requireProviderResponse(value, provider) {
  const response = requireRecord(value, `${provider} RPC response`);

  if (!Number.isSafeInteger(response.status)) {
    throw new TypeError(`${provider} RPC response.status must be an integer`);
  }

  return {
    status: response.status,
    retryAfter: response.retryAfter ?? null,
    body: response.body,
  };
}

async function withTimeout(value, timeoutMs, provider) {
  let timer = null;
  const timeout = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(deadlineError(provider)), timeoutMs);
  });

  try {
    return await Promise.race([Promise.resolve(value), timeout]);
  } finally {
    clearTimeout(timer);
  }
}

function deadlineError(provider) {
  return new ProviderReadError(`${provider} source deadline reached`, {
    provider,
    retryable: true,
    code: "deadline_exceeded",
  });
}

function requireDeadline(value) {
  if (!Number.isFinite(value)) {
    throw new TypeError("deadlineAtMs must be a finite number");
  }

  return value;
}

function requirePositiveNumber(value, fieldName) {
  if (!Number.isFinite(value) || value <= 0) {
    throw new TypeError(`${fieldName} must be greater than zero`);
  }
}

function requireFunction(value, fieldName) {
  if (typeof value !== "function") {
    throw new TypeError(`${fieldName} must be a function`);
  }
}

function requireClock(clock) {
  if (!clock || typeof clock.now !== "function") {
    throw new TypeError("clock.now must be a function");
  }
}
