import { readFile } from 'node:fs/promises';

const accountId = '3e36d14845959c97bd8f93f25552f2eb';

/** Use the existing local login; never include credentials or response bodies in errors. */
export async function cloudflareRequest(path, options = {}) {
  if (!path.startsWith('/')) throw new Error('Expected an account-relative Cloudflare path');
  const config = await readFile('/Users/adeola/Library/Preferences/.wrangler/config/default.toml', 'utf8');
  const token = config.match(/^oauth_token\s*=\s*"([^"]+)"/m)?.[1];
  if (!token) throw new Error('Cloudflare login is unavailable');
  const response = await fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}${path}`, {
    ...options,
    headers: { ...options.headers, Authorization: `Bearer ${token}` },
    redirect: 'manual', signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`Cloudflare request failed with HTTP ${response.status}`);
  }
  const result = await response.json();
  if (!result.success) throw new Error('Cloudflare request was not successful');
  return result.result;
}
