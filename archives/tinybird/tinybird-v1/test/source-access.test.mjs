import assert from 'node:assert/strict';
import test from 'node:test';
import { checkSourceAccess } from '../worker/source-access.ts';

test('source access check follows the private gateway contract and returns only connection metadata', async () => {
  const result = await checkSourceAccess({
    STRIPE_SOURCE: { async read(account, path, parameters) {
      assert.ok(['/account', '/charges'].includes(path));
      if (path === '/charges') assert.deepEqual(parameters, { limit: '1' });
      return { status: 200, retryAfter: null, body: path === '/account' ? { id: account } : { data: [{ private: 'charge data' }] } };
    } },
    ACTIVECAMPAIGN_SOURCE: { async read(path, parameters) {
      assert.equal(path, '/contacts');
      assert.deepEqual(parameters, { limit: '1' });
      return { status: 200, retryAfter: null, body: { contacts: [{ private: 'contact data' }], meta: { total: '123' } } };
    } },
  });
  assert.equal(result.activecampaign.contacts, 123);
  assert.ok(!JSON.stringify(result).includes('private'));
  assert.notEqual(result.stripe.account_id, result.stripe_kajabi.account_id);
});
