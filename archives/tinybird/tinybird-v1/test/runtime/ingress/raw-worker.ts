import { BrowserIngress } from '../../../worker/browser/ingress.ts';
import { RawTouchpointReplay, type RawReplayEnv } from '../../../worker/browser/raw-replay.ts';
import { DurableObject } from 'cloudflare:workers';

interface Env extends RawReplayEnv {
  REPLAY: DurableObjectNamespace<TestRawReplay>;
  BROWSER_IDENTITY_REPLAY: DurableObjectNamespace<TestIdentityStage>;
  FAIL_IDENTITY_STAGE?: string;
}
export { BrowserIngress };
export class TestIdentityStage extends DurableObject<Env> {
  async stage(keys: string[]) {
    if (this.env.FAIL_IDENTITY_STAGE === 'true') throw new Error('Identity staging unavailable');
    await this.ctx.storage.put('keys', keys);
  }
  async keys() { return await this.ctx.storage.get('keys') ?? []; }
}
export class TestRawReplay extends RawTouchpointReplay {
  async start() { const result = await super.start(); await this.park(); return result; }
  async alarm() { await super.alarm(); await this.park(); }
  async step() {
    this.ctx.storage.sql.exec('UPDATE raw_replay_state SET next_scan_at=0');
    this.ctx.storage.sql.exec('UPDATE raw_replay_items SET next_attempt=0');
    await this.alarm();
    return this.status();
  }
  private async park() {
    if ((await this.status()).enabled) await this.ctx.storage.setAlarm(Date.now() + 3_600_000);
  }
}
export default {
  async fetch(request: Request, env: Env) {
    const replay = env.REPLAY.getByName('boom');
    try {
      switch (new URL(request.url).pathname) {
        case '/receive': return Response.json(await env.BROWSER_INGRESS.receiveBatch(await request.json()));
        case '/start': return Response.json(await replay.start());
        case '/step': return Response.json(await replay.step());
        case '/status': return Response.json(await replay.status());
        case '/pause': return Response.json(await replay.pause());
        case '/staged': return Response.json(await env.BROWSER_IDENTITY_REPLAY.getByName('boom').keys());
        default: return new Response('Not found', { status: 404 });
      }
    } catch (error) {
      return Response.json({ error: String(error) }, { status: 500 });
    }
  },
};
