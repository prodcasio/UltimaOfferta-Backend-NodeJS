import fetch from 'node-fetch';
import { config } from '../config.js';
import { universalLog } from '../logger.js';

export async function purgeCache() {
  if (!config.cloudflare.zoneId || !config.cloudflare.apiToken) {
    universalLog('warn', 'cloudflare_missing_config', {});
    return { success: false, error: 'missing config' };
  }
  const url = `https://api.cloudflare.com/client/v4/zones/${config.cloudflare.zoneId}/purge_cache`;
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${config.cloudflare.apiToken}`
    },
    body: JSON.stringify({ purge_everything: true })
  });
  const text = await res.text();
  const ok = res.ok;
  if (!ok) {
    universalLog('error', 'cloudflare_purge_failed', { status: res.status, resp: text });
  }
  return { success: ok, response: text };
}
