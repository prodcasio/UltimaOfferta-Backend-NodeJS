import fetch from 'node-fetch';
import { getAccessToken, getProjectId } from './auth.js';
import { universalLog } from '../logger.js';

const COLLECTION = 'app_init_test';
const DOCUMENT = 'init';

function decodeValue(field) {
  if (!field || typeof field !== 'object') return null;
  if ('booleanValue' in field) return Boolean(field.booleanValue);
  if ('integerValue' in field) return Number(field.integerValue);
  if ('doubleValue' in field) return Number(field.doubleValue);
  if ('stringValue' in field) return field.stringValue;
  if ('timestampValue' in field) {
    const d = new Date(field.timestampValue);
    return Number.isNaN(d.getTime()) ? null : d.toISOString();
  }
  return null;
}

function pickDefined(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v !== null && v !== undefined) out[k] = v;
  }
  return out;
}

export async function getAppInitConfig() {
  const [projectId, accessToken] = await Promise.all([getProjectId(), getAccessToken()]);
  if (!projectId || !accessToken) throw new Error('Missing Firestore credentials');

  const url = `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/${COLLECTION}/${DOCUMENT}`;
  const resp = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Firestore request failed: ${resp.status} ${resp.statusText} - ${text}`);
  }

  const json = await resp.json();
  const fields = json.fields || {};

  const parsed = {
    accept_incoming_requests: decodeValue(fields.accept_incoming_requests),
    android_store_url: decodeValue(fields.android_store_url),
    block_button_android_url: decodeValue(fields.block_button_android_url),
    block_button_ios_url: decodeValue(fields.block_button_ios_url),
    block_button_text: decodeValue(fields.block_button_text),
    block_description: decodeValue(fields.block_description),
    block_description_size: decodeValue(fields.block_description_size),
    block_icon: decodeValue(fields.block_icon),
    block_icon_size: decodeValue(fields.block_icon_size),
    block_show_button: decodeValue(fields.block_show_button),
    block_title: decodeValue(fields.block_title),
    block_title_size: decodeValue(fields.block_title_size),
    show_contest: decodeValue(fields.show_contest),
    ios_store_url: decodeValue(fields.ios_store_url),
    min_version: decodeValue(fields.min_version),
    send_favorites_notifications: decodeValue(fields.send_favorites_notifications),
    send_super_offers_notifications: decodeValue(fields.send_super_offers_notifications),
    share_message: decodeValue(fields.share_message),
    show_block: decodeValue(fields.show_block),
    show_update_popup: decodeValue(fields.show_update_popup),
    show_update_popup_min_version: decodeValue(fields.show_update_popup_min_version),
    start_asking_reviews_from: decodeValue(fields.start_asking_reviews_from)
  };

  const cleaned = pickDefined(parsed);
  universalLog('info', 'app_init_loaded', { source: 'firestore', keys: Object.keys(cleaned) });
  return cleaned;
}
