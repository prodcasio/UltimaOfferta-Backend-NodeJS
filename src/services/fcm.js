import fetch from 'node-fetch';
import { universalLog } from '../logger.js';

const fcmUrl = projectId => `https://fcm.googleapis.com/v1/projects/${projectId}/messages:send`;

export async function sendNotificationToToken(projectId, fcmToken, title, body, data, token) {
  const badgeCount = Number.isFinite(Number(data?.badge_count)) ? Math.max(0, Number(data.badge_count)) : 1;
  const payload = {
    message: {
      token: fcmToken,
      notification: { title, body },
      data: sanitizeData(data),
      android: {
        priority: 'HIGH',
        notification: {
          click_action: 'FLUTTER_NOTIFICATION_CLICK',
          notification_count: badgeCount
        }
      },
      apns: {
        headers: { 'apns-priority': '10' },
        payload: { aps: { badge: badgeCount, sound: 'default' } }
      }
    }
  };

  const res = await fetch(fcmUrl(projectId), {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
  });

  const text = await res.text();
  const ok = res.ok;
  if (!ok) {
    universalLog('error', 'fcm_token_send_failed', { http: res.status, resp: text });
  }
  return { success: ok, http: res.status, response: safeJson(text) };
}

export async function sendNotificationToTopic(projectId, topic, title, body, data, token, img = null) {
  const badgeCount = Number.isFinite(Number(data?.badge_count)) ? Math.max(0, Number(data.badge_count)) : 1;
  const notification = { title, body };
  if (img) notification.image = img;
  const payload = {
    message: {
      topic,
      notification,
      data: sanitizeData(data),
      android: {
        priority: 'HIGH',
        notification: {
          click_action: 'FLUTTER_NOTIFICATION_CLICK',
          notification_count: badgeCount
        }
      },
      apns: {
        headers: { 'apns-priority': '10' },
        payload: { aps: { badge: badgeCount, sound: 'default' } }
      }
    }
  };

  const res = await fetch(fcmUrl(projectId), {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
  });

  const text = await res.text();
  const ok = res.ok;
  if (!ok) {
    universalLog('error', 'fcm_topic_send_failed', { http: res.status, resp: text, topic });
  }
  return { success: ok, http: res.status, response: safeJson(text) };
}

export async function sendDataMessageToToken(projectId, fcmToken, data, token) {
  const payload = {
    message: {
      token: fcmToken,
      data: sanitizeData(data),
      android: {
        priority: 'HIGH'
      },
      apns: {
        headers: { 'apns-priority': '10' },
        payload: { aps: { 'content-available': 1 } }
      }
    }
  };

  const res = await fetch(fcmUrl(projectId), {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
  });

  const text = await res.text();
  const ok = res.ok;
  if (!ok) {
    universalLog('error', 'fcm_data_send_failed', { http: res.status, resp: text });
  }
  return { success: ok, http: res.status, response: safeJson(text) };
}

function sanitizeData(data = {}) {
  const sanitized = {};
  for (const [key, value] of Object.entries(data)) {
    if (value === null || value === undefined) continue;
    sanitized[String(key)] = typeof value === 'string' ? value : JSON.stringify(value);
  }
  return sanitized;
}

function safeJson(text) {
  try { return JSON.parse(text); } catch (_) { return text; }
}
