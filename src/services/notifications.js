import { createFirestoreNotificationDoc, getUserFcmToken, getUserPreferences } from './firestore.js';
import { sendDataMessageToToken, sendNotificationToToken } from './fcm.js';
import { universalLog } from '../logger.js';
import { getPool, withdrawNotificationById, withdrawNotificationsForOffer } from './postgres.js';

function shouldSendNotification(prefs, matchType) {
  if (!prefs?.notifications?.push || !prefs?.notifications?.new_offers) return false;
  if (!prefs?.favorites?.enabled) return false;
  return matchType === 'product' ? prefs.favorites.products : prefs.favorites.searches;
}

export async function notifyUsersAboutOffer(
  projectId,
  userIds,
  post,
  offerId,
  token,
  { useHeartTitle = false, isAvailableAgain = false, reasonByUid = {} } = {}
) {
  const titlePrefix = isAvailableAgain ? '❤️ ' : (useHeartTitle ? '❤️ ' : '🔍 ');
  const title = `${titlePrefix}${post.title || 'Offerta'}`;
  const body = `A soli ${post.price ?? '—'}€ invece di ${post.old_price ?? '—'}€ (-${post.perc ?? '—'}%)`;
  const image = optimizeAmazonImage(post.original_image);
  const data = { target: String(offerId || 'unknown'), calltoaction: 'offer_detail', image };

  // Ensure we have a local, mutable map for reasons
  let finalReasonByUid = reasonByUid && typeof reasonByUid === 'object' ? { ...reasonByUid } : {};

  // Persist notification entry in Postgres so we can link users_notifications with a reason.
  let notificationId = null;
  const clientNotif = await getPool().connect();
  try {
    const notifRes = await clientNotif.query(
      'INSERT INTO notifications (offer_id, title, body, target, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING id',
      [offerId ?? null, title, body, data.target, new Date()]
    );
    notificationId = notifRes.rows[0]?.id ?? null;
  } catch (err) {
    universalLog('warn', 'favorites_notification_insert_failed', { offerId, error: err.message });
  } finally {
    try { clientNotif.release(); } catch (_) {}
  }

  // If no explicit reasons were provided but we have an offerId, try to
  // infer reasons from favorites of type "offer" so that reason holds
  // the favorite id that triggered the notification.
  if ((!finalReasonByUid || Object.keys(finalReasonByUid).length === 0) && offerId) {
    const clientFav = await getPool().connect();
    try {
      const resFav = await clientFav.query(
        'SELECT id, uid FROM favorites WHERE type = $1 AND "key" = $2',
        ['offer', String(offerId)]
      );
      for (const row of resFav.rows) {
        if (row.uid && finalReasonByUid[row.uid] == null && row.id != null) {
          finalReasonByUid[row.uid] = String(row.id);
        }
      }
    } catch (err) {
      universalLog('warn', 'favorites_reason_autofill_failed', { offerId, error: err.message });
    } finally {
      try { clientFav.release(); } catch (_) {}
    }
  }

  const tasks = userIds.map(uid => (async () => {
    try {
      const prefs = await getUserPreferences(projectId, uid, token);
      if (!prefs || !shouldSendNotification(prefs, 'product')) return null;
      const fcmToken = await getUserFcmToken(projectId, uid, token);
      if (!fcmToken) return null;

      const res = await sendNotificationToToken(projectId, fcmToken, title, body, data, token);
      if (!res.success) {
        universalLog('warn', 'favorite_send_failed', { offerId, uid, http: res.http, response: res.response });
        return null;
      }
      return uid;
    } catch (err) {
      universalLog('error', 'notify_user_failed', { user: uid, error: err.message });
      return null;
    }
  })());

  const results = await Promise.allSettled(tasks);
  const notified = results
    .filter(r => r.status === 'fulfilled' && r.value)
    .map(r => r.value);

  if (notified.length > 0) {
    await createFirestoreNotificationDoc(projectId, token, offerId, notified, 'favorites');

    if (notificationId) {
      const client = await getPool().connect();
      try {
        for (const uid of notified) {
          const reason = finalReasonByUid[uid] ?? (offerId ? String(offerId) : null);
          await client.query(
            'INSERT INTO users_notifications (uid, sent_at, notification_id, withdrawn, read, reason) VALUES ($1, $2, $3, $4, $5, $6)',
            [uid, new Date(), notificationId, false, false, reason]
          );
        }
      } catch (err) {
        universalLog('warn', 'favorites_users_notifications_insert_failed', { offerId, error: err.message });
      } finally {
        try { client.release(); } catch (_) {}
      }
    }
  }

  return notified;
}

export async function sendSuperOfferNotification(projectId, token, post, offerId) {
  const title = `🔥 ${post.title || 'Nuova Super Offerta!'}`;
  const body = `A soli ${post.price ?? '—'}€ invece di ${post.old_price ?? '—'}€ (-${post.perc ?? '—'}%)`;
  const imageUrl = optimizeAmazonImage(post.original_image);
  const data = { target: String(offerId), calltoaction: 'offer_detail', special_offer: 'true', image: imageUrl };

  // Fetch recipients from Postgres (users.superoffers = true and token_fcm present)
  const client = await getPool().connect();
  let recipients = [];
  try {
    const res = await client.query(
      'SELECT uid, token_fcm FROM "users" WHERE superoffers = true AND token_fcm IS NOT NULL'
    );
    recipients = res.rows;
  } catch (err) {
    universalLog('error', 'superoffer_recipients_query_failed', { error: err.message });
  } finally {
    try { client.release(); } catch (_) {}
  }

  if (!recipients.length) {
    universalLog('warn', 'superoffer_no_recipients', { offerId });
    return;
  }

  const sendAll = recipients.map(({ uid, token_fcm }) => (
    (async () => {
      try {
        const res = await sendNotificationToToken(projectId, token_fcm, title, body, data, token);
        if (!res.success) {
          universalLog('warn', 'superoffer_send_failed', { offerId, uid, http: res.http, response: res.response });
        }
        return res.success ? uid : null;
      } catch (err) {
        universalLog('error', 'superoffer_send_error', { offerId, uid, error: err.message });
        return null;
      }
    })()
  ));

  const results = await Promise.allSettled(sendAll);
  const notified = results
    .filter(r => r.status === 'fulfilled' && r.value)
    .map(r => r.value);

  if (notified.length > 0) {
    await createFirestoreNotificationDoc(projectId, token, offerId, notified, 'superoffer');
    universalLog('info', 'superoffer_notification_sent_fanout', { offerId, count: notified.length });
  } else {
    universalLog('warn', 'superoffer_notification_sent_zero', { offerId, attempted: recipients.length });
  }
}

export async function withdrawOfferNotifications(projectId, offerId, token) {
  const res = await withdrawNotificationsForOffer(offerId);
  if (!res.success) {
    universalLog('warn', 'withdraw_offer_skipped', { offerId, reason: res.reason });
    return { updated: 0, sent: 0 };
  }

  const rows = res.rows || [];
  const recipients = rows
    .map(r => ({ token: r.token_fcm, notificationId: r.notification_id, target: r.target ?? offerId }))
    .filter(r => r.token);

  const payloadBase = {
    type: 'retract',
    calltoaction: 'withdraw_offer',
    withdrawn: 'true'
  };

  if (!projectId || !token) {
    universalLog('warn', 'withdraw_offer_no_fcm_creds', { offerId, tokens: recipients.length });
    return { updated: res.updated || 0, sent: 0 };
  }

  const sendAll = recipients.map(({ token: tkn, notificationId, target }) => (async () => {
    try {
      const sent = await sendDataMessageToToken(
        projectId,
        tkn,
        {
          ...payloadBase,
          target: target ? String(target) : undefined,
          notif_id: notificationId ? String(notificationId) : undefined
        },
        token
      );
      return sent.success;
    } catch (err) {
      universalLog('error', 'withdraw_offer_send_failed', { offerId, error: err.message });
      return false;
    }
  })());

  const outcomes = await Promise.allSettled(sendAll);
  const successCount = outcomes.filter(r => r.status === 'fulfilled' && r.value).length;
  const failureCount = outcomes.filter(r => r.status === 'fulfilled' && !r.value).length;
  if (failureCount > 0) {
    universalLog('warn', 'withdraw_offer_send_partial', { offerId, successCount, failureCount, tokens: tokens.length });
  }

  return { updated: res.updated || 0, sent: successCount };
}

export async function withdrawNotificationByIdFanout(projectId, notificationId, token) {
  const res = await withdrawNotificationById(notificationId);
  if (!res.success) {
    universalLog('warn', 'withdraw_notification_skipped', { notificationId, reason: res.reason });
    return { updated: 0, sent: 0 };
  }

  const rows = res.rows || [];
  const recipients = rows
    .map(r => ({ token: r.token_fcm, target: r.target, notificationId: r.notification_id }))
    .filter(r => r.token);

  if (!projectId || !token) {
    universalLog('warn', 'withdraw_notification_no_fcm_creds', { notificationId, tokens: recipients.length });
    return { updated: res.updated || 0, sent: 0 };
  }

  const payloadBase = { type: 'retract', withdrawn: 'true', calltoaction: 'withdraw_offer' };

  const sendAll = recipients.map(({ token: tkn, target, notificationId: notifId }) => (async () => {
    try {
      const sent = await sendDataMessageToToken(
        projectId,
        tkn,
        {
          ...payloadBase,
          target: target ? String(target) : undefined,
          notif_id: notifId ? String(notifId) : String(notificationId)
        },
        token
      );
      return sent.success;
    } catch (err) {
      universalLog('error', 'withdraw_notification_send_failed', { notificationId, error: err.message });
      return false;
    }
  })());

  const outcomes = await Promise.allSettled(sendAll);
  const successCount = outcomes.filter(r => r.status === 'fulfilled' && r.value).length;
  const failureCount = outcomes.filter(r => r.status === 'fulfilled' && !r.value).length;
  if (failureCount > 0) {
    universalLog('warn', 'withdraw_notification_send_partial', { notificationId, successCount, failureCount, tokens: recipients.length });
  }

  return { updated: res.updated || 0, sent: successCount };
}

function optimizeAmazonImage(url) {
  if (!url) return null;
  if (url.includes('m.media-amazon.com')) {
    return url.replace(/(\.[^.\/]+)$/i, '._SS250$1');
  }
  return url;
}
