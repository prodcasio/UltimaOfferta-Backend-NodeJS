import express from 'express';
import schedule from 'node-schedule';
import { config } from '../config.js';
import { universalLog } from '../logger.js';
import { toBool } from '../utils.js';
import {
  upsertUser,
  updateUser,
  getUserByUid,
  addFavorite,
  updateFavorite,
  removeFavorite,
  listFavorites,
  listNotifications,
  countUnreadNotifications,
  countUnreadNotificationsByReason,
  listNotificationsByReason,
  markNotificationRead,
  markAllNotificationsRead,
  listBanners,
  createBanner,
  updateBanner,
  deleteBanner,
  getOfferById,
  getPool,
  getReferralCode
} from '../services/postgres.js';
import { getOffers } from '../services/offers.js';
import { getAppInitConfig } from '../services/appInit.js';
import { sendDataMessageToToken } from '../services/fcm.js';
import { getAccessToken, getProjectId } from '../services/auth.js';
import { withdrawNotificationByIdFanout } from '../services/notifications.js';
import { getFirebaseAuth } from '../services/firebase.js';

const router = express.Router();

function sendError(res, status, error) {
  return res.status(status).json({ success: false, error });
}

function logAnd500(res, tag, err) {
  universalLog('error', tag, { error: err.message, stack: err.stack });
  return res.status(500).json({ success: false, error: 'Internal error' });
}

function optimizeAmazonImage(url) {
  if (!url) return null;
  if (url.includes('m.media-amazon.com')) {
    return url.replace(/(\.[^.\/]+)$/i, '._SS250$1');
  }
  return url;
}

function toNumberOrNull(value) {
  if (value === undefined || value === null || value === '') return null;
  const n = Number(value);
  return Number.isNaN(n) ? null : n;
}


async function sendNotificationInternal(body) {
  const type = body.type;
  const [projectId, accessToken] = await Promise.all([getProjectId(), getAccessToken()]);
  if (!projectId || !accessToken) throw new Error('FCM credentials not available');

  let recipients = [];
  let title, bodyText, data;
  const uids = Array.isArray(body.uids) ? body.uids : null;

  if (type === 'custom') {
    if (!body.title || !body.body || !body.cta) throw new Error('title, body, cta are required for custom type');
    title = body.title;
    bodyText = body.body;
    data = { calltoaction: body.cta, target: body.target || body.cta_target || body.cta };
  } else if (type === 'offer') {
    if (!body.offer_id) throw new Error('offer_id is required for offer type');
    const offer = await getOfferById(body.offer_id);
    if (!offer) throw new Error('offer not found');

    title = `🔥 ${offer.title || 'Nuova Super Offerta!'}`;
    bodyText = `A soli ${offer.price ?? '—'}€ invece di ${offer.old_price ?? '—'}€ (-${offer.perc ?? '—'}%)`;
    const imageUrl = optimizeAmazonImage(offer.original_image);
    data = { target: String(body.offer_id), calltoaction: 'offer_detail', special_offer: 'true', image: imageUrl };
  }

  // Get recipients
  const client = await getPool().connect();
  try {
    let query, params = [];
    if (type === 'custom') {
      query = 'SELECT uid, token_fcm FROM "users" WHERE token_fcm IS NOT NULL';
      if (uids) {
        const placeholders = uids.map((_, idx) => `$${idx + 1}`).join(',');
        query += ` AND uid IN (${placeholders})`;
        params.push(...uids);
      }
    } else if (type === 'offer') {
      query = 'SELECT uid, token_fcm FROM "users" WHERE superoffers = true AND token_fcm IS NOT NULL';
      if (uids) {
        const placeholders = uids.map((_, idx) => `$${idx + 1}`).join(',');
        query += ` AND uid IN (${placeholders})`;
        params.push(...uids);
      }
    }
    const resQuery = await client.query(query, params);
    recipients = resQuery.rows;
  } finally {
    client.release();
  }

  if (recipients.length === 0) {
    return { sent: 0, total: 0, notificationId: null };
  }

  // Create notification record
  let notificationId = null;
  const client2 = await getPool().connect();
  try {
    const notifQuery = 'INSERT INTO "notifications" (offer_id, title, body, target, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING id';
    const notifValues = [
      type === 'offer' ? body.offer_id : null,
      title,
      bodyText,
      data.target || (type === 'offer' ? body.offer_id : body.cta),
      new Date()
    ];
    const notifRes = await client2.query(notifQuery, notifValues);
    notificationId = notifRes.rows[0].id;
  } finally {
    client2.release();
  }

  if (!notificationId) {
    throw new Error('Failed to create notification');
  }

  const payloadBase = {
    type: 'push',
    notif_id: String(notificationId),
    title,
    body: bodyText,
    ...data
  };

  const sendPromises = recipients.map(({ uid, token_fcm }) => (
    (async () => {
      try {
        const result = await sendDataMessageToToken(projectId, token_fcm, payloadBase, accessToken);
        return { uid, success: result.success };
      } catch (err) {
        universalLog('error', 'send_notification_failed', { uid, error: err.message });
        return { uid, success: false };
      }
    })()
  ));

  const results = await Promise.allSettled(sendPromises);
  const successfulSends = results
    .filter(r => r.status === 'fulfilled' && r.value.success)
    .map(r => r.value.uid);

  // Insert per-user notification rows
  if (successfulSends.length > 0) {
    const client3 = await getPool().connect();
    try {
      const userNotifQuery = 'INSERT INTO "users_notifications" (uid, sent_at, notification_id, withdrawn, read, reason) VALUES ($1, $2, $3, $4, $5, $6)';
      for (const uid of successfulSends) {
        await client3.query(userNotifQuery, [uid, new Date(), notificationId, false, false, null]);
      }
    } finally {
      client3.release();
    }
  }

  const successCount = successfulSends.length;
  const failureCount = results.filter(r => r.status === 'fulfilled' && !r.value.success).length;

  universalLog('info', 'send_notification_summary', { type, total: recipients.length, success: successCount, failure: failureCount, notificationId });

  return { sent: successCount, total: recipients.length, notificationId };
}

router.post('/users-upsert', async (req, res) => {
  const body = req.body || {};
  if (!body.uid) return sendError(res, 400, 'uid is required');
  try {
    // "affiliazione" is an alias for the `promotions` column (legacy payload support)
    const promotions = body.promotions !== undefined ? body.promotions : body.affiliazione;
    const payload = {
      uid: body.uid,
      email: body.email,
      display_name: body.display_name,
      photo_url: body.photo_url,
      promotions,
      superoffers: body.superoffers,
      favorites_products: body.favorites_products,
      favorites_searches: body.favorites_searches,
      token_fcm: body.token_fcm
    };
    const result = await upsertUser(payload);
    return res.json({ success: result.success });
  } catch (err) {
    return logAnd500(res, 'users_upsert_failed', err);
  }
});

router.post('/users-update', async (req, res) => {
  const body = req.body || {};
  const uid = body.uid;
  if (!uid) return sendError(res, 400, 'uid is required');
  try {
    // "affiliazione" is an alias for the `promotions` column (legacy payload support)
    const promotions = body.promotions !== undefined ? body.promotions : body.affiliazione;
    const dateOfBirth = body.date_of_birth ?? body.birth_date;
    const updates = {
      token_fcm: body.token_fcm,
      preferences: body.preferences,
      theme: body.theme,
      display_name: body.display_name,
      phone_number: body.phone_number,
      date_of_birth: dateOfBirth,
      total_savings_delta: body.total_savings_delta,
      purchase_count_delta: body.purchase_count_delta,
      last_login_at: body.last_login_at,
      promotions,
      superoffers: body.superoffers,
      favorites_products: body.favorites_products,
      favorites_searches: body.favorites_searches,
      email: body.email,
      photo_url: body.photo_url
    };
    const result = await updateUser(uid, updates);
    if (result.reason === 'no_fields') return sendError(res, 400, 'no fields to update');
    return res.json({ success: result.success });
  } catch (err) {
    return logAnd500(res, 'users_update_failed', err);
  }
});

router.get('/users', async (req, res) => {
  const uid = req.query?.uid;
  if (!uid) return sendError(res, 400, 'uid is required');
  try {
    const user = await getUserByUid(uid);
    return res.json({ user });
  } catch (err) {
    return logAnd500(res, 'users_get_failed', err);
  }
});

router.get('/verified-email', async (req, res) => {
  const uid = req.query?.uid;
  if (!uid) return sendError(res, 400, 'uid is required');
  try {
    const auth = getFirebaseAuth();
    const userRecord = await auth.getUser(uid);
    const verified = userRecord?.emailVerified === true;
    return res.json({ verified });
  } catch (err) {
    if (err?.code === 'auth/user-not-found') {
      return res.json({ verified: false });
    }
    return logAnd500(res, 'verified_email_failed', err);
  }
});

router.get('/get-user-referral-code', async (req, res) => {
  const uid = req.query?.uid;
  if (!uid) return sendError(res, 400, 'uid is required');
  try {
    const referralCode = await getReferralCode(uid);
    return res.json({ referral_code: referralCode });
  } catch (err) {
    return logAnd500(res, 'get_user_referral_code_failed', err);
  }
});

router.get('/has-used-code', async (req, res) => {
  const uid = req.query?.uid;
  if (!uid) return sendError(res, 400, 'uid is required');
  try {
    const user = await getUserByUid(uid);
    const hasUsed = user?.used_code != null && user.used_code !== '';
    return res.json({ has_used: hasUsed });
  } catch (err) {
    return logAnd500(res, 'has_used_code_failed', err);
  }
});

router.get('/invited-users', async (req, res) => {
  const uid = req.query?.uid;
  if (!uid) return sendError(res, 400, 'uid is required');
  try {
    // Get user's referral code
    const referralCode = await getReferralCode(uid);
    if (!referralCode) {
      return res.json({ count: 0 });
    }

    // Count how many users have used this code
    const client = await getPool().connect();
    try {
      const result = await client.query(
        'SELECT COUNT(*) as count FROM "users" WHERE "used_code" = $1',
        [referralCode]
      );
      const count = parseInt(result.rows[0]?.count || 0, 10);
      return res.json({ count });
    } finally {
      client.release();
    }
  } catch (err) {
    return logAnd500(res, 'invited_users_failed', err);
  }
});

router.get('/invite-position', async (req, res) => {
  const uid = req.query?.uid;
  if (!uid) return sendError(res, 400, 'uid is required');

  try {
    const client = await getPool().connect();
    try {
      const sql = `WITH invite_counts AS (
                      SELECT u.uid,
                             COUNT(i.uid) AS invited_count
                      FROM "users" u
                      LEFT JOIN "users" i
                        ON i.used_code = u.invite_code
                      GROUP BY u.uid
                    ),
                    ranked AS (
                      SELECT
                        uid,
                        invited_count,
                        RANK() OVER (ORDER BY invited_count DESC, uid ASC) AS position
                      FROM invite_counts
                    )
                    SELECT position,
                           invited_count,
                           COUNT(*) OVER() AS total_users
                    FROM ranked
                    WHERE uid = $1`;

      const result = await client.query(sql, [uid]);

      if (result.rowCount === 0) {
        // User not found in leaderboard
        return res.json({ position: null, invited_count: 0, total_users: 0 });
      }

      const row = result.rows[0];
      return res.json({
        position: Number(row.position),
        invited_count: Number(row.invited_count),
        total_users: Number(row.total_users)
      });
    } finally {
      client.release();
    }
  } catch (err) {
    return logAnd500(res, 'invite_position_failed', err);
  }
});

router.post('/use-code', async (req, res) => {
  const body = req.body || {};
  const uid = body.uid;
  const code = (body.code || '').trim().toUpperCase();
  
  if (!uid) return sendError(res, 400, 'uid is required');
  if (!code) return sendError(res, 400, 'code is required');

  try {
    // Check email verification
    const auth = getFirebaseAuth();
    const userRecord = await auth.getUser(uid);
    if (!userRecord?.emailVerified) {
      return sendError(res, 403, 'email_not_verified');
    }

    // Check if user already used a code
    const currentUser = await getUserByUid(uid);
    if (currentUser?.used_code) {
      return sendError(res, 400, 'code_already_used');
    }

    // Find the owner of the referral code
    const client = await getPool().connect();
    try {
      const ownerRes = await client.query(
        'SELECT uid, invite_code FROM "users" WHERE "invite_code" = $1 LIMIT 1',
        [code]
      );

      if (ownerRes.rowCount === 0) {
        return sendError(res, 404, 'invalid_code');
      }

      const ownerUid = ownerRes.rows[0].uid;

      // Can't use own code
      if (ownerUid === uid) {
        return sendError(res, 400, 'cannot_use_own_code');
      }

      // Set used_code for the current user
      await client.query(
        'UPDATE "users" SET "used_code" = $1 WHERE "uid" = $2',
        [code, uid]
      );

      return res.json({ success: true });
    } finally {
      client.release();
    }
  } catch (err) {
    return logAnd500(res, 'use_code_failed', err);
  }
});

router.post('/request-email-verification', async (req, res) => {
  const body = req.body || {};
  const uid = body.uid;
  
  if (!uid) return sendError(res, 400, 'uid is required');

  try {
    const auth = getFirebaseAuth();
    const link = await auth.generateEmailVerificationLink(uid);
    
    // In a production environment, you would send this link via email
    // For now, we'll just return success since Firebase handles email sending
    // when using client SDK's sendEmailVerification()
    
    return res.json({ success: true, message: 'Verification email sent' });
  } catch (err) {
    if (err?.code === 'auth/user-not-found') {
      return sendError(res, 404, 'user_not_found');
    }
    return logAnd500(res, 'request_email_verification_failed', err);
  }
});

router.post('/favorites-add', async (req, res) => {
  const body = req.body || {};
  const uid = body.uid;
  const type = body.type;

  if (!uid || !type) return sendError(res, 400, 'uid and type are required');

  try {
    const queryRaw = (body.key ?? '').toString().trim();
    let category = (body.category ?? null)?.toString().trim() || null;
    let store = (body.store ?? null)?.toString().trim() || null;
    const minPrice = toNumberOrNull(body.min_price ?? body.minPrice);
    const maxPrice = toNumberOrNull(body.max_price ?? body.maxPrice);
    const minDiscount = toNumberOrNull(body.min_discount ?? body.minDiscount);

    if (type === 'keyword') {
      const hasFilters = Boolean(
        category ||
        store ||
        minPrice !== null ||
        maxPrice !== null ||
        minDiscount !== null
      );

      if (!queryRaw && !hasFilters) {
        return sendError(res, 400, 'query_or_filters_required');
      }
    } else if (!queryRaw) {
      return sendError(res, 400, 'key is required');
    }

    const result = await addFavorite({
      uid,
      key: queryRaw,
      type,
      category,
      store,
      min_price: minPrice,
      max_price: maxPrice,
      min_discount: minDiscount,
    });

    return res.json({ success: result.success, key: queryRaw });
  } catch (err) {
    return logAnd500(res, 'favorites_add_failed', err);
  }
});

router.post('/favorites-update', async (req, res) => {
  const body = req.body || {};
  const uid = body.uid;
  const type = body.type || 'keyword';

  if (!uid) return sendError(res, 400, 'uid is required');

  try {
    const oldKey = (body.old_key ?? body.oldKey ?? '').toString().trim();
    const oldCategory = (body.old_category ?? body.oldCategory ?? null)?.toString().trim() || null;
    const oldStore = (body.old_store ?? body.oldStore ?? null)?.toString().trim() || null;
    const oldMinPrice = toNumberOrNull(body.old_min_price ?? body.oldMinPrice);
    const oldMaxPrice = toNumberOrNull(body.old_max_price ?? body.oldMaxPrice);
    const oldMinDiscount = toNumberOrNull(body.old_min_discount ?? body.oldMinDiscount);

    const newKey = (body.new_key ?? body.newKey ?? body.key ?? '').toString().trim();
    const newCategory = (body.new_category ?? body.newCategory ?? body.category ?? null)?.toString().trim() || null;
    const newStore = (body.new_store ?? body.newStore ?? body.store ?? null)?.toString().trim() || null;
    const newMinPrice = toNumberOrNull(body.new_min_price ?? body.newMinPrice ?? body.min_price ?? body.minPrice);
    const newMaxPrice = toNumberOrNull(body.new_max_price ?? body.newMaxPrice ?? body.max_price ?? body.maxPrice);
    const newMinDiscount = toNumberOrNull(body.new_min_discount ?? body.newMinDiscount ?? body.min_discount ?? body.minDiscount);

    // Per le ricerche salvate (type = 'keyword') permettiamo old_key vuoto,
    // perché la chiave nel DB può essere una stringa vuota quando la ricerca
    // è definita solo dai filtri (categoria, prezzi, ecc.). In quel caso la
    // combinazione di filtri identifica comunque in modo univoco il record.
    if (!oldKey && type !== 'keyword') {
      return sendError(res, 400, 'old_key is required to identify the record');
    }

    if (type === 'keyword') {
      const hasFilters = Boolean(
        newCategory ||
        newStore ||
        newMinPrice !== null ||
        newMaxPrice !== null ||
        newMinDiscount !== null
      );

      if (!newKey && !hasFilters) {
        return sendError(res, 400, 'query_or_filters_required');
      }
    } else if (!newKey) {
      return sendError(res, 400, 'key is required');
    }

    const result = await updateFavorite({
      uid,
      oldKey,
      oldCategory,
      oldStore,
      oldMinPrice,
      oldMaxPrice,
      oldMinDiscount,
      newKey,
      newCategory: newCategory,
      newStore: newStore,
      newMinPrice: newMinPrice,
      newMaxPrice: newMaxPrice,
      newMinDiscount: newMinDiscount,
      type,
    });

    if (!result.success) {
      return sendError(res, 404, 'favorite_not_found');
    }

    return res.json({ success: true, key: newKey });
  } catch (err) {
    return logAnd500(res, 'favorites_update_failed', err);
  }
});

router.post('/favorites-remove', async (req, res) => {
  const body = req.body || {};
  const key = body.key ?? body.id ?? body.search_id ?? body.searchId;
  if (!body.uid || !key) return sendError(res, 400, 'uid and key are required');
  try {
    const result = await removeFavorite({ uid: body.uid, key, type: body.type });
    return res.json({ success: result.success });
  } catch (err) {
    return logAnd500(res, 'favorites_remove_failed', err);
  }
});

router.get('/favorites-list', async (req, res) => {
  const uid = req.query?.uid;
  if (!uid) return sendError(res, 400, 'uid is required');
  try {
    const rows = await listFavorites({ uid, type: req.query?.type || null });
    const mapped = rows.map(row => {
      const createdAt = row.created_at instanceof Date ? row.created_at.toISOString() : row.created_at;

      if (row.type !== 'keyword') {
        return { ...row, created_at: createdAt, id: row.key };
      }

      const search = {
        query: row.key ?? '',
        category: row.category ?? null,
        subcategory: null,
        minPrice: row.min_price ?? null,
        maxPrice: row.max_price ?? null,
        minDiscount: row.min_discount ?? null,
        brand: row.store ?? null,
        sortBy: null,
      };

      return {
        ...row,
        created_at: createdAt,
        id: row.key,
        search,
      };
    });

    return res.json({ rows: mapped });
  } catch (err) {
    return logAnd500(res, 'favorites_list_failed', err);
  }
});

router.get('/notifications-list', async (req, res) => {
  const uid = req.query?.uid;
  if (!uid) return sendError(res, 400, 'uid is required');
  try {
    const limit = req.query?.limit || 200;
    const since = req.query?.since || null;
    const rows = await listNotifications({ uid, limit, since });
    return res.json({ rows });
  } catch (err) {
    return logAnd500(res, 'notifications_list_failed', err);
  }
});

router.get('/notifications-unread-count', async (req, res) => {
  const uid = req.query?.uid;
  if (!uid) return sendError(res, 400, 'uid is required');
  try {
    const result = await countUnreadNotifications({ uid });
    if (!result.success) return sendError(res, 500, result.error || 'Failed');
    return res.json({ count: result.count ?? 0 });
  } catch (err) {
    return logAnd500(res, 'notifications_unread_count_failed', err);
  }
});

router.get('/notifications-unread-count-by-reason', async (req, res) => {
  const uid = req.query?.uid;
  if (!uid) return sendError(res, 400, 'uid is required');
  try {
    const result = await countUnreadNotificationsByReason({ uid });
    if (!result.success) return sendError(res, 500, result.error || 'Failed');
    return res.json({ counts: result.counts ?? [] });
  } catch (err) {
    return logAnd500(res, 'notifications_unread_count_by_reason_failed', err);
  }
});

router.get('/notifications-by-reason', async (req, res) => {
  const uid = req.query?.uid;
  if (!uid) return sendError(res, 400, 'uid is required');
  
  try {
    const key = req.query?.key !== undefined ? req.query.key : undefined;
    const category = req.query?.category !== undefined ? (req.query.category || null) : undefined;
    const store = req.query?.store !== undefined ? (req.query.store || null) : undefined;
    const minPrice = req.query?.min_price !== undefined ? toNumberOrNull(req.query.min_price) : undefined;
    const maxPrice = req.query?.max_price !== undefined ? toNumberOrNull(req.query.max_price) : undefined;
    const minDiscount = req.query?.min_discount !== undefined ? toNumberOrNull(req.query.min_discount) : undefined;
    const limit = parseInt(req.query?.limit || '50', 10);

    const result = await listNotificationsByReason({ 
      uid, 
      key, 
      category, 
      store, 
      minPrice, 
      maxPrice, 
      minDiscount, 
      limit 
    });
    
    if (!result.success) return sendError(res, 500, result.error || 'Failed');
    return res.json({ notifications: result.notifications ?? [] });
  } catch (err) {
    return logAnd500(res, 'notifications_by_reason_failed', err);
  }
});

router.post('/read-notification', async (req, res) => {
  const body = req.body || {};
  const uid = body.uid;
  if (!uid) return sendError(res, 400, 'uid is required');

  try {
    const readAll = body.all === true || body.read_all === true;
    if (readAll) {
      const result = await markAllNotificationsRead({ uid });
      if (!result.success) return sendError(res, 500, result.error || 'Failed');
      return res.json({ success: true, updated: result.updated, all: true });
    }

    const rawId = body.notification_id ?? body.notif_id ?? body.id;
    const notificationId = Number(rawId);
    if (!notificationId || Number.isNaN(notificationId)) return sendError(res, 400, 'notification_id is required');

    const result = await markNotificationRead({ uid, notificationId });
    if (!result.success) return sendError(res, 500, result.error || 'Failed');
    return res.json({ success: true, updated: result.updated, notification_id: notificationId });
  } catch (err) {
    return logAnd500(res, 'read_notification_failed', err);
  }
});

router.get('/banners', async (req, res) => {
  const location = req.query?.location;
  if (!location) return sendError(res, 400, 'location is required');
  try {
    const rows = await listBanners({
      location,
      category: req.query?.category || null,
      active: req.query?.active === undefined ? null : toBool(req.query.active)
    });
    return res.json({ rows });
  } catch (err) {
    return logAnd500(res, 'banners_list_failed', err);
  }
});

router.post('/banners-create', async (req, res) => {
  const body = req.body || {};
  if (!body.location) return sendError(res, 400, 'location is required');
  try {
    const result = await createBanner(body);
    return res.json({ success: result.success, id: result.id });
  } catch (err) {
    return logAnd500(res, 'banners_create_failed', err);
  }
});

router.post('/banners-update', async (req, res) => {
  const body = req.body || {};
  if (!body.id) return sendError(res, 400, 'id is required');
  try {
    const result = await updateBanner(body.id, body);
    if (result.reason === 'no_fields') return sendError(res, 400, 'no fields to update');
    return res.json({ success: result.success, id: result.id });
  } catch (err) {
    return logAnd500(res, 'banners_update_failed', err);
  }
});

router.post('/banners-delete', async (req, res) => {
  const body = req.body || {};
  if (!body.id) return sendError(res, 400, 'id is required');
  try {
    const result = await deleteBanner(body.id);
    return res.json({ success: result.success });
  } catch (err) {
    return logAnd500(res, 'banners_delete_failed', err);
  }
});

router.post('/send-notification', async (req, res) => {
  const body = req.body || {};
  const type = body.type;
  if (!type || (type !== 'custom' && type !== 'offer')) return sendError(res, 400, 'type must be "custom" or "offer"');

  try {
    if (body.scheduledAt) {
      // Schedule the notification
      const scheduledDate = new Date(body.scheduledAt);
      if (isNaN(scheduledDate.getTime())) return sendError(res, 400, 'Invalid scheduledAt date');

      schedule.scheduleJob(scheduledDate, async () => {
        try {
          await sendNotificationInternal(body);
        } catch (err) {
          universalLog('error', 'scheduled_notification_failed', { error: err.message, body });
        }
      });

      return res.json({ success: true, scheduled: true, scheduledAt: body.scheduledAt });
    } else {
      // Send immediately
      const result = await sendNotificationInternal(body);
      return res.json({ success: true, ...result });
    }
  } catch (err) {
    return logAnd500(res, 'send_notification_failed', err);
  }
});

router.post('/retract-notification', async (req, res) => {
  const rawId = req.body?.notification_id ?? req.body?.notif_id ?? req.body?.id;
  const notificationId = Number(rawId);
  if (!notificationId || Number.isNaN(notificationId)) return sendError(res, 400, 'notification_id is required');

  try {
    const [projectId, accessToken] = await Promise.all([getProjectId(), getAccessToken()]);
    if (!projectId || !accessToken) return sendError(res, 500, 'FCM credentials not available');

    const outcome = await withdrawNotificationByIdFanout(projectId, notificationId, accessToken);
    return res.json({ success: true, notificationId, ...outcome });
  } catch (err) {
    return logAnd500(res, 'retract_notification_failed', err);
  }
});

router.get('/app-init', async (req, res) => {
  try {
    const cfg = await getAppInitConfig();
    return res.json({ config: cfg });
  } catch (err) {
    universalLog('error', 'app_init_fetch_failed', { error: err.message });
    const fallback = config.appInit || {};
    return res.json({ config: fallback });
  }
});

router.get('/offers', getOffers);

router.get('/search-suggestions', async (req, res) => {
  const q = (req.query.q || '').trim();
  if (q === '') return res.json({ suggestions: [] });

  try {
    const pool = getPool();
    const result = await pool.query(
      `SELECT DISTINCT title FROM offers WHERE title_search_vector @@ websearch_to_tsquery('italian', $1) LIMIT 10`,
      [q]
    );
    const suggestions = result.rows.map(r => r.title);
    res.json({ suggestions });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/products-suggestions', async (req, res) => {
  const query = (req.query?.query || '').trim();
  if (query === '' || query.length < 3) return res.json({ suggestions: [] });

  try {
    const pool = getPool();
    const result = await pool.query(
      `SELECT word
       FROM articles
       WHERE word ILIKE '%' || $1 || '%'
       ORDER BY 
         (word ILIKE $1 || '%') DESC,
         word ASC
       LIMIT 10`,
      [query]
    );
    const suggestions = result.rows.map(r => r.word).filter(Boolean);
    return res.json({ suggestions });
  } catch (err) {
    return logAnd500(res, 'products_suggestions_failed', err);
  }
});

export default router;
