import { universalLog } from '../logger.js';
import { getPool } from './postgres.js';

// NOTE: This module now uses PostgreSQL instead of Firestore. Signatures stay the same to
// avoid touching the rest of the codebase; projectId/token parameters are ignored.

export async function getUserPreferences(_projectId, userId, _token) {
  const client = await getPool().connect();
  try {
    const res = await client.query(
      'SELECT superoffers, favorites_searches, favorites_products FROM "users" WHERE uid = $1 LIMIT 1',
      [userId]
    );
    if (res.rowCount === 0) return null;
    const row = res.rows[0];
    const superOffers = row.superoffers === true;
    const favProducts = row.favorites_products === true;
    const favSearches = row.favorites_searches === true;
    const favoritesEnabled = favProducts || favSearches;
    const pushEnabled = superOffers || favoritesEnabled;
    return {
      notifications: {
        // notifEnabled is false ONLY when all flags are false
        push: pushEnabled,
        // No dedicated column; align with push gate
        new_offers: pushEnabled,
        super: superOffers
      },
      favorites: {
        enabled: favoritesEnabled,
        products: favProducts,
        searches: favSearches
      }
    };
  } catch (err) {
    universalLog('error', 'getUserPreferences_failed', { userId, error: err.message });
    return null;
  } finally {
    // Always release the connection
    try { client.release(); } catch (_) {}
  }
}

export async function getUserFcmToken(_projectId, userId, _token) {
  const client = await getPool().connect();
  try {
    const res = await client.query('SELECT token_fcm FROM "users" WHERE uid = $1 LIMIT 1', [userId]);
    if (res.rowCount === 0) return null;
    const token = res.rows[0].token_fcm;
    return token ? String(token).trim() : null;
  } catch (err) {
    universalLog('error', 'getUserFcmToken_failed', { userId, error: err.message });
    return null;
  } finally {
    try { client.release(); } catch (_) {}
  }
}

export async function createFirestoreNotificationDoc(_projectId, _token, offerId, userIds, type) {
  const client = await getPool().connect();
  const createdAt = new Date();
  const users = userIds && userIds.length > 0 ? userIds : [null];
  try {
    for (const uid of users) {
      await client.query(
        'INSERT INTO "notifications" (offer_id, created_at, uid, type) VALUES ($1, $2, $3, $4)',
        [String(offerId), createdAt, uid, type]
      );
    }
  } catch (err) {
    universalLog('error', 'createNotification_failed', { offerId, type, error: err.message });
  } finally {
    try { client.release(); } catch (_) {}
  }
}

export async function findFavoritersByOffer(_projectId, offerId, _token, altOfferId = null) {
  const client = await getPool().connect();
  try {
    const keys = [offerId];
    if (altOfferId && altOfferId !== offerId) keys.push(altOfferId);
    const params = keys;
    const placeholders = keys.map((_, idx) => `$${idx + 1}`).join(',');
    const res = await client.query(
      `SELECT DISTINCT uid FROM "favorites" WHERE type = 'offer' AND "key" IN (${placeholders})`,
      params
    );
    return res.rows.map(r => r.uid).filter(Boolean);
  } catch (err) {
    universalLog('error', 'findFavoritersByOffer_failed', { offerId, altOfferId, error: err.message });
    return [];
  } finally {
    try { client.release(); } catch (_) {}
  }
}

export async function hasOfferBeenNotified(_projectId, offerId, _token) {
  const client = await getPool().connect();
  try {
    const res = await client.query(
      'SELECT 1 FROM "notifications" WHERE offer_id = $1 AND created_at > (now() - interval \'7 days\') LIMIT 1',
      [offerId]
    );
    return res.rowCount > 0;
  } catch (err) {
    universalLog('error', 'hasOfferBeenNotified_failed', { offerId, error: err.message });
    return false;
  } finally {
    try { client.release(); } catch (_) {}
  }
}

export async function hasOfferNotificationOnDate(_projectId, offerId, _token, date) {
  const client = await getPool().connect();
  try {
    const start = new Date(date);
    start.setHours(0, 0, 0, 0);
    const end = new Date(date);
    end.setHours(23, 59, 59, 999);
    const res = await client.query(
      'SELECT 1 FROM "notifications" WHERE offer_id = $1 AND created_at >= $2 AND created_at <= $3 LIMIT 1',
      [offerId, start, end]
    );
    return res.rowCount > 0;
  } catch (err) {
    universalLog('error', 'hasOfferNotificationOnDate_failed', { offerId, error: err.message });
    return false;
  } finally {
    try { client.release(); } catch (_) {}
  }
}

export async function hasOfferNotificationAfter(_projectId, offerId, _token, timestamp) {
  const client = await getPool().connect();
  try {
    const res = await client.query(
      'SELECT 1 FROM "notifications" WHERE offer_id = $1 AND created_at > $2 LIMIT 1',
      [offerId, timestamp]
    );
    return res.rowCount > 0;
  } catch (err) {
    universalLog('error', 'hasOfferNotificationAfter_failed', { offerId, error: err.message });
    return false;
  } finally {
    try { client.release(); } catch (_) {}
  }
}
