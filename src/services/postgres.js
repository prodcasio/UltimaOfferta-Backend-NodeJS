import { Pool } from 'pg';
import { config } from '../config.js';
import { universalLog } from '../logger.js';
import { generateOfferId, nowMs, toBool } from '../utils.js';

let pool;
const columnCache = new Map();

export function getPool() {
  if (!pool) {
    pool = new Pool({
      host: config.postgres.host,
      port: config.postgres.port,
      database: config.postgres.database,
      user: config.postgres.user,
      password: config.postgres.password,
      connectionTimeoutMillis: 5000,
      idleTimeoutMillis: 30000,
      allowExitOnIdle: true,
      keepAlive: true,
      keepAliveInitialDelayMillis: 0,
      ssl: false,
      options: '-c search_path=dev'
    });
    
    pool.on('connect', client => {
      client.query('SET search_path TO dev').catch(err => {
        universalLog('error', 'set_search_path_failed', { error: err.message });
      });
    });
  }
  return pool;
}

export async function getOfferByCode(code) {
  const client = await getPool().connect();
  try {
    const res = await client.query('SELECT * FROM "offers" WHERE "code" = $1 ORDER BY "timestamp" DESC LIMIT 1', [code]);
    return res.rows[0] || null;
  } finally {
    client.release();
  }
}

export async function getOfferById(id) {
  const client = await getPool().connect();
  try {
    const res = await client.query('SELECT * FROM "offers" WHERE "offer_id" = $1 LIMIT 1', [id]);
    return res.rows[0] || null;
  } finally {
    client.release();
  }
}

export async function inferColumnTypes() {
  const client = await getPool().connect();
  try {
    const res = await client.query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'offers' AND table_schema = current_schema()");
    const types = {};
    for (const row of res.rows) {
      const t = row.data_type.toLowerCase();
      if (t.includes('int')) types[row.column_name] = 'int';
      else if (t.includes('numeric') || t.includes('real') || t.includes('double') || t.includes('decimal')) types[row.column_name] = 'decimal';
      else if (t === 'boolean') types[row.column_name] = 'bool';
      else types[row.column_name] = 'string';
    }
    return types;
  } finally {
    client.release();
  }
}

export function buildRowFromPost(payload, post, colTypes) {
  const row = {};
  let offerId = String(post.offer_id || '');
  if (!offerId) offerId = generateOfferId();
  row.offer_id = offerId;
  row.timestamp = Number(post.timestamp ?? payload.timestamp ?? nowMs());

  for (const [col, t] of Object.entries(colTypes)) {
    if (col === 'offer_id' || col === 'timestamp') continue;
    if (!(col in post)) continue;
    let val = post[col];
    if (col === 'features' && Array.isArray(val)) {
      row[col] = JSON.stringify(val);
      continue;
    }
    if (Array.isArray(val) || typeof val === 'object') continue;
    if (t === 'int') {
      if (val === true) val = 1; else if (val === false) val = 0;
      row[col] = val === null || val === '' ? null : parseInt(val, 10);
    } else if (t === 'decimal') {
      row[col] = val === null || val === '' ? null : parseFloat(val);
    } else if (t === 'bool') {
      row[col] = toBool(val);
    } else {
      row[col] = val === null ? null : String(val);
    }
  }

  return row;
}

export async function insertOrUpdateOffer(row) {
  const client = await getPool().connect();
  try {
    const existsRes = await client.query('SELECT 1 FROM "offers" WHERE "offer_id" = $1 LIMIT 1', [row.offer_id]);
    const cols = Object.keys(row);
    const placeholders = cols.map((_, idx) => `$${idx + 1}`);

    if (existsRes.rowCount === 0) {
      const quotedCols = cols.map(c => `"${c}"`).join(',');
      const sql = `INSERT INTO "offers" (${quotedCols}) VALUES (${placeholders.join(',')})`;
      await client.query(sql, cols.map(c => row[c]));
    } else {
      const assignments = cols.filter(c => c !== 'offer_id').map((c, idx) => `"${c}" = $${idx + 1}`);
      const values = cols.filter(c => c !== 'offer_id').map(c => row[c]);
      values.push(row.offer_id);
      const sql = `UPDATE "offers" SET ${assignments.join(',')} WHERE "offer_id" = $${values.length}`;
      await client.query(sql, values);
    }
  } finally {
    client.release();
  }
}

export async function markOfferDeleted(id, deleteTs = null) {
  const client = await getPool().connect();
  try {
    await client.query('UPDATE "offers" SET "is_deleted" = true, "is_expired" = true, "timestamp_expired" = $1 WHERE "offer_id" = $2', [deleteTs ?? nowMs(), id]);
  } finally {
    client.release();
  }
}

export async function hardDeleteOffer(id) {
  const client = await getPool().connect();
  try {
    await client.query('DELETE FROM "offers" WHERE "offer_id" = $1', [id]);
  } finally {
    client.release();
  }
}

async function getTableColumns(tableName) {
  if (columnCache.has(tableName)) return columnCache.get(tableName);
  const client = await getPool().connect();
  try {
    const res = await client.query(
      'SELECT column_name FROM information_schema.columns WHERE table_name = $1 AND table_schema = current_schema()',
      [tableName]
    );
    const cols = new Set(res.rows.map(r => r.column_name));
    columnCache.set(tableName, cols);
    return cols;
  } finally {
    client.release();
  }
}

function pickAllowed(data, allowed) {
  const out = {};
  for (const [k, v] of Object.entries(data || {})) {
    if (v === undefined) continue;
    if (allowed.has(k)) out[k] = v;
  }
  return out;
}

function parseMaybeDate(value) {
  if (!value) return null;
  const d = value instanceof Date ? value : new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

export async function withdrawNotificationsForOffer(offerId) {
  const client = await getPool().connect();
  try {
    const res = await client.query(
      'SELECT n.target, un.notification_id, un.uid, COALESCE(un.withdrawn, false) AS withdrawn, u.token_fcm\n       FROM notifications n\n       JOIN users_notifications un ON un.notification_id = n.id\n       LEFT JOIN "users" u ON u.uid = un.uid\n       WHERE n.offer_id = $1 AND COALESCE(un.withdrawn, false) = false',
      [offerId]
    );

    const notificationIds = Array.from(new Set(res.rows.map(r => r.notification_id).filter(id => id !== null && id !== undefined)));
    if (notificationIds.length > 0) {
      await client.query('UPDATE users_notifications SET withdrawn = true WHERE notification_id = ANY($1::int[])', [notificationIds]);
    }

    return { success: true, updated: notificationIds.length, rows: res.rows, notificationIds };
  } catch (err) {
    universalLog('error', 'withdraw_notifications_failed', { offerId, error: err.message });
    return { success: false, reason: 'query_failed', error: err.message };
  } finally {
    client.release();
  }
}

export async function withdrawNotificationById(notificationId) {
  const client = await getPool().connect();
  try {
    const res = await client.query(
      'SELECT n.target, un.notification_id, un.uid, COALESCE(un.withdrawn, false) AS withdrawn, u.token_fcm\n       FROM notifications n\n       JOIN users_notifications un ON un.notification_id = n.id\n       LEFT JOIN "users" u ON u.uid = un.uid\n       WHERE n.id = $1 AND COALESCE(un.withdrawn, false) = false',
      [notificationId]
    );

    const notificationIds = Array.from(new Set(res.rows.map(r => r.notification_id).filter(id => id !== null && id !== undefined)));
    if (notificationIds.length > 0) {
      await client.query('UPDATE users_notifications SET withdrawn = true WHERE notification_id = ANY($1::int[])', [notificationIds]);
    }

    return { success: true, updated: notificationIds.length, rows: res.rows, notificationIds };
  } catch (err) {
    universalLog('error', 'withdraw_notification_by_id_failed', { notificationId, error: err.message });
    return { success: false, reason: 'query_failed', error: err.message };
  } finally {
    client.release();
  }
}

export async function upsertUser(userData) {
  const uid = userData?.uid;
  if (!uid) throw new Error('uid is required');

  const columns = await getTableColumns('users');
  if (!columns.has('uid')) throw new Error('users table not available');
  const payload = pickAllowed(userData, columns);
  if (Object.keys(payload).length === 0) return { success: false, reason: 'no_fields' };

  const cols = Object.keys(payload);
  const placeholders = cols.map((_, idx) => `$${idx + 1}`);
  const values = cols.map(c => payload[c]);

  const updates = cols
    .filter(c => c !== 'uid')
    .map(c => `"${c}" = EXCLUDED."${c}"`);

  const sql = `INSERT INTO "users" (${cols.map(c => `"${c}"`).join(',')}) VALUES (${placeholders.join(',')}) ` +
    (updates.length > 0 ? `ON CONFLICT ("uid") DO UPDATE SET ${updates.join(',')}` : 'ON CONFLICT ("uid") DO NOTHING') +
    ' RETURNING *';

  const client = await getPool().connect();
  try {
    const res = await client.query(sql, values);
    return { success: true, row: res.rows[0] || null };
  } finally {
    client.release();
  }
}

export async function updateUser(uid, updates) {
  if (!uid) throw new Error('uid is required');
  const columns = await getTableColumns('users');
  if (!columns.has('uid')) throw new Error('users table not available');

  const sets = [];
  const values = [];
  let idx = 1;

  const directFields = [
    'token_fcm',
    'preferences',
    'theme',
    'display_name',
    'phone_number',
    'date_of_birth',
    'promotions',
    'superoffers',
    'favorites_products',
    'favorites_searches',
    'email',
    'photo_url',
    'affiliazione',
    'last_login_at',
    'invite_code',
    'used_code',
  ];
  for (const field of directFields) {
    if (!(field in updates)) continue;
    if (!columns.has(field)) continue;

    // IMPORTANT: callers sometimes build an updates object with many keys set to `undefined`.
    // In node-postgres, `undefined` will be treated like NULL, and `toBool(undefined)` becomes false.
    // Both behaviors would unintentionally overwrite existing user data.
    if (updates[field] === undefined || updates[field] === null) continue;

    let val = field === 'promotions' || field === 'superoffers' || field === 'favorites_products' || field === 'favorites_searches'
      ? toBool(updates[field])
      : field === 'last_login_at'
        ? parseMaybeDate(updates[field])
        : updates[field];

    if (val === null) continue; // Skip updating to null to avoid accidentally clearing fields
    sets.push(`"${field}" = $${idx++}`);
    values.push(val);
  }

  if (updates.total_savings_delta !== undefined && columns.has('total_savings')) {
    sets.push(`"total_savings" = COALESCE("total_savings", 0) + $${idx++}`);
    values.push(Number(updates.total_savings_delta));
  }

  if (updates.purchase_count_delta !== undefined && columns.has('purchase_count')) {
    sets.push(`"purchase_count" = COALESCE("purchase_count", 0) + $${idx++}`);
    values.push(Number(updates.purchase_count_delta));
  }

  if (sets.length === 0) return { success: false, reason: 'no_fields' };

  values.push(uid);
  const client = await getPool().connect();
  try {
    const res = await client.query(`UPDATE "users" SET ${sets.join(', ')} WHERE "uid" = $${idx} RETURNING *`, values);
    return { success: res.rowCount > 0, row: res.rows[0] || null };
  } finally {
    client.release();
  }
}

export async function getUserByUid(uid) {
  const client = await getPool().connect();
  try {
    const res = await client.query('SELECT * FROM "users" WHERE "uid" = $1 LIMIT 1', [uid]);
    return res.rows[0] || null;
  } finally {
    client.release();
  }
}

export async function getReferralCode(uid) {
  if (!uid) throw new Error('uid is required');
  const client = await getPool().connect();
  try {
    const existing = await client.query('SELECT invite_code FROM "users" WHERE "uid" = $1 LIMIT 1', [uid]);
    return existing.rows[0]?.invite_code ?? null;
  } finally {
    client.release();
  }
}

export async function addFavorite({
  uid,
  key,
  type,
  category = null,
  store = null,
  min_price = null,
  max_price = null,
  min_discount = null,
}) {
  const client = await getPool().connect();
  try {
    await client.query(
      'INSERT INTO favorites (uid, "key", "type", category, store, min_price, max_price, min_discount) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (uid, "key", category, store, min_price, max_price, min_discount) DO UPDATE SET "type" = EXCLUDED."type", created_at = now()',
      [uid, key, type, category, store, min_price, max_price, min_discount]
    );
    return { success: true };
  } finally {
    client.release();
  }
}

export async function updateFavorite({
  uid,
  oldKey,
  oldCategory = null,
  oldStore = null,
  oldMinPrice = null,
  oldMaxPrice = null,
  oldMinDiscount = null,
  newKey,
  newCategory = null,
  newStore = null,
  newMinPrice = null,
  newMaxPrice = null,
  newMinDiscount = null,
  type = null,
}) {
  const client = await getPool().connect();
  try {
    const sets = [];
    const values = [];
    let idx = 1;

    sets.push(`"key" = $${idx++}`);
    values.push(newKey);

    sets.push(`category = $${idx++}`);
    values.push(newCategory);

    sets.push(`store = $${idx++}`);
    values.push(newStore);

    sets.push(`min_price = $${idx++}`);
    values.push(newMinPrice);

    sets.push(`max_price = $${idx++}`);
    values.push(newMaxPrice);

    sets.push(`min_discount = $${idx++}`);
    values.push(newMinDiscount);

    sets.push(`created_at = now()`);

    const whereConditions = [
      `uid = $${idx++}`,
      `"key" = $${idx++}`,
      `($${idx++}::text IS NULL OR category IS NOT DISTINCT FROM $${idx - 1}::text)`,
      `($${idx++}::text IS NULL OR store IS NOT DISTINCT FROM $${idx - 1}::text)`,
      `($${idx++}::numeric IS NULL OR min_price IS NOT DISTINCT FROM $${idx - 1}::numeric)`,
      `($${idx++}::numeric IS NULL OR max_price IS NOT DISTINCT FROM $${idx - 1}::numeric)`,
      `($${idx++}::numeric IS NULL OR min_discount IS NOT DISTINCT FROM $${idx - 1}::numeric)`,
    ];
    // I placeholder usati nelle where conditions fanno riferimento una sola
    // volta a ciascun valore "old*", quindi è sufficiente passare ognuno di
    // essi una sola volta nell'array values.
    values.push(uid, oldKey, oldCategory, oldStore, oldMinPrice, oldMaxPrice, oldMinDiscount);

    if (type) {
      whereConditions.push(`"type" = $${idx++}`);
      values.push(type);
    }

    const sql = `UPDATE favorites SET ${sets.join(', ')} WHERE ${whereConditions.join(' AND ')} RETURNING *`;
    const res = await client.query(sql, values);

    return { success: res.rowCount > 0, row: res.rows[0] || null };
  } finally {
    client.release();
  }
}

export async function removeFavorite({ uid, key, type }) {
  const client = await getPool().connect();
  try {
    const res = await client.query('DELETE FROM favorites WHERE uid = $1 AND "key" = $2 AND ($3::text IS NULL OR "type" = $3)', [uid, key, type || null]);
    return { success: res.rowCount > 0 };
  } finally {
    client.release();
  }
}

export async function listFavorites({ uid, type }) {
  const client = await getPool().connect();
  try {
    const res = await client.query(
      'SELECT "key", "type", created_at, category, store, min_price, max_price, min_discount FROM favorites WHERE uid = $1 AND ($2::text IS NULL OR "type" = $2) ORDER BY created_at DESC',
      [uid, type || null]
    );
    return res.rows;
  } finally {
    client.release();
  }
}

export async function listNotifications({ uid, limit = 200, since = null }) {
  const client = await getPool().connect();
  try {
    const parsedSince = parseMaybeDate(since);
    const res = await client.query(
      'SELECT n.id, n.title, n.body, n.target, n.created_at, n.offer_id, un.sent_at, COALESCE(un.read, false) AS read, COALESCE(un.withdrawn, false) AS withdrawn\n       FROM notifications n\n       JOIN users_notifications un ON un.notification_id = n.id\n       WHERE un.uid = $1\n         AND COALESCE(un.withdrawn, false) = false\n         AND ($2::timestamptz IS NULL OR un.sent_at >= $2)\n       ORDER BY un.sent_at DESC\n       LIMIT $3',
      [uid, parsedSince, Math.min(Math.max(parseInt(limit, 10) || 200, 1), 500)]
    );
    return res.rows;
  } finally {
    client.release();
  }
}

export async function findUsersWithMatchingKeywords({
  titleWords = [],
  rawTitle = null,
  offerId,
  offerPrice = null,
  offerDiscount = null,
  offerStore = null,
  offerCategory = null,
}) {
  const client = await getPool().connect();
  try {
    // Normalize title for phrases
    const titleForPhrases = rawTitle ?? titleWords.join(' ');
    const titleNoPunct = (titleForPhrases || '')
      .replace(/[^\p{L}\p{N}\s]/gu, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    const orderedTokens = titleNoPunct ? titleNoPunct.split(' ') : [];
    const orderedTokensLow = orderedTokens.map(w => w.toLowerCase());

    // Single words (lower + TitleCase) limited for perf
    const singleWords = Array.from(new Set(titleWords.map(w => w.toLowerCase()).filter(Boolean))).slice(0, 5);

    // Bigrams & trigrams
    const bigrams = [];
    for (let i = 0; i + 1 < orderedTokens.length; i++) {
      bigrams.push({ low: `${orderedTokensLow[i]} ${orderedTokensLow[i + 1]}` });
    }
    const trigrams = [];
    for (let i = 0; i + 2 < orderedTokens.length; i++) {
      trigrams.push({ low: `${orderedTokensLow[i]} ${orderedTokensLow[i + 1]} ${orderedTokensLow[i + 2]}` });
    }

    const bigramKeys = bigrams.slice(0, 5).map(b => b.low);
    const trigramKeys = trigrams.slice(0, 3).map(t => t.low);

    const candidates = [];
    for (const w of singleWords) {
      candidates.push(w);
      if (w.length > 0) {
        candidates.push(w.charAt(0).toUpperCase() + w.slice(1));
      }
    }
    candidates.push(...bigramKeys, ...trigramKeys);

    const candidateSet = Array.from(new Set(candidates.filter(Boolean)));

    const matches = [];

    if (candidateSet.length > 0) {
      // Use lower(key) for case-insensitive match; chunk for safety
      const chunks = [];
      for (let i = 0; i < candidateSet.length; i += 20) {
        chunks.push(candidateSet.slice(i, i + 20));
      }

      for (const chunk of chunks) {
        const res = await client.query(
          'SELECT id, uid, key, category, store, min_price, max_price, min_discount FROM favorites WHERE type = $1 AND lower(key) = ANY($2)',
          ['keyword', chunk.map(c => c.toLowerCase())]
        );
        for (const row of res.rows) {
          // Filter by offer metadata
          let isMatch = true;

          // Check category match
          if (row.category && offerCategory) {
            const favCatLower = String(row.category).toLowerCase().trim();
            const offerCatLower = String(offerCategory).toLowerCase().trim();
            if (favCatLower !== offerCatLower) {
              isMatch = false;
            }
          }

          // Check store match
          if (isMatch && row.store && offerStore) {
            const favStoreLower = String(row.store).toLowerCase().trim();
            const offerStoreLower = String(offerStore).toLowerCase().trim();
            if (favStoreLower !== offerStoreLower) {
              isMatch = false;
            }
          }

          // Check min price
          if (isMatch && row.min_price !== null && offerPrice !== null) {
            if (offerPrice < row.min_price) {
              isMatch = false;
            }
          }

          // Check max price
          if (isMatch && row.max_price !== null && offerPrice !== null) {
            if (offerPrice > row.max_price) {
              isMatch = false;
            }
          }

          // Check min discount
          if (isMatch && row.min_discount !== null && offerDiscount !== null) {
            if (offerDiscount < row.min_discount) {
              isMatch = false;
            }
          }

          if (isMatch) {
            matches.push({
              userId: row.uid,
              matchType: 'keyword',
              keyword: row.key,
              favoriteId: row.id,
            });
          }
        }
      }
    }

    // Product favorites
    if (offerId) {
      const res = await client.query(
        'SELECT id, uid FROM favorites WHERE type = $1 AND key = $2',
        ['offer', String(offerId)]
      );
      for (const row of res.rows) {
        matches.push({
          userId: row.uid,
          matchType: 'product',
          offerId,
          favoriteId: row.id,
        });
      }
    }

    // Deduplicate by user + match info
    const seen = new Set();
    const unique = [];
    for (const m of matches) {
      const key = `${m.userId}|${m.matchType}|${m.keyword || m.offerId || ''}`;
      if (seen.has(key)) continue;
      seen.add(key);
      unique.push(m);
    }
    return unique;
  } finally {
    client.release();
  }
}

export async function markNotificationRead({ uid, notificationId }) {
  const client = await getPool().connect();
  try {
    const res = await client.query(
      'UPDATE users_notifications\n       SET read = true\n       WHERE uid = $1 AND notification_id = $2 AND COALESCE(withdrawn, false) = false AND COALESCE(read, false) = false',
      [uid, Number(notificationId)]
    );
    return { success: true, updated: res.rowCount || 0 };
  } catch (err) {
    universalLog('error', 'mark_notification_read_failed', { uid, notificationId, error: err.message });
    return { success: false, error: err.message };
  } finally {
    client.release();
  }
}

export async function markAllNotificationsRead({ uid }) {
  const client = await getPool().connect();
  try {
    const res = await client.query(
      'UPDATE users_notifications\n       SET read = true\n       WHERE uid = $1 AND COALESCE(withdrawn, false) = false AND COALESCE(read, false) = false',
      [uid]
    );
    return { success: true, updated: res.rowCount || 0 };
  } catch (err) {
    universalLog('error', 'mark_all_notifications_read_failed', { uid, error: err.message });
    return { success: false, error: err.message };
  } finally {
    client.release();
  }
}

export async function countUnreadNotifications({ uid }) {
  const client = await getPool().connect();
  try {
    const res = await client.query(
      'SELECT COUNT(*) AS cnt FROM users_notifications WHERE uid = $1 AND COALESCE(withdrawn, false) = false AND COALESCE(read, false) = false',
      [uid]
    );
    const raw = res.rows[0]?.cnt ?? 0;
    const count = typeof raw === 'string' ? parseInt(raw, 10) : Number(raw) || 0;
    return { success: true, count };
  } catch (err) {
    universalLog('error', 'count_unread_notifications_failed', { uid, error: err.message });
    return { success: false, error: err.message };
  } finally {
    client.release();
  }
}

export async function countUnreadNotificationsByReason({ uid }) {
  const client = await getPool().connect();
  try {
    const res = await client.query(
      `SELECT 
        reason_key,
        reason_category,
        reason_store,
        reason_min_price,
        reason_max_price,
        reason_min_discount,
        COUNT(*) AS count
      FROM users_notifications 
      WHERE uid = $1 
        AND COALESCE(withdrawn, false) = false 
        AND COALESCE(read, false) = false
        AND reason_key IS NOT NULL
      GROUP BY reason_key, reason_category, reason_store, reason_min_price, reason_max_price, reason_min_discount`,
      [uid]
    );
    
    const counts = res.rows.map(row => ({
      key: row.reason_key,
      category: row.reason_category,
      store: row.reason_store,
      minPrice: row.reason_min_price,
      maxPrice: row.reason_max_price,
      minDiscount: row.reason_min_discount,
      count: typeof row.count === 'string' ? parseInt(row.count, 10) : Number(row.count) || 0
    }));
    
    return { success: true, counts };
  } catch (err) {
    universalLog('error', 'count_unread_notifications_by_reason_failed', { uid, error: err.message });
    return { success: false, error: err.message };
  } finally {
    client.release();
  }
}

export async function listNotificationsByReason({ uid, key, category, store, minPrice, maxPrice, minDiscount, limit = 50 }) {
  const client = await getPool().connect();
  try {
    const where = ['un.uid = $1', 'COALESCE(un.withdrawn, false) = false', 'COALESCE(un.read, false) = false'];
    const values = [uid];
    let idx = 2;

    // Match on reason fields
    if (key !== undefined) {
      where.push(`un.reason_key = $${idx++}`);
      values.push(key);
    }
    if (category !== undefined) {
      where.push(`un.reason_category ${category === null ? 'IS NULL' : `= $${idx++}`}`);
      if (category !== null) values.push(category);
    }
    if (store !== undefined) {
      where.push(`un.reason_store ${store === null ? 'IS NULL' : `= $${idx++}`}`);
      if (store !== null) values.push(store);
    }
    if (minPrice !== undefined) {
      where.push(`un.reason_min_price ${minPrice === null ? 'IS NULL' : `= $${idx++}`}`);
      if (minPrice !== null) values.push(minPrice);
    }
    if (maxPrice !== undefined) {
      where.push(`un.reason_max_price ${maxPrice === null ? 'IS NULL' : `= $${idx++}`}`);
      if (maxPrice !== null) values.push(maxPrice);
    }
    if (minDiscount !== undefined) {
      where.push(`un.reason_min_discount ${minDiscount === null ? 'IS NULL' : `= $${idx++}`}`);
      if (minDiscount !== null) values.push(minDiscount);
    }

    const res = await client.query(
      `SELECT 
        un.notification_id,
        un.sent_at,
        un.read,
        un.withdrawn,
        n.offer_id,
        n.title,
        n.body,
        n.target,
        n.created_at
      FROM users_notifications un
      JOIN notifications n ON un.notification_id = n.id
      WHERE ${where.join(' AND ')}
      ORDER BY un.sent_at DESC
      LIMIT $${idx}`,
      [...values, limit]
    );

    return { success: true, notifications: res.rows };
  } catch (err) {
    universalLog('error', 'list_notifications_by_reason_failed', { uid, error: err.message });
    return { success: false, error: err.message };
  } finally {
    client.release();
  }
}

export async function listBanners({ location, category = null, active = null }) {
  const columns = await getTableColumns('banners');
  if (!columns.has('location')) throw new Error('banners table not available');

  const selectCols = ['id', 'location', 'image_url', 'link_url', 'cta_ios', 'cta_android', 'valid_from', 'valid_to', 'active', 'category'];
  if (columns.has('order')) selectCols.push('order');

  const where = ['"location" = $1'];
  const values = [location];
  let idx = 2;

  if (category) {
    where.push(`"category" = $${idx++}`);
    values.push(category);
  }
  if (active !== null && active !== undefined && columns.has('active')) {
    where.push(`"active" = $${idx++}`);
    values.push(toBool(active));
  }

  const orderBy = columns.has('order')
    ? 'ORDER BY "order" ASC NULLS LAST, id DESC'
    : 'ORDER BY valid_from DESC NULLS LAST, id DESC';

  const client = await getPool().connect();
  try {
    const res = await client.query(
      `SELECT ${selectCols.map(c => `"${c}"`).join(', ')} FROM banners WHERE ${where.join(' AND ')} ${orderBy}`
      , values
    );
    return res.rows;
  } finally {
    client.release();
  }
}

export async function createBanner(data) {
  const columns = await getTableColumns('banners');
  if (!columns.has('location')) throw new Error('banners table not available');

  const payload = pickAllowed(data, columns);
  if (!payload.location) throw new Error('location is required');

  if ('active' in payload) payload.active = toBool(payload.active);
  if (payload.valid_from) payload.valid_from = parseMaybeDate(payload.valid_from);
  if (payload.valid_to) payload.valid_to = parseMaybeDate(payload.valid_to);

  const cols = Object.keys(payload);
  const placeholders = cols.map((_, idx) => `$${idx + 1}`);
  const values = cols.map(c => payload[c]);

  const client = await getPool().connect();
  try {
    const res = await client.query(
      `INSERT INTO banners (${cols.map(c => `"${c}"`).join(',')}) VALUES (${placeholders.join(',')}) RETURNING id`,
      values
    );
    return { success: true, id: res.rows[0]?.id ?? null };
  } finally {
    client.release();
  }
}

export async function updateBanner(id, data) {
  const columns = await getTableColumns('banners');
  if (!columns.has('id')) throw new Error('banners table not available');

  const payload = pickAllowed(data, columns);
  delete payload.id;
  if ('active' in payload) payload.active = toBool(payload.active);
  if (payload.valid_from) payload.valid_from = parseMaybeDate(payload.valid_from);
  if (payload.valid_to) payload.valid_to = parseMaybeDate(payload.valid_to);

  const cols = Object.keys(payload);
  if (cols.length === 0) return { success: false, reason: 'no_fields' };

  const assignments = cols.map((c, idx) => `"${c}" = $${idx + 1}`);
  const values = cols.map(c => payload[c]);
  values.push(id);

  const client = await getPool().connect();
  try {
    const res = await client.query(`UPDATE banners SET ${assignments.join(', ')} WHERE id = $${values.length} RETURNING id`, values);
    return { success: res.rowCount > 0, id: res.rows[0]?.id ?? id };
  } finally {
    client.release();
  }
}

export async function deleteBanner(id) {
  const client = await getPool().connect();
  try {
    const res = await client.query('DELETE FROM banners WHERE id = $1', [id]);
    return { success: res.rowCount > 0 };
  } finally {
    client.release();
  }
}
