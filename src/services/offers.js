import { types } from 'pg';
import { getPool } from './postgres.js';

// --- Configuration ---

const COLUMNS = {
  offer_id: 'string', post_id: 'string', timestamp_created: 'int', timestamp_edited: 'int',
  timestamp: 'int', code: 'string', link: 'string', short_link: 'string', domain: 'string',
  channel_id: 'int', title: 'string', edited_title: 'string', price: 'string', old_price: 'string',
  price_numeric: 'float', oldprice_numeric: 'float', currency: 'string', discount_amount: 'float',
  perc: 'int', discount_percentage: 'float', disc: 'string', category: 'string', main_category: 'string',
  category_original: 'string', sub_categories: 'string', store: 'string', store_name: 'string',
  telegram_id: 'int', image: 'string', original_image: 'string', is_used: 'bool',
  is_lowest_price: 'bool', lowest_price: 'int', average_price: 'int', average90_price: 'int',
  maximum_price: 'int', is_lightning_deal: 'bool', lightning_deal_end: 'int',
  lightning_deal_requested_percentage: 'int', subscribe_and_save_percentage: 'int', graph_link: 'string',
  coupon: 'string', description: 'string', is_expired: 'bool', timestamp_expired: 'int',
  is_deleted: 'bool', post_type: 'string', is_vpc: 'bool', vpc_discount: 'int',
  vpc_text: 'string', checkout_discount: 'string', custom1: 'string', custom2: 'string',
  custom3: 'string', custom4: 'string', custom5: 'string', custom6: 'string',
  sold_by: 'string', shipped_by: 'string', reviews_total: 'int', reviews_stars: 'float',
  feedback_seller_perc: 'string', feedback_seller_count: 'int', formatted_date: 'string',
  formatted_time: 'string', message: 'string', message_no_link: 'string', api_exclusive: 'bool',
  super_offer: 'bool', super_offer_forced: 'bool', pinned: 'bool', is_explicit: 'bool', is_alcool: 'bool',
  is_event: 'bool', daily_offer: 'bool'
};

types.setTypeParser(20, (val) => (val === null ? null : parseInt(val, 10)));
types.setTypeParser(1700, (val) => (val === null ? null : parseFloat(val)));
types.setTypeParser(700, (val) => (val === null ? null : parseFloat(val)));
types.setTypeParser(701, (val) => (val === null ? null : parseFloat(val)));

// --- Helper Functions ---

function toBool(v) {
  if (typeof v === 'boolean') return v;
  if (v === null || v === undefined) return false;
  const s = String(v).trim().toLowerCase();
  return ['1', 'true', 'yes', 'on'].includes(s);
}

function goLog(level, message, context = null) {
  try {
    const now = new Date();
    const tz = 'Europe/Rome';
    const dateStr = now.toLocaleString('it-IT', { timeZone: tz, hour12: false });
    const logLine = `[${dateStr}] ${level} ${message}${context ? ' ' + JSON.stringify(context) : ''}\n`;

    const logDir = '/var/logs/backend';
    const dateFile = now.toLocaleDateString('it-IT', { timeZone: tz }).replace(/\//g, '');
    const filename = `${logDir}/getOffers_log_${dateFile}.txt`;

    // Async write, non-blocking
    console.log(logLine);
    import('fs').then(fs => {
      fs.appendFile(filename, logLine, (err) => {
        if (err) console.error('Log write failed:', err.message);
      });
    });
  } catch (e) {
    // Ignore logging errors
  }
}

const pool = getPool();

export async function getOffers(req, res) {
  const startTime = Date.now();

  try {
    // --- Input Parsing ---

    const limit = Math.max(1, Math.min(200, parseInt(req.query.limit) || 50));
    const page = req.query.page ? Math.max(1, parseInt(req.query.page)) : null;
    const offset = page ? (page - 1) * limit : Math.max(0, parseInt(req.query.offset) || 0);
    const orderBy = (req.query.orderBy && COLUMNS[req.query.orderBy]) ? req.query.orderBy : 'timestamp';
    const orderDir = req.query.orderDir?.toLowerCase() === 'asc' ? 'ASC' : 'DESC';
    const subcategory = req.query.subcategory || null;
    const includeDeleted = toBool(req.query.includeDeleted);
    const includeExpired = toBool(req.query.includeExpired);
    const q = (req.query.q || '').trim();
    const strictSearch = toBool(req.query.strictSearch);
    const debug = toBool(req.query.debug);

    // Parse filters
    let filters = req.query.filter || {};
    let filtersLike = req.query.filter_like || {};
    let filtersIn = req.query.whereIn || {};

    // Support JSON string for whereIn
    if (typeof filtersIn === 'string') {
      try {
        filtersIn = JSON.parse(filtersIn);
      } catch (e) {
      }
    }

    const whereIn = filtersIn; // For response metadata

    let afterId = req.query.afterId || req.query.startAfter || null;
    if (afterId !== null) {
      afterId = String(afterId).trim();
      if (afterId === '') afterId = null;
    }

    // --- Query Building ---

    const where = [];
    const params = [];
    let paramIndex = 1; // PostgreSQL uses $1, $2, etc.

    if (!includeDeleted) where.push('COALESCE("is_deleted", false) = false');
    if (!includeExpired) where.push('COALESCE("is_expired", false) = false');

    // Full-text search
    if (q !== '') {
      const tokens = q.split(/\s+/)
        .map(t => t.replace(/[^a-zA-Z0-9àèéìòùÀÈÉÌÒÙ]/g, ''))
        .filter(t => t !== '');

      if (tokens.length > 0) {
        const qTs = tokens.map(t => `${t}:*`).join(' & ');

        // Build sub_categories condition for query words
        const queryWords = q.split(/\s+/)
          .map(w => w.trim().toLowerCase())
          .filter(w => w.length > 3);
        let subCatCondition = '';
        if (queryWords.length > 0) {
          const subCatConditions = queryWords.map(() => `LOWER(REPLACE("sub_categories", ' ', '')) LIKE $${paramIndex++}`);
          subCatCondition = ` OR (${subCatConditions.join(' AND ')})`;
          params.push(...queryWords.map(w => `%${w}%`));
        }

        if (strictSearch) {
          where.push(`(title_search_vector @@ to_tsquery('italian', $${paramIndex})${subCatCondition})`);
          params.push(qTs);
          paramIndex += 1;
        } else {
          where.push(`(title_search_vector @@ to_tsquery('italian', $${paramIndex}) OR ("title" ILIKE $${paramIndex + 1})${subCatCondition})`);
          params.push(qTs, q + '%');
          paramIndex += 2;
        }
      }
    }

    // Exact/range filters
    for (const [col, val] of Object.entries(filters)) {
      if (!COLUMNS[col]) continue;
      const type = COLUMNS[col];

      // Special case: filter[store]=altro
      if (col === 'store' && typeof val === 'string' && val.trim().toLowerCase() === 'altro') {
        const excluded = ['amazon', 'mediaworld', 'ebay', 'unieuro', 'aliexpress'];
        const placeholders = excluded.map(() => `$${paramIndex++}`).join(',');
        where.push(`("store" IS NULL OR LOWER("store") NOT IN (${placeholders}))`);
        params.push(...excluded);
        continue;
      }

      if (Array.isArray(val) || (typeof val === 'object' && val !== null)) {
        // Range filter
        if (val.min !== undefined && val.min !== '' && type !== 'string') {
          where.push(`"${col}" >= $${paramIndex++}`);
          params.push(val.min);
        }
        if (val.max !== undefined && val.max !== '' && type !== 'string') {
          where.push(`"${col}" <= $${paramIndex++}`);
          params.push(val.max);
        }
      } else {
        // Exact match
        where.push(`"${col}" = $${paramIndex++}`);
        if (type === 'bool') {
          params.push(toBool(val));
        } else {
          params.push(val);
        }
      }
    }

    // LIKE filters
    for (const [col, val] of Object.entries(filtersLike)) {
      if (!COLUMNS[col] || val === '') continue;
      where.push(`"${col}" LIKE $${paramIndex++}`);
      params.push(`%${val}%`);
    }

    // IN filters
    for (const [col, vals] of Object.entries(filtersIn)) {
      if (!COLUMNS[col] || !Array.isArray(vals) || vals.length === 0) continue;

      const type = COLUMNS[col];
      const placeholders = [];

      for (const val of vals) {
        placeholders.push(`$${paramIndex++}`);
        switch (type) {
          case 'bool':
            params.push(toBool(val));
            break;
          case 'int':
            params.push(parseInt(val));
            break;
          case 'float':
            params.push(parseFloat(val));
            break;
          default:
            params.push(val);
        }
      }

      where.push(`"${col}" IN (${placeholders.join(',')})`);
    }

    // afterId cursor pagination
    if (afterId !== null) {
      const anchorResult = await pool.query(
        `SELECT "${orderBy}" AS ob, "offer_id" AS oid FROM "offers" WHERE "offer_id" = $1`,
        [afterId]
      );

      if (anchorResult.rows.length > 0) {
        const anchor = anchorResult.rows[0];
        const op = orderDir === 'DESC' ? '<' : '>';
        where.push(`(("${orderBy}" ${op} $${paramIndex}) OR ("${orderBy}" = $${paramIndex} AND "offer_id" ${op} $${paramIndex + 1}))`);
        params.push(anchor.ob, anchor.oid);
        paramIndex += 2;
      }
    }

    if (subcategory !== null) {
      const words = subcategory
        .split(/\s+/)
        .map(w => w.trim().toLowerCase())
        .filter(w => w.length > 3); // Ignore words 3 chars or less as separators

      if (words.length > 0) {
        const conditions = words.map(() => `LOWER(REPLACE("sub_categories", ' ', '')) LIKE $${paramIndex++}`);
        where.push(`(${conditions.join(' AND ')})`);
        params.push(...words.map(w => `%${w}%`));
      }
    }

    const whereSql = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';
    let orderBySql = `ORDER BY "${orderBy}" ${orderDir}${orderBy !== 'offer_id' ? ', "offer_id" ' + orderDir : ''}`;

    // If category filter is applied and orderBy is default (timestamp), prioritize pinned_locally offers
    console.log('Filters:', filters);
    if (filters.main_category !== undefined && orderBy === 'timestamp') {
        orderBySql = `ORDER BY "pinned_locally" DESC, "${orderBy}" ${orderDir}${orderBy !== 'offer_id' ? ', "offer_id" ' + orderDir : ''}`;
    }

    //const sqlCount = `SELECT COUNT(*) FROM "offers" ${whereSql}`;
    const sqlData = `SELECT * FROM "offers" ${whereSql} ${orderBySql} LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`;

    // --- Execute Queries ---

    //const countResult = await pool.query(sqlCount, params);
    //const total = parseInt(countResult.rows[0].count);

    const dataParams = [...params, limit, offset];
    const dataResult = await pool.query(sqlData, dataParams);
    const rows = dataResult.rows;

    // --- Response ---
    const timestamp = new Date().toISOString();
    const response = {
      success: true,
      timestamp: timestamp,
      //total,
      limit,
      offset,
      page,
      orderBy,
      orderDir,
      filtersApplied: { includeDeleted, includeExpired, q, filters, filtersLike, whereIn, afterId },
      rows
    };

    const durationMs = Date.now() - startTime;
    console.log(`[${timestamp}] ${durationMs}ms | rows:${rows.length} | query: ${req.originalUrl}`);
    /*
    goLog('INFO', 'request', {
      total,
      returned: rows.length,
      duration_ms: durationMs,
      params: response.filtersApplied
    });
    */

    res.json(response);

  } catch (error) {
    const durationMs = Date.now() - startTime;
    goLog('ERROR', 'query_failed', { error: error.message, duration_ms: durationMs });

    const errorResponse = {
      success: false,
      error: 'Query failed',
      details: error.message
    };

    res.status(500).json(errorResponse);
  }
}
