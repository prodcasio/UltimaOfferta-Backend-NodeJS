import express from 'express';
import { config, CATEGORIES, SKIP_CHANNELS } from '../config.js';
import { universalLog } from '../logger.js';
import { assignCategoryWithGroq } from '../services/groq.js';
import { deleteAtPlusDays, determineMainCategory, extractWordsFromTitle, toBool } from '../utils.js';
import { findFavoritersByOffer, hasOfferBeenNotified } from '../services/firestore.js';
import { getAccessToken, getProjectId } from '../services/auth.js';
import { notifyUsersAboutOffer, sendSuperOfferNotification, withdrawOfferNotifications } from '../services/notifications.js';
import { purgeCache } from '../services/cache.js';
import {
  buildRowFromPost,
  getOfferByCode,
  inferColumnTypes,
  insertOrUpdateOffer,
  markOfferDeleted,
  hardDeleteOffer,
  findUsersWithMatchingKeywords
} from '../services/postgres.js';
import { sendTelegramError } from '../services/telegram.js';
import { getAppInitConfig } from '../services/appInit.js';

const router = express.Router();

function getChannelPriority(id) {
  switch (id) {
    case '-1001305107383':
      return 1;
    case '-1001207516682':
      return 2;
    default:
      return 3;
  }
}

function applyChatGptOverride(post) {
  const chatgpt = post?.chatgpt;
  if (!chatgpt || typeof chatgpt !== 'object') return post;
  const map = {
    title: 'title',
    descr: 'description',
    custom1: 'custom1',
    custom2: 'custom2',
    custom3: 'custom3',
    custom4: 'custom4',
    custom5: 'custom5',
    custom6: 'custom6',
    emojicat: 'emojicat',
    features: 'features'
  };
  const next = { ...post };
  for (const [src, dest] of Object.entries(map)) {
    if (!(src in chatgpt)) continue;
    const val = chatgpt[src];
    const shouldOverride = Array.isArray(val) ? val.length > 0 : String(val || '').trim() !== '';
    if (shouldOverride) next[dest] = val;
  }
  return next;
}

async function purgeAndLog() {
  const res = await purgeCache();
  if (!res.success) {
    universalLog('error', 'cloudflare_purge_failed', { error: res.error || res.response });
  }
}

async function withdrawNotificationsSafe(projectId, token, offerId) {
  if (!offerId) return;
  try {
    await withdrawOfferNotifications(projectId, offerId, token);
  } catch (err) {
    universalLog('error', 'withdraw_notifications_error', { offerId, error: err.message });
  }
}

router.post('/manage-offer', async (req, res) => {
  const payload = req.body;
  try {
    universalLog('debug', 'raw_body_check', { is_empty: !payload });

    // Fetch app init config
    let appInit = {};
    try {
      appInit = await getAppInitConfig();
    } catch (err) {
      universalLog('error', 'app_init_fetch_failed_manage_offer', { error: err.message });
      appInit = { accept_incoming_requests: false, send_favorites_notifications: false, send_super_offers_notifications: false };
    }

    // Check if incoming requests are accepted
    if (!appInit.accept_incoming_requests) {
      return res.status(200).json({ ok: true, action: 'skipped_incoming_disabled' });
    }

    const type = payload?.type;
    const timestampRequest = payload?.timestamp ?? null;
    let post = payload?.post ?? null;
    if (!type || !post || !post.offer_id) {
      return res.status(400).json({ ok: false, error: 'Missing fields' });
    }

    if (post.title_clean) post.title = post.title_clean;
    post = applyChatGptOverride(post);

    const isExplicit = post.is_explicit ?? false;
    const channelId = String(post.channel_id || '');
    const link = post.link || '';
    if (
      isExplicit ||
      SKIP_CHANNELS.has(channelId) ||
      link.includes('\\u0026aod\\u003d1')
    ) {
      return res.status(200).json({ ok: true, action: 'skipped' });
    }

    const colTypes = await inferColumnTypes();
    let projectId = '';
    let accessToken = '';
    try {
      projectId = await getProjectId();
      accessToken = await getAccessToken();
    } catch (err) {
      universalLog('warn', 'firebase_auth_failed', { error: err.message });
    }

    const newPriority = getChannelPriority(channelId);
    const code = post.code || '';
    if (!code) return res.status(400).json({ ok: false, error: 'Missing code' });

    const existing = await getOfferByCode(code);
    let existingPriority = 3;
    let existingDocIsActive = false;
    let docIdToUse = post.offer_id;
    if (existing) {
      docIdToUse = existing.offer_id;
      existingPriority = getChannelPriority(String(existing.channel_id || ''));
      existingDocIsActive = !toBool(existing.is_expired) && !toBool(existing.is_deleted);
    }

    const superOfferNotify = toBool(post.super_offer_notify);
    delete post.super_offer_notify;
    let superofferSent = false;

    if (type === 'POST_CREATED') {
      if ((post.is_expired ?? false) || (post.is_deleted ?? false)) {
        await withdrawNotificationsSafe(projectId, accessToken, docIdToUse);
        if (newPriority <= existingPriority) {
          const favoriters = await findFavoritersByOffer(projectId, docIdToUse, accessToken);
          const notified = await hasOfferBeenNotified(projectId, docIdToUse, accessToken);
          if (favoriters.length > 0) {
            await markOfferDeleted(docIdToUse, post.timestamp ? Number(post.timestamp) : null);
            await purgeAndLog();
            return res.status(200).json({ ok: true, action: 'ignored_deleted_because_in_favorites' });
          } else if (notified) {
            await markOfferDeleted(docIdToUse, deleteAtPlusDays());
            await purgeAndLog();
            return res.status(200).json({ ok: true, action: 'soft_deleted' });
          }
          await hardDeleteOffer(docIdToUse);
          await purgeAndLog();
          return res.status(200).json({ ok: true, action: 'deleted_via_create' });
        }
        return res.status(200).json({ ok: true, action: 'delete_ignored' });
      }

      if (superOfferNotify && !superofferSent && projectId && accessToken && appInit.send_super_offers_notifications) {
        await sendSuperOfferNotification(projectId, accessToken, post, docIdToUse);
        superofferSent = true;
      }

      if (existing && existingDocIsActive && newPriority > existingPriority) {
        return res.status(200).json({ ok: true, action: 'skipped', reason: 'lower_priority_channel' });
      }

      if (post.timestamp === -1) {
        post.timestamp = timestampRequest === -1 ? Date.now() : timestampRequest;
      }

      await assignCategoryWithGroq(post, docIdToUse);
      post.daily_offer = channelId === '-1001207516682' || channelId === '-1001305107383';
      post.main_category = determineMainCategory(post.category_original ?? null);
      if (existing) post.offer_id = existing.offer_id;
      if (Array.isArray(post.features) && post.features.length === 0) delete post.features;

      let row = buildRowFromPost(payload, post, colTypes);
      row.offer_id = docIdToUse;

      const wasPreviouslyFlagged = existing ? toBool(existing.is_expired) || toBool(existing.is_deleted) : false;
      const isNowAvailable = (post.is_expired ?? false) === false && (post.is_deleted ?? false) === false;
      if (existing && wasPreviouslyFlagged && isNowAvailable) {
        const favoriters = await findFavoritersByOffer(projectId, docIdToUse, accessToken, post.offer_id || null);
        if (favoriters.length > 0 && projectId && accessToken && appInit.send_favorites_notifications) {
          await notifyUsersAboutOffer(projectId, favoriters, post, docIdToUse, accessToken, { useHeartTitle: true, isAvailableAgain: true });
        }
      }

      await insertOrUpdateOffer(row);
      await purgeAndLog();

      if (post.title && projectId && accessToken) {
        try {
          const titleWords = extractWordsFromTitle(post.title);
          const matches = await findUsersWithMatchingKeywords({
            titleWords,
            rawTitle: post.title,
            offerId: docIdToUse,
            offerPrice: post.price_numeric ?? null,
            offerDiscount: post.perc ?? null,
            offerStore: post.store || post.store_name || null,
            offerCategory: post.category || post.main_category || null,
          });

          const keywordMatches = matches.filter(m => m.matchType === 'keyword');
          const keywordUids = keywordMatches.map(m => m.userId);
          const reasonByUid = {};
          for (const m of keywordMatches) {
            if (m.userId && m.favoriteId != null) {
              reasonByUid[m.userId] = String(m.favoriteId);
            }
          }

          if (keywordUids.length > 0 && appInit.send_favorites_notifications) {
            await notifyUsersAboutOffer(projectId, keywordUids, post, docIdToUse, accessToken, {
              useHeartTitle: true,
              reasonByUid,
            });
          }
        } catch (err) {
          universalLog('warn', 'keyword_match_notify_failed', { error: err.message, offerId: docIdToUse });
        }
      }

      return res.status(200).json({ ok: true, action: 'created', offer_id: docIdToUse });
    }

    if (type === 'POST_EDITED') {
      const isExpiredOrDeleted = (post.is_expired ?? false) || (post.is_deleted ?? false);
      if (isExpiredOrDeleted) {
        await withdrawNotificationsSafe(projectId, accessToken, docIdToUse);
        if (newPriority <= existingPriority) {
          const favoriters = await findFavoritersByOffer(projectId, docIdToUse, accessToken);
          const notified = await hasOfferBeenNotified(projectId, docIdToUse, accessToken);
          if (favoriters.length > 0) {
            await markOfferDeleted(docIdToUse, post.timestamp ? Number(post.timestamp) : null);
            await purgeAndLog();
            return res.status(200).json({ ok: true, action: 'ignored_deleted_because_in_favorites' });
          } else if (notified) {
            await markOfferDeleted(docIdToUse, deleteAtPlusDays());
            await purgeAndLog();
            return res.status(200).json({ ok: true, action: 'soft_deleted' });
          }
          await markOfferDeleted(docIdToUse, deleteAtPlusDays());
          await purgeAndLog();
          return res.status(200).json({ ok: true, action: 'soft_deleted_via_edit' });
        }
        return res.status(200).json({ ok: true, action: 'delete_ignored' });
      }

      if (superOfferNotify && !superofferSent && projectId && accessToken && appInit.send_super_offers_notifications) {
        await sendSuperOfferNotification(projectId, accessToken, post, docIdToUse);
        superofferSent = true;
      }

      if (existingDocIsActive && newPriority > existingPriority) {
        return res.status(200).json({ ok: true, action: 'skipped', reason: 'lower_priority_on_edit' });
      }

      const existingPrice = existing?.price_numeric ? Number(existing.price_numeric) : null;
      const newPrice = post.price_numeric !== undefined ? Number(post.price_numeric) : null;
      const wasPreviouslyFlagged = existing ? toBool(existing.is_expired) || toBool(existing.is_deleted) : false;
      const isNowAvailable = (post.is_expired ?? false) === false && (post.is_deleted ?? false) === false;

      const favoriters = await findFavoritersByOffer(projectId, docIdToUse, accessToken, post.offer_id || null);

      if (wasPreviouslyFlagged && isNowAvailable && favoriters.length > 0 && projectId && accessToken && appInit.send_favorites_notifications) {
        await notifyUsersAboutOffer(projectId, favoriters, post, docIdToUse, accessToken, { useHeartTitle: true, isAvailableAgain: true });
      }

      if (existingPrice !== null && newPrice !== null && newPrice < existingPrice && favoriters.length > 0 && projectId && accessToken && appInit.send_favorites_notifications) {
        await notifyUsersAboutOffer(projectId, favoriters, post, docIdToUse, accessToken, { useHeartTitle: true });
      }

      if (post.timestamp === -1) {
        post.timestamp = timestampRequest === -1 ? Date.now() : timestampRequest;
      }

      await assignCategoryWithGroq(post, docIdToUse);
      const incomingCat = post.category || post.category_original || null;
      const incomingValid = incomingCat ? CATEGORIES.some(c => c.toLowerCase() === incomingCat.toLowerCase()) : false;
      if (!incomingValid && existing?.category) {
        post.category = String(existing.category);
        post.category_original = String(existing.category);
      }

      post.daily_offer = channelId === '-1001207516682' || channelId === '-1001305107383';
      post.main_category = determineMainCategory(post.category_original ?? null);
      if (existing) post.offer_id = existing.offer_id;
      const wasSoftDeleted = existing ? toBool(existing.is_deleted) || toBool(existing.is_expired) : false;
      const nowRestored = (post.is_deleted ?? false) === false && (post.is_expired ?? false) === false;
      if (wasSoftDeleted && nowRestored) {
        post.is_deleted = false;
        post.is_expired = false;
        post.timestamp_expired = -1;
      }
      if (Array.isArray(post.features) && post.features.length === 0) delete post.features;

      const row = buildRowFromPost(payload, post, colTypes);
      row.offer_id = docIdToUse;
      await insertOrUpdateOffer(row);
      await purgeAndLog();

      return res.status(200).json({ ok: true, action: 'edited', offer_id: docIdToUse });
    }

    if (type === 'POST_DELETED') {
      await withdrawNotificationsSafe(projectId, accessToken, docIdToUse);
      if (newPriority > existingPriority) {
        return res.status(200).json({ ok: true, action: 'skipped', reason: 'lower_priority_on_delete' });
      }
      const favoriters = await findFavoritersByOffer(projectId, docIdToUse, accessToken);
      const notified = await hasOfferBeenNotified(projectId, docIdToUse, accessToken);
      if (favoriters.length > 0) {
        await markOfferDeleted(docIdToUse, post.timestamp ? Number(post.timestamp) : null);
        await purgeAndLog();
        return res.status(200).json({ ok: true, action: 'soft_deleted' });
      }
      if (notified) {
        await markOfferDeleted(docIdToUse, deleteAtPlusDays());
        await purgeAndLog();
        return res.status(200).json({ ok: true, action: 'soft_deleted' });
      }
      await hardDeleteOffer(docIdToUse);
      await purgeAndLog();
      return res.status(200).json({ ok: true, action: 'deleted' });
    }

    return res.status(400).json({ ok: false, error: 'Unknown type' });
  } catch (err) {
    universalLog('error', 'Unhandled error', { error: err.message, stack: err.stack });
    await sendTelegramError(`‼️ Errore manageOfferRequest: ${err.message}`);
    return res.status(500).json({ ok: false, error: 'Internal error' });
  }
});

export default router;
