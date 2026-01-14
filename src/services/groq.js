import fetch from 'node-fetch';
import { CATEGORIES, config } from '../config.js';
import { mapResponseToCategory } from '../utils.js';
import { universalLog } from '../logger.js';

function buildCategoryPrompt(title, categories = CATEGORIES) {
  const list = categories.join(', ');
  return `Hai questa lista di categorie: [${list}].\nTitolo dell'offerta: "${title}"\n\nScegli UNA SOLA categoria dalla lista che meglio descrive questo titolo.\nRispondi SOLO e ESATTAMENTE con il nome della categoria scelta (senza spiegazioni, senza punteggiatura aggiuntiva, senza virgolette).`;
}

export async function callGroqForCategory(title, categories = CATEGORIES) {
  if (!config.groq.apiKey) throw new Error('GROQ API key not configured');
  const payload = {
    model: config.groq.model,
    messages: [{ role: 'user', content: buildCategoryPrompt(title, categories) }],
    max_tokens: 32,
    temperature: 0
  };

  const res = await fetch(config.groq.endpoint, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${config.groq.apiKey}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`GROQ HTTP ${res.status}: ${text}`);
  }
  const data = await res.json();
  const content = data?.choices?.[0]?.message?.content || data?.choices?.[0]?.text || '';
  return content.trim().replace(/\.+$/, '').replace(/["'\s]+$/g, '').trim();
}

export async function assignCategoryWithGroq(post, offerId) {
  const domain = post.domain || null;
  const isAmazon = domain && domain.toLowerCase().includes('amazon');
  const currentCategory = post.category_original || null;
  const categoryIsKnown = currentCategory && CATEGORIES.some(c => c.toLowerCase() === currentCategory.toLowerCase());

  if (isAmazon || categoryIsKnown) {
    universalLog('info', 'groq_skipped', {
      offer_id: offerId,
      domain,
      category: currentCategory,
      title: post.title || null,
      status: 'OK'
    });
    return post;
  }

  try {
    universalLog('info', 'groq_call_start', {
      offer_id: offerId,
      title: (post.title || '').slice(0, 200),
      status: 'OK'
    });

    const rawResp = await callGroqForCategory(post.title || '', CATEGORIES);
    const mapped = mapResponseToCategory(rawResp) || (CATEGORIES.includes('Uncategorized') ? 'Uncategorized' : CATEGORIES[0]);

    post.category_original = mapped;
    post.category = mapped;

    universalLog('info', 'groq_category_assigned', { offer_id: offerId, category: mapped });
  } catch (err) {
    universalLog('error', 'groq_call_failed', {
      offer_id: offerId,
      error: err.message,
      title: post.title || null,
      status: 'ERROR'
    });
    const fallback = CATEGORIES.includes('Uncategorized') ? 'Uncategorized' : currentCategory || CATEGORIES[0];
    post.category_original = fallback;
    post.category = fallback;
  }

  return post;
}
