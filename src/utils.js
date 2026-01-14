import crypto from 'crypto';
import { CATEGORIES } from './config.js';

export function toBool(value) {
  if (typeof value === 'boolean') return value;
  if (value === null || value === undefined) return false;
  if (typeof value === 'number') return value === 1;
  const normalized = String(value).trim().toLowerCase();
  return ['1', 't', 'true', 'yes', 'on'].includes(normalized);
}

export function determineMainCategory(originalCategory) {
  if (!originalCategory) return 'Altro';
  const subCategoriesMap = {
    Tecnologia: ['informatica', 'elettronica'],
    Moda: ['abbigliamento', 'moda', 'gioielli', 'valigeria', 'scarpe e borse', 'orologi'],
    Casa: ['grandi elettrodomestici', 'casa e cucina', 'illuminazione'],
    Spesa: ['alimentari e cura della casa', 'salute e cura della persona'],
    Bellezza: ['bellezza'],
    Animali: ['prodotti per animali domestici'],
    Infanzia: ['prima infanzia'],
    Libri: ['libri', 'kindle store'],
    'Tempo Libero': ['giardino e giardinaggio', 'sport e tempo libero', 'fai da te'],
    Altro: [
      'auto e moto',
      'musica digitale',
      'altro',
      'libri in altre lingue',
      'buoni regalo',
      'handmade',
      'app e giochi',
      'film e tv',
      'cd e vinili',
      'strumenti musicali e dj',
      'software',
      'videogiochi',
      'cancelleria e prodotti per ufficio',
      'industria e scienza',
      'giochi e giocattoli'
    ]
  };

  const normalized = originalCategory.toLowerCase().trim();
  for (const [main, subs] of Object.entries(subCategoriesMap)) {
    for (const sub of subs) {
      if (normalized.includes(sub)) return main;
    }
  }
  return 'Altro';
}

export function mapResponseToCategory(resp) {
  for (const cat of CATEGORIES) {
    if (resp.localeCompare(cat, undefined, { sensitivity: 'accent' }) === 0) return cat;
  }
  for (const cat of CATEGORIES) {
    if (resp.toLowerCase().includes(cat.toLowerCase())) return cat;
  }
  return null;
}

export function extractWordsFromTitle(title) {
  const clean = title.toLowerCase().replace(/[^\w\s]/g, ' ');
  const words = clean
    .split(' ')
    .map(w => w.trim())
    .filter(w => w.length >= 3);
  return Array.from(new Set(words));
}

export function generateOfferId() {
  return crypto.randomUUID();
}

export function nowMs() {
  return Math.floor(Date.now());
}

export function deleteAtPlusDays(days = 7) {
  return Date.now() + days * 24 * 60 * 60 * 1000;
}
