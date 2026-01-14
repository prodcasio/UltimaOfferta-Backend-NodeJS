import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function envBool(name) {
  const v = process.env[name];
  if (v === undefined) return null;
  return ['1', 'true', 'yes', 'on', 't'].includes(String(v).trim().toLowerCase());
}

function envNumber(name) {
  const v = process.env[name];
  if (v === undefined) return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
}

export const CATEGORIES = [
  'Abbigliamento',
  'Grandi elettrodomestici',
  'Auto e Moto',
  'Prima infanzia',
  'Bellezza',
  'Libri',
  'Informatica',
  'Musica Digitale',
  'Elettronica',
  'Altro',
  'Moda',
  'Giardino e giardinaggio',
  'Buoni Regalo',
  'Alimentari e cura della casa',
  'Handmade',
  'Salute e cura della persona',
  'Casa e cucina',
  'Industria e Scienza',
  'Gioielli',
  'Kindle Store',
  'Illuminazione',
  'Valigeria',
  'App e Giochi',
  'Film e TV',
  'CD e Vinili',
  'Strumenti musicali e DJ',
  'Cancelleria e prodotti per ufficio',
  'Prodotti per animali domestici',
  'Scarpe e borse',
  'Software',
  'Sport e tempo libero',
  'Fai da te',
  'Giochi e giocattoli',
  'Videogiochi',
  'Orologi',
  'Uncategorized'
];

export const config = {
  port: parseInt(process.env.PORT || '3000', 10),
  serviceAccountPath: process.env.SERVICE_ACCOUNT_PATH || '/var/backend/service-account.json',
  oauthScope: process.env.OAUTH_SCOPE || 'https://www.googleapis.com/auth/cloud-platform',
  telegramBotToken: process.env.TELEGRAM_BOT_TOKEN || '',
  telegramErrorChannel: process.env.TELEGRAM_ERROR_CHANNEL || '',
  groq: {
    apiKey: process.env.GROQ_API_KEY || '',
    endpoint: process.env.GROQ_ENDPOINT || 'https://api.groq.com/openai/v1/chat/completions',
    model: process.env.GROQ_MODEL || 'llama-3.3-70b-versatile'
  },
  postgres: {
    host: process.env.POSTGRES_HOST || 'database',
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    database: process.env.POSTGRES_DB || '',
    user: process.env.POSTGRES_USER || '',
    password: process.env.POSTGRES_PASSWORD || ''
  },
  cloudflare: {
    zoneId: process.env.CLOUDFLARE_ZONE_ID || '',
    apiToken: process.env.CLOUDFLARE_API_TOKEN || ''
  },
  logDir: process.env.LOG_DIR || path.join(__dirname, '..', 'logs'),
};

export const SKIP_CHANNELS = new Set([
  '-1003191179900',
  '-1002335639614',
  '-1001313053355'
]);
