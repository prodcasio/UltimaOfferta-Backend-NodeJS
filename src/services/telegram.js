import fetch from 'node-fetch';
import { config } from '../config.js';
import { universalLog } from '../logger.js';

export async function sendTelegramError(message) {
  if (!config.telegramBotToken || !config.telegramErrorChannel) return;
  const url = `https://api.telegram.org/bot${config.telegramBotToken}/sendMessage`;
  try {
    await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: config.telegramErrorChannel, text: message })
    });
  } catch (err) {
    universalLog('error', 'telegram_notify_failed', { error: err.message });
  }
}
