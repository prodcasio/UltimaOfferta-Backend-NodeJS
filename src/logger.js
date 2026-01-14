import fs from 'fs';
import path from 'path';
import pino from 'pino';
import { config } from './config.js';

fs.mkdirSync(config.logDir, { recursive: true });
 
const logFile = path.join(config.logDir, 'manage-offer.log');
const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  base: null,
  timestamp: () => `,"ts":"${new Date().toISOString()}"`
}, pino.destination({ dest: logFile, sync: false }));

export function universalLog(level, message, context) {
  const payload = context ? { message, ...context } : { message };
  logger[level] ? logger[level](payload) : logger.info(payload);
}

export function writeLogAppend(basename, line) {
  const candidates = [config.logDir, process.cwd(), fs.mkdtempSync(path.join(process.cwd(), 'tmp-'))];
  for (const dir of candidates) {
    try {
      fs.mkdirSync(dir, { recursive: true });
      const filePath = path.join(dir, basename);
      fs.appendFileSync(filePath, line, { encoding: 'utf8' });
      return filePath;
    } catch (err) {
      // skip and try next
    }
  }
  return null;
}

export default logger;
