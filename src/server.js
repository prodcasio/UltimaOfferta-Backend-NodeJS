import express from 'express';
import cors from 'cors';
import { config } from './config.js';
import manageOfferRouter from './routes/manageOffer.js';
import apiRouter from './routes/api.js';
import { universalLog } from './logger.js';

const app = express();
app.use(cors({
  origin: (origin, callback) => callback(null, origin || '*'),
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Accept', 'Authorization'],
  maxAge: 86400
}));
app.use(express.json({ limit: '2mb' }));
app.use(manageOfferRouter);
app.use(apiRouter);

app.use((req, res) => {
  res.status(404).json({ ok: false, error: 'Not found' });
});

app.use((err, req, res, next) => {
  universalLog('error', 'express_unhandled', { error: err.message, stack: err.stack });
  res.status(500).json({ ok: false, error: 'Internal error' });
});

app.listen(config.port, () => {
  universalLog('info', 'server_started', { port: config.port });
});
