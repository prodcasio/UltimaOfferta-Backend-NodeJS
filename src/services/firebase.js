import admin from 'firebase-admin';
import { config } from '../config.js';

let appInitialized = false;

function ensureApp() {
  if (appInitialized) return;
  admin.initializeApp({
    credential: admin.credential.cert(config.serviceAccountPath)
  });
  appInitialized = true;
}

export function getFirebaseAuth() {
  ensureApp();
  return admin.auth();
}
