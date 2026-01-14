import { GoogleAuth } from 'google-auth-library';
import { config } from '../config.js';

const auth = new GoogleAuth({
  keyFile: config.serviceAccountPath,
  scopes: [config.oauthScope]
});

export async function getAccessToken() {
  const client = await auth.getClient();
  const token = await client.getAccessToken();
  return token?.token || '';
}

export async function getProjectId() {
  return auth.getProjectId();
}
