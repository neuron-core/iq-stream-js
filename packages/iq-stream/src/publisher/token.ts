import type { Scope } from '../shared/protocol.js';

const encoder = new TextEncoder();

export function base64url(bytes: Uint8Array): string {
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function fromBase64url(text: string): Uint8Array<ArrayBuffer> {
  const binary = atob(text.replace(/-/g, '+').replace(/_/g, '/'));
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

/**
 * The first half of a token. Compact, in the order app_key, stream, exp, slashes unescaped: the form
 * the three languages share, so that their tokens can be compared byte for byte.
 */
export function claims(appKey: string, streamId: string, exp: number): string {
  return base64url(encoder.encode(JSON.stringify({ app_key: appKey, stream: streamId, exp })));
}

export function importSecret(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey('raw', encoder.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign', 'verify']);
}

export async function sign(key: CryptoKey, scope: Scope, body: string): Promise<string> {
  return `${body}.${base64url(new Uint8Array(await crypto.subtle.sign('HMAC', key, encoder.encode(`${scope}.${body}`))))}`;
}

/** True when the token carries this app's signature for this scope and stream and has not expired. */
export async function verify(key: CryptoKey, scope: Scope, appKey: string, streamId: string, token: string, nowSeconds: number): Promise<boolean> {
  try {
    const [body, signature, ...rest] = token.split('.');
    if (!body || !signature || rest.length > 0) return false;
    // subtle.verify compares in constant time.
    if (!(await crypto.subtle.verify('HMAC', key, fromBase64url(signature), encoder.encode(`${scope}.${body}`)))) return false;
    const parsed = JSON.parse(new TextDecoder().decode(fromBase64url(body))) as { app_key?: unknown; stream?: unknown; exp?: unknown };
    return parsed.app_key === appKey && parsed.stream === streamId && typeof parsed.exp === 'number' && parsed.exp > nowSeconds;
  } catch {
    return false;
  }
}
