import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import { isIP } from 'node:net';

const targetUrl = process.env.WEBEX_SIDECAR_TARGET_URL ?? 'http://127.0.0.1:8787/webex/events';
const target = validateLoopbackUrl(
  targetUrl,
  'WEBEX_SIDECAR_TARGET_URL',
  process.env.WEBEX_SIDECAR_ALLOW_NON_LOOPBACK === '1'
);
const forwardToken = process.env.WEBEX_SIDECAR_TOKEN;
const allowUnauthenticatedForward = process.env.WEBEX_SIDECAR_ALLOW_UNAUTHENTICATED === '1';
const forwardTimeoutMs = parsePositiveInteger(process.env.WEBEX_SIDECAR_FORWARD_TIMEOUT_MS, 10000);
const maxInFlightForwards = parsePositiveInteger(process.env.WEBEX_SIDECAR_MAX_IN_FLIGHT, 8);
const forwardRetries = parseNonNegativeInteger(process.env.WEBEX_SIDECAR_FORWARD_RETRIES, 3);
const retryBaseMs = parsePositiveInteger(process.env.WEBEX_SIDECAR_RETRY_BASE_MS, 500);
const retryMaxMs = parsePositiveInteger(process.env.WEBEX_SIDECAR_RETRY_MAX_MS, 5000);
const tokenReloadIntervalMs = parseNonNegativeInteger(
  process.env.WEBEX_SIDECAR_TOKEN_RELOAD_INTERVAL_MS,
  60000
);
const healthBind = process.env.WEBEX_SIDECAR_HEALTH_BIND;
const validateConfig = process.env.WEBEX_SIDECAR_VALIDATE_CONFIG === '1';
const healthPath = process.env.WEBEX_SIDECAR_HEALTH_PATH ?? '/healthz';
const readyPath = process.env.WEBEX_SIDECAR_READY_PATH ?? '/readyz';
const livePath = process.env.WEBEX_SIDECAR_LIVE_PATH ?? '/livez';
const messageEvents = parseMessageEvents(process.env.WEBEX_SIDECAR_MESSAGE_EVENTS ?? 'created,deleted');

let webexModules;
let inFlightForwards = 0;
let shuttingDown = false;
let activeListener = null;
let healthServer = null;
let reloadTimer = null;
let reloadInProgress = false;

const status = {
  startedAt: new Date().toISOString(),
  listening: false,
  target: target.href,
  messageEvents,
  tokenSource: null,
  tokenFingerprint: null,
  tokenExpiresAt: null,
  forwardCount: 0,
  inFlightForwards: 0,
  lastForwardAt: null,
  lastForwardError: null,
  lastReloadAt: null,
  lastReloadError: null,
};

class ForwardHttpError extends Error {
  constructor(statusCode, body) {
    super(`forward failed status=${statusCode} body=${body}`);
    this.name = 'ForwardHttpError';
    this.statusCode = statusCode;
  }
}

function messagePayload(event) {
  return event && typeof event === 'object' && event.data && typeof event.data === 'object'
    ? event.data
    : event;
}

function parsePositiveInteger(value, fallback) {
  if (!value) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseNonNegativeInteger(value, fallback) {
  if (!value) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

function parseMessageEvents(value) {
  const events = value
    .split(',')
    .map((event) => event.trim())
    .filter(Boolean);
  if (events.length === 0) {
    throw new Error('WEBEX_SIDECAR_MESSAGE_EVENTS must contain at least one event');
  }
  return events;
}

function isLoopbackHost(hostname) {
  const host = hostname.toLowerCase().replace(/^\[(.*)\]$/, '$1');
  if (host === 'localhost') {
    return true;
  }
  if (isIP(host) === 4) {
    return host.split('.')[0] === '127';
  }
  return isIP(host) === 6 && host === '::1';
}

function validateLoopbackUrl(value, name, allowNonLoopback) {
  const parsed = new URL(value);
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new Error(`${name} must use http or https`);
  }
  if (!allowNonLoopback && !isLoopbackHost(parsed.hostname)) {
    throw new Error(
      `${name} must use a loopback host; set WEBEX_SIDECAR_ALLOW_NON_LOOPBACK=1 only for an explicitly secured deployment`
    );
  }
  if (allowNonLoopback) {
    console.warn('sidecar_target_non_loopback_allowed=true');
  }
  return parsed;
}

function parseHealthBind(value) {
  const parsed = new URL(value.includes('://') ? value : `http://${value}`);
  const host = parsed.hostname.replace(/^\[(.*)\]$/, '$1');
  if (!host || !parsed.port) {
    throw new Error('WEBEX_SIDECAR_HEALTH_BIND must be host:port');
  }
  const port = Number.parseInt(parsed.port, 10);
  if (!Number.isFinite(port) || port <= 0 || port > 65535) {
    throw new Error('WEBEX_SIDECAR_HEALTH_BIND has an invalid port');
  }
  const allowNonLoopback = process.env.WEBEX_SIDECAR_HEALTH_ALLOW_NON_LOOPBACK === '1';
  if (!allowNonLoopback && !isLoopbackHost(host)) {
    throw new Error(
      'WEBEX_SIDECAR_HEALTH_BIND must use a loopback host; set WEBEX_SIDECAR_HEALTH_ALLOW_NON_LOOPBACK=1 only for an explicitly secured deployment'
    );
  }
  if (allowNonLoopback) {
    console.warn('sidecar_health_non_loopback_allowed=true');
  }
  return { host, port };
}

function accessTokenConfigFromEnv() {
  const file = process.env.WEBEX_ACCESS_TOKEN_FILE ?? process.env.WEBEX_TOKEN_FILE;
  const token = process.env.WEBEX_ACCESS_TOKEN;
  if (file) {
    if (token) {
      console.warn('sidecar_token_file_overrides_access_token=true');
    }
    return { kind: 'file', path: file, source: `file:${file}` };
  }
  if (token && token.trim()) {
    return { kind: 'env', token: token.trim(), source: 'env:WEBEX_ACCESS_TOKEN' };
  }
  throw new Error(
    'WEBEX_TOKEN_FILE, WEBEX_ACCESS_TOKEN_FILE, or WEBEX_ACCESS_TOKEN is required unless WEBEX_SIDECAR_MOCK_EVENT=1'
  );
}

function validateForwardingAuth() {
  if (forwardToken) {
    return;
  }
  if (allowUnauthenticatedForward) {
    console.warn('sidecar_forward_unauthenticated=true');
    return;
  }
  throw new Error(
    'WEBEX_SIDECAR_TOKEN is required; set WEBEX_SIDECAR_ALLOW_UNAUTHENTICATED=1 only for local unsafe testing'
  );
}

async function loadAccessToken(config) {
  if (config.kind === 'env') {
    return loadedAccessToken(config.token, config.source, null);
  }

  const raw = (await readFile(config.path, 'utf8')).trim();
  if (!raw) {
    throw new Error(`${config.source} is empty`);
  }

  if (!raw.startsWith('{')) {
    return loadedAccessToken(raw, config.source, null);
  }

  const parsed = JSON.parse(raw);
  const token = parsed.accessToken ?? parsed.access_token;
  if (!token || typeof token !== 'string' || !token.trim()) {
    throw new Error(`${config.source} does not contain accessToken`);
  }
  const expiresAt = parsed.expiresAt ?? parsed.expires_at ?? null;
  return loadedAccessToken(token.trim(), config.source, expiresAt);
}

function loadedAccessToken(accessToken, source, expiresAt) {
  return {
    accessToken,
    source,
    expiresAt,
    fingerprint: createHash('sha256').update(accessToken).digest('hex').slice(0, 12),
  };
}

async function forward(resource, event, data) {
  if (inFlightForwards >= maxInFlightForwards) {
    throw new Error(`too many in-flight forwards limit=${maxInFlightForwards}`);
  }
  inFlightForwards += 1;
  status.inFlightForwards = inFlightForwards;
  try {
    for (let attempt = 0; ; attempt += 1) {
      try {
        await forwardOnce(resource, event, data);
        status.forwardCount += 1;
        status.lastForwardAt = new Date().toISOString();
        status.lastForwardError = null;
        return;
      } catch (error) {
        if (!shouldRetryForward(error, attempt)) {
          status.lastForwardError = error.message;
          throw error;
        }
        const delay = retryDelayMs(attempt);
        console.warn(
          `sidecar_forward_retry resource=${resource} event=${event} attempt=${attempt + 1} delay_ms=${delay} error=${error.message}`
        );
        await sleepMs(delay);
      }
    }
  } finally {
    inFlightForwards -= 1;
    status.inFlightForwards = inFlightForwards;
  }
}

async function forwardOnce(resource, event, data) {
  const envelope = {
    version: 1,
    resource,
    event,
    receivedAt: new Date().toISOString(),
    data,
  };
  const headers = {
    'content-type': 'application/json',
    'user-agent': 'webex-headless-sidecar/0.1.0',
  };
  if (forwardToken) {
    headers.authorization = `Bearer ${forwardToken}`;
  }

  const response = await fetch(target, {
    method: 'POST',
    headers,
    body: JSON.stringify(envelope),
    signal: AbortSignal.timeout(forwardTimeoutMs),
  });
  const body = await response.text();
  if (!response.ok) {
    throw new ForwardHttpError(response.status, body);
  }
  console.log(`sidecar_forwarded resource=${resource} event=${event} status=${response.status}`);
}

function shouldRetryForward(error, attempt) {
  if (attempt >= forwardRetries) {
    return false;
  }
  if (error instanceof ForwardHttpError) {
    return error.statusCode === 408 || error.statusCode === 429 || error.statusCode >= 500;
  }
  return true;
}

function retryDelayMs(attempt) {
  const base = Math.min(retryMaxMs, retryBaseMs * 2 ** attempt);
  return base + Math.floor(Math.random() * Math.max(1, Math.floor(base * 0.2)));
}

function sleepMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function loadWebexModules() {
  if (webexModules) {
    return webexModules;
  }
  const require = createRequire(import.meta.url);
  const merge = require('lodash/merge');
  const WebexCore = require('@webex/webex-core').default;
  const webexDefaultConfig = require('@webex/webex-core/dist/config.js').default;
  require('@webex/plugin-authorization');
  require('@webex/plugin-logger');
  require('@webex/internal-plugin-support');
  require('@webex/plugin-people');
  require('@webex/plugin-messages');
  webexModules = { merge, WebexCore, webexDefaultConfig };
  return webexModules;
}

function createWebex(accessToken) {
  const { merge, WebexCore, webexDefaultConfig } = loadWebexModules();
  const Webex = WebexCore.extend({
    webex: true,
    version: 'webex-headless-sidecar/0.1.0',
  });
  return new Webex({
    config: merge({}, webexDefaultConfig, {
      logger: {
        level: process.env.WEBEX_SDK_LOG_LEVEL ?? 'error',
      },
    }),
    credentials: {
      access_token: accessToken,
    },
  });
}

async function startListener(loadedToken) {
  const webex = createWebex(loadedToken.accessToken);
  await webex.people.get('me');
  await webex.internal.services.waitForCatalog('postauth', 30);
  await webex.messages.listen();

  const handlers = new Map();
  for (const eventName of messageEvents) {
    const handler = (event) => {
      forward('messages', eventName, messagePayload(event)).catch((error) => {
        console.error(error);
        shutdown(1).catch((shutdownError) => {
          console.error(shutdownError);
          process.exit(1);
        });
      });
    };
    webex.messages.on(eventName, handler);
    handlers.set(eventName, handler);
  }

  status.listening = true;
  status.tokenSource = loadedToken.source;
  status.tokenFingerprint = loadedToken.fingerprint;
  status.tokenExpiresAt = loadedToken.expiresAt;
  status.lastReloadError = null;
  console.log(
    `sidecar_listening resource=messages events=${messageEvents.join(',')} target=${target.href} token_source=${loadedToken.source} token_fingerprint=${loadedToken.fingerprint}`
  );
  return { webex, handlers, loadedToken };
}

async function stopListener(listener, markStopped = true) {
  for (const [eventName, handler] of listener.handlers) {
    try {
      listener.webex.messages.off(eventName, handler);
    } catch (_) {
      listener.webex.messages.off(eventName);
    }
  }
  await listener.webex.messages.stopListening();
  await listener.webex.internal.mercury.disconnect();
  if (markStopped) {
    status.listening = false;
  }
}

async function reloadTokenIfChanged(tokenConfig, reason) {
  if (tokenConfig.kind !== 'file') {
    return;
  }
  if (reloadInProgress) {
    console.warn(`sidecar_token_reload_skipped reason=${reason} already_in_progress=true`);
    return;
  }
  reloadInProgress = true;
  try {
    const loadedToken = await loadAccessToken(tokenConfig);
    status.lastReloadAt = new Date().toISOString();
    if (activeListener && loadedToken.fingerprint === activeListener.loadedToken.fingerprint) {
      return;
    }
    console.log(
      `sidecar_token_reloading reason=${reason} token_source=${loadedToken.source} token_fingerprint=${loadedToken.fingerprint}`
    );
    const nextListener = await startListener(loadedToken);
    const previousListener = activeListener;
    if (previousListener) {
      try {
        await stopListener(previousListener, false);
      } catch (error) {
        status.lastReloadError = error.message;
        console.error(error);
        activeListener = nextListener;
        await shutdown(1).catch((shutdownError) => {
          console.error(shutdownError);
          process.exit(1);
        });
        return;
      }
    }
    activeListener = nextListener;
  } catch (error) {
    status.lastReloadError = error.message;
    console.error(error);
    if (!activeListener) {
      await shutdown(1);
    }
  } finally {
    reloadInProgress = false;
  }
}

async function startHealthServer(bind) {
  if (!bind) {
    return null;
  }
  const { host, port } = parseHealthBind(bind);
  const server = createServer((request, response) => {
    const requestUrl = new URL(request.url ?? '/', 'http://localhost');
    if (request.method !== 'GET') {
      writeJson(response, 405, { ok: false, error: 'method not allowed' });
      return;
    }
    if (requestUrl.pathname === livePath) {
      writeJson(response, 200, healthPayload(true));
      return;
    }
    if (requestUrl.pathname === healthPath || requestUrl.pathname === readyPath) {
      writeJson(response, status.listening ? 200 : 503, healthPayload(status.listening));
      return;
    }
    writeJson(response, 404, { ok: false, error: 'not found' });
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, () => {
      server.off('error', reject);
      resolve();
    });
  });
  console.log(`sidecar_health_listening=${host}:${port}`);
  return server;
}

function healthErrorStatus(error) {
  return error ? 'redacted' : null;
}

function healthPayload(ok) {
  return {
    ok,
    startedAt: status.startedAt,
    listening: status.listening,
    target: status.target,
    messageEvents: status.messageEvents,
    tokenLoaded: Boolean(status.tokenFingerprint),
    tokenExpiresAt: status.tokenExpiresAt,
    forwardCount: status.forwardCount,
    inFlightForwards: status.inFlightForwards,
    lastForwardAt: status.lastForwardAt,
    lastForwardError: healthErrorStatus(status.lastForwardError),
    lastReloadAt: status.lastReloadAt,
    lastReloadError: healthErrorStatus(status.lastReloadError),
  };
}

function writeJson(response, statusCode, value) {
  const body = JSON.stringify(value);
  response.writeHead(statusCode, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(body),
  });
  response.end(body);
}

async function closeHealthServer() {
  if (!healthServer) {
    return;
  }
  await new Promise((resolve, reject) => {
    healthServer.close((error) => (error ? reject(error) : resolve()));
  });
  healthServer = null;
}

async function shutdown(code = 0) {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  if (reloadTimer) {
    clearInterval(reloadTimer);
    reloadTimer = null;
  }
  if (activeListener) {
    await stopListener(activeListener);
    activeListener = null;
  }
  await closeHealthServer();
  process.exit(code);
}

async function main() {
  validateForwardingAuth();

  if (process.env.WEBEX_SIDECAR_MOCK_EVENT) {
    await forward('messages', 'created', {
      id: 'mock-message',
      roomId: 'mock-room',
      personEmail: 'generic-account@example.com',
      text: 'mock sidecar event',
      created: new Date().toISOString(),
    });
    console.log('sidecar_mock_event_sent=true');
    return;
  }

  const tokenConfig = accessTokenConfigFromEnv();
  if (healthBind && validateConfig) {
    parseHealthBind(healthBind);
  }
  if (validateConfig) {
    const loadedToken = await loadAccessToken(tokenConfig);
    console.log(
      `sidecar_config_valid=true token_source=${loadedToken.source} token_fingerprint=${loadedToken.fingerprint}`
    );
    return;
  }

  healthServer = await startHealthServer(healthBind);
  activeListener = await startListener(await loadAccessToken(tokenConfig));
  if (tokenConfig.kind === 'file' && tokenReloadIntervalMs > 0) {
    reloadTimer = setInterval(() => {
      reloadTokenIfChanged(tokenConfig, 'interval').catch((error) => {
        console.error(error);
      });
    }, tokenReloadIntervalMs);
  }

  process.on('SIGHUP', () => {
    reloadTokenIfChanged(tokenConfig, 'sighup').catch((error) => {
      console.error(error);
    });
  });
  process.stdin.resume();
}

process.on('SIGINT', () => {
  shutdown().catch((error) => {
    console.error(error);
    process.exit(1);
  });
});
process.on('SIGTERM', () => {
  shutdown().catch((error) => {
    console.error(error);
    process.exit(1);
  });
});

await main().catch(async (error) => {
  console.error(error);
  await shutdown(1);
});
