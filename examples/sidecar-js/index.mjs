import { createRequire } from 'node:module';
import { dirname } from 'node:path';

const targetUrl = process.env.WEBEX_SIDECAR_TARGET_URL ?? 'http://127.0.0.1:8787/webex/events';
const forwardToken = process.env.WEBEX_SIDECAR_TOKEN;
const messageEvents = (process.env.WEBEX_SIDECAR_MESSAGE_EVENTS ?? 'created,deleted')
  .split(',')
  .map((event) => event.trim())
  .filter(Boolean);
let shuttingDown = false;

async function forward(resource, event, data) {
  const envelope = {
    version: 1,
    resource,
    event,
    receivedAt: new Date().toISOString(),
    data,
  };
  const headers = {
    'content-type': 'application/json',
    'user-agent': 'webex-headless-sidecar-demo/0.1.0',
  };
  if (forwardToken) {
    headers.authorization = `Bearer ${forwardToken}`;
  }

  const response = await fetch(targetUrl, {
    method: 'POST',
    headers,
    body: JSON.stringify(envelope),
  });
  const body = await response.text();
  if (!response.ok) {
    throw new Error(`forward failed status=${response.status} body=${body}`);
  }
  console.log(`sidecar_forwarded resource=${resource} event=${event} status=${response.status}`);
}

if (process.env.WEBEX_SIDECAR_MOCK_EVENT) {
  await forward('messages', 'created', {
    id: 'mock-message',
    roomId: 'mock-room',
    personEmail: 'generic-account@example.com',
    text: 'mock sidecar event',
    created: new Date().toISOString(),
  });
  console.log('sidecar_mock_event_sent=true');
  process.exit(0);
}

const accessToken = process.env.WEBEX_ACCESS_TOKEN;
if (!accessToken) {
  throw new Error('WEBEX_ACCESS_TOKEN is required unless WEBEX_SIDECAR_MOCK_EVENT=1');
}

const require = createRequire(import.meta.url);
const merge = require('lodash/merge');
const WebexCore = require('@webex/webex-core').default;
const webexPackageDir = dirname(require.resolve('webex/package'));
const webexDefaultConfig = require(`${webexPackageDir}/dist/config.js`).default;
require('@webex/plugin-authorization');
require('@webex/plugin-logger');
require('@webex/internal-plugin-support');
require('@webex/plugin-people');
require('@webex/plugin-messages');

const Webex = WebexCore.extend({
  webex: true,
  version: 'webex-headless-sidecar-demo/0.1.0',
});
const webex = new Webex({
  config: merge({}, webexDefaultConfig, {
    logger: {
      level: process.env.WEBEX_SDK_LOG_LEVEL ?? 'error',
    },
  }),
  credentials: {
    access_token: accessToken,
  },
});

await webex.people.get('me');
await webex.internal.services.waitForCatalog('postauth', 30);
await webex.messages.listen();
for (const eventName of messageEvents) {
  webex.messages.on(eventName, (event) => {
    forward('messages', eventName, event).catch((error) => {
      console.error(error);
      shutdown(1).catch((shutdownError) => {
        console.error(shutdownError);
        process.exit(1);
      });
    });
  });
}

console.log(`sidecar_listening resource=messages events=${messageEvents.join(',')} target=${targetUrl}`);

async function shutdown(code = 0) {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  for (const eventName of messageEvents) {
    webex.messages.off(eventName);
  }
  await webex.messages.stopListening();
  process.exit(code);
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
process.stdin.resume();
