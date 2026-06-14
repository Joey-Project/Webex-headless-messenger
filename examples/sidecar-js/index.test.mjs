import assert from "node:assert/strict";
import { createServer } from "node:http";
import { test } from "node:test";

const sidecarEnvKeys = [
  "WEBEX_SIDECAR_TARGET_URL",
  "WEBEX_SIDECAR_MAX_IN_FLIGHT",
  "WEBEX_SIDECAR_MAX_QUEUED_FORWARDS",
  "WEBEX_SIDECAR_FORWARD_RETRIES",
  "WEBEX_SIDECAR_RETRY_BASE_MS",
  "WEBEX_SIDECAR_RETRY_MAX_MS",
];

function saveEnv(keys) {
  return new Map(keys.map((key) => [key, process.env[key]]));
}

function restoreEnv(snapshot) {
  for (const [key, value] of snapshot) {
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }
}

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
}

async function close(server) {
  await new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

async function waitFor(predicate, timeoutMs = 1500) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.fail("timed out waiting for condition");
}

test("retry-delayed forwards count against the bounded outstanding budget", async () => {
  let requestCount = 0;
  const server = createServer((_request, response) => {
    requestCount += 1;
    response.writeHead(503, { "retry-after": "1" });
    response.end("busy");
  });
  await listen(server);

  const env = saveEnv(sidecarEnvKeys);
  try {
    const { port } = server.address();
    process.env.WEBEX_SIDECAR_TARGET_URL = `http://127.0.0.1:${port}/events`;
    process.env.WEBEX_SIDECAR_MAX_IN_FLIGHT = "1";
    process.env.WEBEX_SIDECAR_MAX_QUEUED_FORWARDS = "1";
    process.env.WEBEX_SIDECAR_FORWARD_RETRIES = "1";
    process.env.WEBEX_SIDECAR_RETRY_BASE_MS = "1000";
    process.env.WEBEX_SIDECAR_RETRY_MAX_MS = "1000";

    const sidecar = await import(`./index.mjs?test=${Date.now()}`);
    const first = sidecar.__test
      .forward("messages", "created", { id: "first" })
      .catch((error) => error);
    await waitFor(() => requestCount >= 1);

    const second = sidecar.__test
      .forward("messages", "created", { id: "second" })
      .catch((error) => error);
    await waitFor(() => requestCount >= 2);
    await waitFor(() => sidecar.__test.status.queuedForwards >= 2);

    await assert.rejects(
      sidecar.__test.forward("messages", "created", { id: "third" }),
      { name: "ForwardQueueFullError" }
    );

    assert.equal(sidecar.__test.status.inFlightForwards, 0);
    assert.equal(sidecar.__test.status.queuedForwards, 2);
    assert.match(sidecar.__test.status.lastForwardError, /too many queued forwards/);

    assert.equal((await first).name, "ForwardHttpError");
    assert.equal((await second).name, "ForwardHttpError");
    assert.equal(sidecar.__test.status.queuedForwards, 0);
  } finally {
    restoreEnv(env);
    await close(server);
  }
});
