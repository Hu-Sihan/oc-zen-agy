import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import https from 'node:https';
import net from 'node:net';
import tls from 'node:tls';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { setTimeout as delay } from 'node:timers/promises';
import { defaults, createForwarder, startServers, requestID, loadConfig, readLimited, sessions } from '../src/forwarder.mjs';
import { clientHeaders, tools } from '../src/profile.mjs';
import { listen, close, fetch } from '../support/virtual-http.mjs';

const firstSession = 'ses_0123456789abABCDEFGHIJKLMN';
const secondSession = 'ses_0123456789ab0123456789abcd';
const payload = { model: 'big-pickle', messages: [{ role: 'user', content: 'private test prompt' }], max_tokens: 256 };
const frame = value => `data: ${JSON.stringify(value)}\n\n`;
const chunk = (content, finish = null) => ({ id: 'completion-test', model: 'big-pickle', created: 123,
  choices: [{ index: 0, delta: { role: 'assistant', content }, finish_reason: finish }] });
const readBody = async request => JSON.parse((await readLimited(request, 1024 * 1024)).toString());

async function temporary(t) {
  const directory = await mkdtemp(join(tmpdir(), 'zen-forwarder-test-'));
  t.after(async () => {
    assert.ok(directory.startsWith(join(tmpdir(), 'zen-forwarder-test-')));
    await rm(directory, { recursive: true, force: true });
  });
  return directory;
}

async function setup(t, handler, options = {}) {
  const stateDir = await temporary(t);
  await writeFile(join(stateDir, 'sessions.txt'), options.initialSessions ?? `${firstSession}\n${secondSession}\n`);
  const upstream = http.createServer(handler);
  const url = await listen(upstream);
  const config = { ...defaults, stateDir, baseURL: `${url}/v1`, modelsURL: `${url}/models`,
    apiKey: 'upstream-secret', localApiKey: '', ...options };
  const logs = [], servers = [];
  const forwarder = createForwarder(config, { sessionIds: await sessions(config), log: event => logs.push(event) });
  const addListener = async () => {
    const server = forwarder.createServer(); servers.push(server);
    return listen(server);
  };
  const local = await addListener();
  t.after(async () => { for (const server of servers) await close(server); forwarder.close(); await close(upstream); });
  const send = (body = payload, headers = {}, target = local, signal) => fetch(`${target}/v1/chat/completions`, {
    method: 'POST', headers: { 'content-type': 'application/json', ...headers }, signal,
    body: typeof body === 'string' ? body : JSON.stringify(body),
  });
  return { config, stateDir, local, logs, send, addListener, forwarder };
}

async function waitForQueue(app, queued) {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    const response = await fetch(`${app.local}/healthz`);
    const state = await response.json();
    if (state.queued === queued) return state;
    await delay(2);
  }
  assert.fail(`queue did not reach ${queued}`);
}

test('request IDs have the expected shape and do not repeat', () => {
  const ids = Array.from({ length: 100 }, requestID);
  assert.equal(new Set(ids).size, ids.length);
  assert.ok(ids.every(id => /^msg_[0-9a-f]{12}[A-Za-z0-9]{14}$/.test(id)));
});

test('plain chat uses the compiled profile and reads only sessions from state', async t => {
  const seen = [];
  const app = await setup(t, async (req, res) => {
    seen.push({ headers: req.headers, body: await readBody(req) });
    res.setHeader('content-type', 'application/json'); res.end('{"ok":true}');
  });
  await writeFile(join(app.stateDir, 'client.json'), 'invalid old state');
  await writeFile(join(app.stateDir, 'tools.json'), 'invalid old state');
  for (let index = 0; index < 2; index++) {
    const response = await app.send(payload, { authorization: 'Bearer downstream-secret' });
    assert.equal(response.status, 200); await response.text();
    assert.equal(response.headers.get('x-zen-compatibility'), 'builtin-tools-none');
  }
  assert.equal(tools.length, 88);
  assert.deepEqual(seen[0].body, { ...payload, tools, tool_choice: 'none', stream: true, stream_options: { include_usage: true } });
  for (const [key, value] of Object.entries(clientHeaders)) assert.equal(seen[0].headers[key], value);
  assert.equal(seen[0].headers.authorization, 'Bearer upstream-secret');
  assert.equal(seen[0].headers['x-opencode-session'], firstSession);
  assert.equal(seen[1].headers['x-opencode-session'], secondSession);
  assert.notEqual(seen[0].headers['x-opencode-request'], seen[1].headers['x-opencode-request']);
  assert.ok(!JSON.stringify(app.logs).includes('private test prompt'));
  assert.ok(!JSON.stringify(app.logs).includes('upstream-secret'));
  assert.ok(!JSON.stringify(app.logs).includes(firstSession));
});

test('client messages, multimodal data, tool policies and output budget are preserved', async t => {
  const seen = [];
  const app = await setup(t, async (req, res) => { seen.push(await readBody(req)); res.end('{}'); });
  const bodies = [
    { ...payload, tools: [], tool_choice: 'none', temperature: 0.4, messages: [{ role: 'user', content: [{ type: 'text', text: 'hello' }] }] },
    { ...payload, tool_choice: 'none', stream_options: { include_usage: false } },
  ];
  for (const body of bodies) { const response = await app.send(body); await response.text(); assert.equal(response.headers.get('x-zen-compatibility'), 'passthrough'); }
  for (let index = 0; index < bodies.length; index++)
    assert.deepEqual(seen[index], { ...bodies[index], stream: true, stream_options: { include_usage: true, ...bodies[index].stream_options } });
});

test('upstream SSE is merged for JSON clients, including usage and reasoning', async t => {
  const app = await setup(t, async (req, res) => {
    const body = await readBody(req); assert.equal(body.max_tokens, 256); assert.equal(body.stream, true);
    res.writeHead(200, { 'content-type': 'text/event-stream', 'x-opencode-log-id': 'upstream-log' });
    const first = chunk('O'); first.choices[0].delta.reasoning_content = 'think';
    res.end(frame(first) + frame(chunk('K', 'stop')) + frame({ id: 'completion-test', choices: [], usage: { total_tokens: 30 } }) + 'data: [DONE]\n\n');
  });
  const response = await app.send();
  assert.equal(response.headers.get('x-opencode-log-id'), 'upstream-log');
  assert.match(response.headers.get('content-type'), /application\/json/);
  const body = await response.json();
  assert.equal(body.choices[0].message.content, 'OK');
  assert.equal(body.choices[0].message.reasoning_content, 'think');
  assert.equal(body.choices[0].finish_reason, 'stop');
  assert.equal(body.usage.total_tokens, 30);
});

test('SSE clients receive the first chunk before completion', async t => {
  const gate = Promise.withResolvers(); t.after(() => gate.resolve());
  const app = await setup(t, async (req, res) => {
    await readBody(req); res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write(frame(chunk('first'))); await gate.promise;
    res.end(frame(chunk('last', 'stop')) + 'data: [DONE]\n\n');
  });
  const response = await app.send({ ...payload, stream: true });
  const reader = response.body.getReader();
  assert.match(new TextDecoder().decode((await reader.read()).value), /first/);
  gate.resolve();
  let rest = ''; for (;;) { const part = await reader.read(); if (part.done) break; rest += new TextDecoder().decode(part.value); }
  assert.match(rest, /last/); assert.match(rest, /\[DONE\]/);
});

test('listeners share one FIFO queue and retain a concurrency slot until the stream finishes', async t => {
  const calls = [], selected = [];
  const gate = Promise.withResolvers(); t.after(() => gate.resolve());
  const app = await setup(t, async (req, res) => {
    if (req.method === 'GET') { calls.push('models'); res.end('{"data":[]}'); return; }
    const body = await readBody(req); calls.push(body.marker); selected.push(req.headers['x-opencode-session']);
    res.writeHead(200, { 'content-type': 'text/event-stream' }); res.write(frame(chunk('first')));
    await gate.promise; res.end(frame(chunk('last', 'stop')) + 'data: [DONE]\n\n');
  }, { concurrency: 1 });
  const secondListener = await app.addListener();
  const first = await app.send({ ...payload, marker: 'first', stream: true });
  const second = app.send({ ...payload, marker: 'second' }, {}, secondListener);
  assert.equal((await waitForQueue(app, 1)).active, 1);
  const models = fetch(`${secondListener}/v1/models`);
  await waitForQueue(app, 2);
  assert.deepEqual(calls, ['first']);
  gate.resolve(); await first.text();
  const next = await second; assert.equal(next.status, 200); await next.text();
  const list = await models; assert.equal(list.status, 200); await list.text();
  assert.deepEqual(calls, ['first', 'second', 'models']);
  const third = await app.send({ ...payload, marker: 'third' }, {}, secondListener); await third.text();
  assert.deepEqual(selected, [firstSession, secondSession, firstSession]);
  assert.equal((await waitForQueue(app, 0)).active, 0);
});

test('the scheduler dispatches only the available number of concurrent requests', async t => {
  const gate = Promise.withResolvers(); t.after(() => gate.resolve());
  let calls = 0;
  const app = await setup(t, async (req, res) => {
    await readBody(req); calls++; res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write(frame(chunk('start'))); await gate.promise; res.end(frame(chunk('end', 'stop')) + 'data: [DONE]\n\n');
  }, { concurrency: 2 });
  const one = await app.send({ ...payload, stream: true });
  const two = await app.send({ ...payload, stream: true });
  const three = app.send();
  assert.equal((await waitForQueue(app, 1)).active, 2);
  assert.equal(calls, 2); gate.resolve(); await one.text(); await two.text();
  const third = await three; assert.equal(third.status, 200); await third.text(); assert.equal(calls, 3);
});

test('the entry callback prepares and snapshots the session before queueing', async t => {
  const gate = Promise.withResolvers(); t.after(() => gate.resolve());
  const seen = [];
  const app = await setup(t, async (req, res) => {
    const body = await readBody(req); seen.push({ session: req.headers['x-opencode-session'], body });
    res.writeHead(200, { 'content-type': 'text/event-stream' }); res.write(frame(chunk('start')));
    await gate.promise; res.end(frame(chunk('end', 'stop')) + 'data: [DONE]\n\n');
  }, { concurrency: 1 });
  const first = await app.send({ ...payload, stream: true });
  const pending = app.send(); await waitForQueue(app, 1);
  await writeFile(join(app.stateDir, 'sessions.txt'), '');
  gate.resolve(); await first.text();
  const second = await pending; assert.equal(second.status, 200); await second.text();
  assert.equal(seen[1].session, secondSession);
  assert.deepEqual(seen[1].body.tools, tools); assert.equal(seen[1].body.stream, true);
  const next = await app.send(); assert.equal(next.status, 200); await next.text();
  assert.equal(seen[2].session, firstSession);
});

test('invalid or unauthenticated requests are rejected before entering a full queue', async t => {
  const gate = Promise.withResolvers(); t.after(() => gate.resolve());
  let calls = 0;
  const app = await setup(t, async (req, res) => {
    await readBody(req); calls++; res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write(frame(chunk('start'))); await gate.promise; res.end('data: [DONE]\n\n');
  }, { concurrency: 1, localApiKey: 'local-secret' });
  const headers = { authorization: 'Bearer local-secret' };
  const first = await app.send({ ...payload, stream: true }, headers);
  const invalid = await app.send('{', headers); assert.equal(invalid.status, 400); await invalid.text();
  const denied = await app.send(); assert.equal(denied.status, 401); await denied.text();
  const state = await fetch(`${app.local}/healthz`, { headers });
  assert.equal((await state.json()).queued, 0); assert.equal(calls, 1);
  gate.resolve(); await first.text();
});

test('a disconnected queued client is removed and never sent upstream', { timeout: 3000 }, async t => {
  const gate = Promise.withResolvers(); t.after(() => gate.resolve());
  const seen = [];
  const app = await setup(t, async (req, res) => {
    const body = await readBody(req); seen.push(body.marker);
    res.writeHead(200, { 'content-type': 'text/event-stream' }); res.write(frame(chunk('start')));
    await gate.promise; res.end(frame(chunk('end', 'stop')) + 'data: [DONE]\n\n');
  }, { concurrency: 1 });
  const first = await app.send({ ...payload, marker: 'first', stream: true });
  const aborter = new AbortController();
  const pending = app.send({ ...payload, marker: 'cancelled' }, {}, app.local, aborter.signal);
  const rejected = assert.rejects(pending);
  await waitForQueue(app, 1); aborter.abort(); await rejected; await waitForQueue(app, 0);
  const next = app.send({ ...payload, marker: 'next' }); await waitForQueue(app, 1);
  gate.resolve(); await first.text(); const response = await next; assert.equal(response.status, 200); await response.text();
  assert.deepEqual(seen, ['first', 'next']);
});

test('a queued request can expire without being dispatched or consuming a running slot', { timeout: 3000 }, async t => {
  const gate = Promise.withResolvers(); t.after(() => gate.resolve());
  let calls = 0;
  const app = await setup(t, async (req, res) => {
    await readBody(req); calls++; res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write(frame(chunk('start'))); await gate.promise; res.end('data: [DONE]\n\n');
  }, { concurrency: 1, requestTimeoutMs: 2000 });
  const first = await app.send({ ...payload, stream: true });
  app.config.requestTimeoutMs = 80;
  const pending = app.send(); await waitForQueue(app, 1);
  const timedOut = await pending; assert.equal(timedOut.status, 504);
  assert.equal((await timedOut.json()).error.type, 'request_timeout');
  assert.equal((await waitForQueue(app, 0)).active, 1); assert.equal(calls, 1);
  gate.resolve(); await first.text();
});

test('shutdown rejects queued work, permits an active response to finish, and stops dispatch', async t => {
  const gate = Promise.withResolvers(); t.after(() => gate.resolve());
  let calls = 0;
  const app = await setup(t, async (req, res) => {
    await readBody(req); calls++; res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write(frame(chunk('start'))); await gate.promise; res.end('data: [DONE]\n\n');
  }, { concurrency: 1 });
  const first = await app.send({ ...payload, stream: true });
  const pending = app.send(); await waitForQueue(app, 1);
  app.forwarder.stop();
  const stopped = await pending; assert.equal(stopped.status, 503);
  assert.equal((await stopped.json()).error.type, 'server_closing');
  gate.resolve(); await first.text(); assert.equal(calls, 1);
  const next = await app.send(); assert.equal(next.status, 503); await next.text();
});

test('both model routes fetch fresh upstream bytes without filtering, forwarding queries', async t => {
  let calls = 0;
  const app = await setup(t, (req, res) => {
    assert.equal(req.url, '/models?fresh=1'); assert.equal(req.headers.authorization, 'Bearer upstream-secret');
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(`{ "data": [{"id":"paid-model"},{"id":"big-pickle"}], "revision": ${++calls} }\n`);
  });
  await writeFile(join(app.stateDir, 'sessions.txt'), '');
  for (const route of ['/v1/model', '/v1/models']) {
    const response = await fetch(`${app.local}${route}?fresh=1`);
    assert.equal(response.status, 200);
    assert.equal(await response.text(), `{ "data": [{"id":"paid-model"},{"id":"big-pickle"}], "revision": ${calls} }\n`);
  }
  assert.equal(calls, 2);
});

test('upstream error statuses, bodies and rate limit headers are preserved without retries', async t => {
  for (const status of [400, 403, 429, 503]) {
    let calls = 0;
    const app = await setup(t, async (req, res) => {
      if (req.method === 'POST') await readBody(req);
      calls++; res.writeHead(status, { 'content-type': 'application/json', 'retry-after': '5', 'x-ratelimit-remaining': '0' });
      res.end('{"error":{"type":"upstream_denied"}}');
    });
    for (const response of [await app.send(), await fetch(`${app.local}/v1/model`)]) {
      assert.equal(response.status, status); assert.equal(response.headers.get('retry-after'), '5');
      assert.equal(await response.text(), '{"error":{"type":"upstream_denied"}}');
    }
    assert.equal(calls, 2);
  }
});

test('local authentication protects every route and is never forwarded as the upstream key', async t => {
  const app = await setup(t, async (req, res) => {
    if (req.method === 'POST') await readBody(req);
    assert.equal(req.headers.authorization, 'Bearer upstream-secret'); res.end('{}');
  }, { localApiKey: 'local-secret' });
  for (const route of ['/healthz', '/v1/model', '/v1/models']) {
    const denied = await fetch(`${app.local}${route}`); assert.equal(denied.status, 401); await denied.text();
    const allowed = await fetch(`${app.local}${route}`, { headers: { authorization: 'Bearer local-secret' } });
    assert.equal(allowed.status, 200); await allowed.text();
  }
  const denied = await app.send(); assert.equal(denied.status, 401); await denied.text();
  const allowed = await app.send(payload, { authorization: 'Bearer local-secret' }); assert.equal(allowed.status, 200); await allowed.text();
});

test('invalid requests fail locally and release the concurrency slot', async t => {
  let calls = 0;
  const app = await setup(t, () => { calls++; }, { concurrency: 1 });
  for (const [body, type] of [['{', 'invalid_json'], [{ model: '' }, 'invalid_model'],
    [{ ...payload, messages: [] }, 'invalid_messages'], [{ ...payload, stream: 'yes' }, 'invalid_stream'],
    [{ ...payload, stream_options: null }, 'invalid_stream_options']]) {
    const response = await app.send(body); assert.equal(response.status, 400); assert.equal((await response.json()).error.type, type);
  }
  const unsupported = await fetch(`${app.local}/v1/responses`); assert.equal(unsupported.status, 404); await unsupported.text();
  assert.equal(calls, 0);
  const health = await fetch(`${app.local}/healthz`); assert.equal((await health.json()).active, 0);
});

test('oversized requests return JSON 413 and do not occupy a slot', async t => {
  const app = await setup(t, () => assert.fail('must reject before upstream'), { maxBodyBytes: 128, concurrency: 1 });
  const response = await app.send({ ...payload, padding: 'a'.repeat(1000) });
  assert.equal(response.status, 413); assert.equal((await response.json()).error.type, 'body_too_large');
  const health = await fetch(`${app.local}/healthz`); assert.equal((await health.json()).active, 0);
});

test('sessions are read once and deduplicated in order, with round robin and no response or file updates', async t => {
  const seen = [];
  const source = `\uFEFF# comment\r\n${firstSession}\r\n${secondSession}\r\n${firstSession}\r\n`;
  const returned = 'ses_abcdef012345ABCDEFGHIJKLMN';
  const app = await setup(t, async (req, res) => {
    await readBody(req); seen.push(req.headers['x-opencode-session']);
    res.writeHead(200, { 'content-type': 'application/json', 'x-opencode-session': returned });
    res.end(JSON.stringify({ session_id: returned, id: returned }));
  }, { initialSessions: source });
  const file = join(app.stateDir, 'sessions.txt');
  assert.equal(await readFile(file, 'utf8'), source);
  await writeFile(file, 'changed while running');
  for (let index = 0; index < 2; index++) { const response = await app.send(); assert.equal(response.status, 200); await response.text(); }
  assert.equal(await readFile(file, 'utf8'), 'changed while running');
  await rm(file);
  for (let index = 0; index < 2; index++) { const response = await app.send(); assert.equal(response.status, 200); await response.text(); }
  assert.deepEqual(seen, [firstSession, secondSession, firstSession, secondSession]);
  const health = await fetch(`${app.local}/healthz`); assert.equal((await health.json()).sessions, 2);
  await assert.rejects(readFile(file), { code: 'ENOENT' });
});

test('the startup session list has no capacity truncation and invalid IDs fail before listening', async t => {
  const stateDir = await temporary(t), file = join(stateDir, 'sessions.txt');
  const ids = Array.from({ length: 128 }, (_, index) => `ses_${index.toString(16).padStart(12, '0')}ABCDEFGHIJKLMN`);
  await writeFile(file, [...ids, ...ids].join('\n'));
  assert.deepEqual(await sessions({ stateDir }), ids);
  await writeFile(file, 'not-a-session');
  await assert.rejects(startServers({ ...defaults, stateDir, apiKey: 'public', localApiKey: '' }), { code: 'invalid_sessions' });
  const empty = await setup(t, () => assert.fail('no session must not reach upstream'), { initialSessions: '' });
  const response = await empty.send(); assert.equal(response.status, 503); assert.equal((await response.json()).error.type, 'session_missing');
});

test('timeouts terminate the upstream and release the slot', { timeout: 3000 }, async t => {
  const app = await setup(t, async req => { await readBody(req); }, { requestTimeoutMs: 100, concurrency: 1 });
  const response = await app.send(); assert.equal(response.status, 504); assert.equal((await response.json()).error.type, 'request_timeout');
  const health = await fetch(`${app.local}/healthz`); assert.equal((await health.json()).active, 0);
});

test('client disconnect cancels an active stream and frees concurrency', { timeout: 3000 }, async t => {
  const closed = Promise.withResolvers();
  const app = await setup(t, async (req, res) => {
    await readBody(req); res.once('close', () => closed.resolve());
    res.writeHead(200, { 'content-type': 'text/event-stream' }); res.write(frame(chunk('first')));
  }, { concurrency: 1 });
  const aborter = new AbortController();
  const response = await app.send({ ...payload, stream: true }, {}, app.local, aborter.signal);
  const reader = response.body.getReader(); await reader.read();
  aborter.abort(); await assert.rejects(reader.read()); await closed.promise;
  const health = await fetch(`${app.local}/healthz`); assert.equal((await health.json()).active, 0);
});

test('incomplete upstream SSE is not returned as a successful JSON completion', async t => {
  const app = await setup(t, async (req, res) => { await readBody(req); res.writeHead(200, { 'content-type': 'text/event-stream' }); res.end(frame(chunk('partial'))); });
  const response = await app.send(); assert.equal(response.status, 502); assert.equal((await response.json()).error.type, 'incomplete_upstream_stream');
});

test('HTTPS keeps certificate verification enabled', { timeout: 5000 }, async t => {
  const cert = await readFile(new URL('./fixtures/localhost-cert.pem', import.meta.url));
  const key = await readFile(new URL('./fixtures/localhost-key.pem', import.meta.url));
  const target = https.createServer({ cert, key }, async (req, res) => { await readBody(req); res.end('{"secure":true}'); });
  const targetURL = (await listen(target)).replace('http:', 'https:');
  t.after(() => close(target));
  const app = await setup(t, () => assert.fail('must use HTTPS'), { baseURL: `${targetURL}/v1` });
  const connect = tls.connect;
  let trustFixture = false;
  t.mock.method(tls, 'connect', function (options, ...rest) {
    return connect.call(this, { ...options, socket: net.createConnection({ host: options.host, port: options.port }),
      ...(trustFixture ? { ca: cert } : {}) }, ...rest);
  });
  const untrusted = await app.send(); assert.equal(untrusted.status, 502);
  assert.match((await untrusted.json()).error.type, /SELF_SIGNED_CERT|UNABLE_TO_VERIFY/);
  trustFixture = true;
  const trusted = await app.send(); const body = await trusted.json();
  assert.equal(trusted.status, 200, JSON.stringify(body)); assert.deepEqual(body, { secure: true });
});

test('configuration accepts multiple addresses and wildcards, rejecting invalid limits and overlaps', async t => {
  const directory = await temporary(t), file = join(directory, 'config.json');
  for (const host of ['0.0.0.0', ['127.0.0.1', '192.0.2.1'], ['0.0.0.0', '::']]) {
    await writeFile(file, JSON.stringify({ host, concurrency: 7 }));
    const config = await loadConfig(file); assert.equal(config.concurrency, 7); assert.deepEqual(config.host, Array.isArray(host) ? host : [host]);
    assert.equal(config.stateDir, join(directory, 'state'));
  }
  for (const invalid of [{ concurrency: 0 }, { concurrency: 1.5 }, { host: [] }, { host: 'https://bad' },
    { host: ['0.0.0.0', '127.0.0.1'] }, { port: 70000 }, { unexpected: true }, { baseURL: 'file:///tmp/upstream' }]) {
    await writeFile(file, JSON.stringify(invalid)); await assert.rejects(loadConfig(file));
  }
});

test('startup binds every configured address, with IPv6-only sockets, and closes all listeners', async t => {
  const stateDir = await temporary(t);
  const bound = [], closed = [];
  t.mock.method(http.Server.prototype, 'listen', function (options) { bound.push(options); queueMicrotask(() => this.emit('listening')); return this; });
  t.mock.method(http.Server.prototype, 'close', function (callback) { closed.push(this); queueMicrotask(callback); return this; });
  const app = await startServers({ ...defaults, stateDir, host: ['127.0.0.1', '::1'], apiKey: 'public', localApiKey: '' });
  assert.deepEqual(bound, [{ host: '127.0.0.1', port: 9999, ipv6Only: false }, { host: '::1', port: 9999, ipv6Only: true }]);
  await app.close(); assert.equal(closed.length, 2);
});

test('a bind failure rolls back all listeners instead of leaving a partial service', async t => {
  const stateDir = await temporary(t);
  const closed = []; let count = 0;
  t.mock.method(http.Server.prototype, 'listen', function () {
    const failed = ++count === 2;
    queueMicrotask(() => failed ? this.emit('error', Object.assign(new Error('busy'), { code: 'EADDRINUSE' })) : this.emit('listening'));
    return this;
  });
  t.mock.method(http.Server.prototype, 'close', function (callback) { closed.push(this); queueMicrotask(callback); return this; });
  await assert.rejects(startServers({ ...defaults, stateDir, host: ['127.0.0.1', '::1'], apiKey: 'public', localApiKey: '' }), { code: 'EADDRINUSE' });
  assert.equal(closed.length, 2);
});
