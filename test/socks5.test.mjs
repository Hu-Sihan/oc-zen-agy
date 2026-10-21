import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import https from 'node:https';
import tls from 'node:tls';
import { mkdtemp, writeFile, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { defaults, createForwarder, loadConfig, startServers } from '../src/forwarder.mjs';
import { parseProxies, loadProxies, createProxyPool, openSocks5 } from '../src/socks5.mjs';
import { listen, close, fetch } from '../support/virtual-http.mjs';
import { socksServer } from '../support/socks5-server.mjs';

const session = 'ses_0123456789abABCDEFGHIJKLMN';
const payload = { model: 'test-model', messages: [{ role: 'user', content: 'test' }], tools: [] };
const proxyEntry = (server, auth = {}) => ({ host: server.host, port: server.port, username: '', password: '', ...auth });

async function directory(t) {
  const dir = await mkdtemp(join(tmpdir(), 'oc-zen-socks-test-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  return dir;
}

async function upstream(t, handler, secure) {
  const server = secure ? https.createServer(secure, handler) : http.createServer(handler);
  const url = (await listen(server)).replace('http:', secure ? 'https:' : 'http:');
  t.after(() => close(server));
  return url;
}

async function app(t, pool, overrides = {}) {
  const config = { ...defaults, requestTimeoutMs: 1500, host: ['127.0.0.1'], apiKey: 'upstream-test-key', localApiKey: '',
    baseURL: 'http://remote.invalid:12345/v1', modelsURL: 'http://remote.invalid:12345/models', ...overrides };
  const logs = [];
  const forwarder = createForwarder(config, { sessionIds: [session], proxyPool: pool, log: event => logs.push(event) });
  const server = forwarder.createServer(), local = await listen(server);
  t.after(async () => { forwarder.close(); await close(server); });
  const send = (body = payload, signal) => fetch(`${local}/v1/chat/completions`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body), signal,
  });
  const health = async () => (await fetch(`${local}/healthz`)).json();
  return { send, health, local, logs };
}

test('proxy files accept Webshare lines, URLs, encoded credentials, BOM, IPv6 and ordered deduplication', async t => {
  const text = '\uFEFF# comment\r\n127.0.0.1:1080:user:p:a@ss\r\nsocks5://user:p%3Aa%40ss@127.0.0.1:1080\n' +
    'socks5h://proxy.example:1234\n[::1]:9999:other:secret\n';
  const proxies = parseProxies(text);
  assert.deepEqual(proxies, [
    { host: '127.0.0.1', port: 1080, username: 'user', password: 'p:a@ss' },
    { host: 'proxy.example', port: 1234, username: '', password: '' },
    { host: '::1', port: 9999, username: 'other', password: 'secret' },
  ]);
  const dir = await directory(t), file = join(dir, 'socks5.txt');
  await writeFile(file, text); assert.deepEqual(await loadProxies(file), proxies);
  for (const line of ['http://user:secret@proxy:80', 'host:0:user:secret', 'host:65536', 'host:10:user:',
    'socks5://:secret@host:1080', 'socks5://host:1080/path', 'socks5://host:1080?secret=1',
    'socks5://user:%ZZ@host:1080', `host:1080:${'u'.repeat(256)}:secret`, '# empty']) {
    assert.throws(() => parseProxies(line), error => !error.message.includes('secret'));
  }
});

test('proxy rotation skips exhausted entries and restores every entry at UTC midnight', () => {
  let time = Date.parse('2026-10-21T23:59:58.500Z');
  const entries = parseProxies('first.example:1080\nsecond.example:1080');
  const pool = createProxyPool(entries, () => time);
  assert.equal(pool.next(), entries[0]); assert.equal(pool.next(), entries[1]);
  pool.disable(entries[0]); assert.equal(pool.next(), entries[1]);
  pool.disable(entries[1]); pool.disable(entries[1]);
  assert.deepEqual(pool.status(), { total: 2, available: 0, disabled: 2, resetAt: '2026-10-22T00:00:00.000Z' });
  assert.throws(() => pool.next(), error => error.status === 503 && error.code === 'proxy_pool_exhausted' && error.retryAfter === 2);
  time = Date.parse('2026-10-21T23:59:59.999Z'); assert.equal(pool.status().available, 0);
  time++; assert.equal(pool.status().available, 2); assert.equal(pool.next(), entries[0]);
  pool.disable(entries[0]); time += 86400000 * 3; assert.equal(pool.status().available, 2);
});

test('SOCKS5 config resolves paths beside config, and a configured missing pool fails before listening', async t => {
  const dir = await directory(t), configFile = join(dir, 'config.json');
  await writeFile(configFile, JSON.stringify({ stateDir: '.', socks5File: './accounts.txt' }));
  const config = await loadConfig(configFile);
  assert.equal(config.socks5File, join(dir, 'accounts.txt'));
  await assert.rejects(startServers(config), { code: 'ENOENT' });
  assert.throws(() => createForwarder(config, { sessionIds: [session] }), /SOCKS5/);
  await writeFile(configFile, '{"socks5File":false}'); await assert.rejects(loadConfig(configFile), /socks5File/);
});

test('authenticated SOCKS5 forwards chat and both model routes with remote DNS, reading the file once', async t => {
  const requests = [];
  const target = await upstream(t, async (req, res) => {
    let text = ''; for await (const bytes of req) text += bytes;
    requests.push({ path: req.url, headers: req.headers, body: text }); res.end('{"ok":true}');
  });
  const auth = { username: 'test-user', password: 'test-password' };
  const proxy = await socksServer(t, { targetURL: target, auth, fragment: true });
  const dir = await directory(t), file = join(dir, 'socks5.txt');
  await writeFile(file, `127.0.0.1:${proxy.port}:${auth.username}:${auth.password}`);
  const pool = createProxyPool(await loadProxies(file)), service = await app(t, pool);
  await rm(file);
  const chat = await service.send(); assert.equal(chat.status, 200); await chat.text();
  for (const route of ['/v1/model', '/v1/models']) {
    const response = await fetch(`${service.local}${route}?revision=2`);
    assert.equal(response.status, 200); await response.text();
  }
  assert.equal(proxy.targets.length, 3);
  assert.ok(proxy.targets.every(item => item.host === 'remote.invalid' && item.port === 12345 && item.type === 3));
  assert.deepEqual(proxy.credentials, [auth, auth, auth]);
  assert.deepEqual(requests.map(item => item.path), ['/v1/chat/completions', '/models?revision=2', '/models?revision=2']);
  assert.ok(requests.every(item => item.headers.authorization === 'Bearer upstream-test-key'));
  assert.equal(JSON.parse(requests[0].body).stream, true);
  const health = await service.health(); assert.equal(health.proxyPool.available, 1);
  const visible = JSON.stringify({ logs: service.logs, health, requests });
  assert.ok(!visible.includes(auth.password)); assert.ok(!visible.includes(auth.username));
});

test('429 disables the selected proxy before queued work dispatches, without retry or direct fallback', async t => {
  const gate = Promise.withResolvers(); t.after(() => gate.resolve());
  let firstLimited = true, secondLimited = false, callsA = 0, callsB = 0;
  const targetA = await upstream(t, async (req, res) => {
    for await (const bytes of req) {}
    callsA++; await gate.promise;
    res.writeHead(firstLimited ? 429 : 200, { 'content-type': 'application/json', 'retry-after': '7' });
    res.end(firstLimited ? '{"error":"quota"}' : '{"source":"A"}');
  });
  const targetB = await upstream(t, async (req, res) => {
    for await (const bytes of req) {}
    callsB++; res.writeHead(secondLimited ? 429 : 200, { 'content-type': 'application/json' }); res.end('{"source":"B"}');
  });
  const proxyA = await socksServer(t, { targetURL: targetA }), proxyB = await socksServer(t, { targetURL: targetB });
  let time = Date.parse('2026-10-21T23:59:00Z');
  const pool = createProxyPool([proxyEntry(proxyA), proxyEntry(proxyB)], () => time);
  const service = await app(t, pool, { concurrency: 1 });
  const first = service.send(), second = service.send();
  for (let count = 0; count < 200 && (await service.health()).queued !== 1; count++) await delay(2);
  assert.equal((await service.health()).queued, 1); assert.equal(proxyB.connections.length, 0);
  gate.resolve(); const limited = await first;
  assert.equal(limited.status, 429); assert.equal(limited.headers.get('retry-after'), '7'); assert.equal(await limited.text(), '{"error":"quota"}');
  assert.equal((await second).status, 200); assert.equal(callsA, 1); assert.equal(callsB, 1);
  const healthy = await service.send(); await healthy.text(); assert.equal(callsA, 1); assert.equal(callsB, 2);
  secondLimited = true;
  const list = await fetch(`${service.local}/v1/models`); assert.equal(list.status, 429); await list.text();
  const exhausted = await service.send(); assert.equal(exhausted.status, 503);
  assert.equal(exhausted.headers.get('retry-after'), '60'); assert.equal((await exhausted.json()).error.type, 'proxy_pool_exhausted');
  assert.equal(callsA, 1); assert.equal(callsB, 3); assert.equal((await service.health()).proxyPool.available, 0);
  firstLimited = false; time = Date.parse('2026-10-22T00:00:00Z');
  const recovered = await service.send(); assert.equal(recovered.status, 200); await recovered.text();
  assert.equal((await service.health()).proxyPool.available, 2); assert.equal(callsA, 2);
});

test('SOCKS5 supports IPv4/IPv6 targets and domain/IPv6 bound replies', async t => {
  const target = await upstream(t, (req, res) => res.end('OK'));
  for (const [destination, expectedType, successReply] of [
    ['http://192.0.2.1:1234', 1, Buffer.from([5, 0, 0, 3, 3, 98, 110, 100, 0, 0])],
    ['http://[2001:db8::1]:2345', 4, Buffer.concat([Buffer.from([5, 0, 0, 4]), Buffer.alloc(18)])],
  ]) {
    const proxy = await socksServer(t, { targetURL: target, successReply, fragment: true });
    const socket = await openSocks5(proxyEntry(proxy), new URL(destination), AbortSignal.timeout(2000));
    assert.equal(proxy.targets[0].type, expectedType); assert.equal(proxy.targets[0].port, Number(new URL(destination).port));
    if (expectedType === 4) assert.equal(proxy.targets[0].host, '20010db8000000000000000000000001');
    socket.destroy();
  }
});

test('upstream non-429 error statuses do not disable proxy accounts', async t => {
  let status = 403;
  const target = await upstream(t, async (req, res) => {
    for await (const bytes of req) {}
    res.writeHead(status, { 'content-type': 'application/json' }); res.end('{"error":"upstream"}');
  });
  const proxy = await socksServer(t, { targetURL: target });
  const pool = createProxyPool([proxyEntry(proxy)]), service = await app(t, pool);
  for (status of [401, 403, 503]) {
    const response = await service.send(); assert.equal(response.status, status); await response.text();
    assert.equal(pool.status().available, 1);
  }
});

test('SOCKS5 rejects invalid negotiation, authentication, failed CONNECT and truncated replies', async t => {
  for (const options of [
    { greeting: Buffer.from([4, 0]) }, { greeting: Buffer.from([5, 2]) },
    { greeting: Buffer.from([5]), truncateGreeting: true },
    { auth: { username: 'expected', password: 'expected' } },
    { connectReply: Buffer.from([5, 5, 0, 1]) },
    { connectReply: Buffer.from([5, 0, 0, 9]) },
    { connectReply: Buffer.from([5, 0, 0, 1, 127]) },
  ]) {
    const proxy = await socksServer(t, options);
    const entry = proxyEntry(proxy, options.auth ? { username: 'wrong', password: 'private-password' } : {});
    await assert.rejects(openSocks5(entry, new URL('http://target.example'), AbortSignal.timeout(1000)),
      error => error.code === 'socks_proxy_error' && !error.message.includes('private-password'));
  }
});

test('SOCKS5 handshake timeout and client cancellation close sockets and release concurrency', async t => {
  const proxy = await socksServer(t, { stall: 'greeting' });
  const pool = createProxyPool([proxyEntry(proxy)]), service = await app(t, pool, { requestTimeoutMs: 120, concurrency: 1 });
  const response = await service.send(); assert.equal(response.status, 504); await response.text();
  for (let count = 0; count < 50 && !proxy.connections[0]?.destroyed; count++) await delay(2);
  assert.equal(proxy.connections[0].destroyed, true); assert.equal((await service.health()).active, 0);
  assert.equal(pool.status().available, 1);
  const controller = new AbortController(), pending = service.send(payload, controller.signal);
  const rejected = assert.rejects(pending);
  for (let count = 0; count < 50 && proxy.connections.length < 2; count++) await delay(2);
  controller.abort(); await rejected;
  for (let count = 0; count < 50 && (await service.health()).active; count++) await delay(2);
  assert.equal((await service.health()).active, 0); assert.equal(pool.status().available, 1);
});

test('HTTPS over SOCKS5 sends SNI, validates certificates and preserves SSE streaming', async t => {
  const cert = await readFile(new URL('./fixtures/localhost-cert.pem', import.meta.url));
  const key = await readFile(new URL('./fixtures/localhost-key.pem', import.meta.url));
  const gate = Promise.withResolvers(); t.after(() => gate.resolve());
  const target = await upstream(t, async (req, res) => {
    for await (const bytes of req) {}
    res.writeHead(200, { 'content-type': 'text/event-stream' }); res.write('data: {"first":true}\n\n');
    await gate.promise; res.end('data: [DONE]\n\n');
  }, { cert, key });
  const proxy = await socksServer(t, { targetURL: target });
  const pool = createProxyPool([proxyEntry(proxy)]);
  const service = await app(t, pool, { baseURL: `https://localhost:${new URL(target).port}/v1`, concurrency: 1 });
  const connect = tls.connect, seen = []; let trusted = false;
  t.mock.method(tls, 'connect', function (options, ...args) {
    seen.push(options); return connect.call(this, { ...options, ...(trusted ? { ca: cert } : {}) }, ...args);
  });
  const untrusted = await service.send(); assert.equal(untrusted.status, 502);
  assert.match((await untrusted.json()).error.type, /SELF_SIGNED_CERT|UNABLE_TO_VERIFY/);
  assert.equal(pool.status().available, 1);
  trusted = true;
  const stream = await service.send({ ...payload, stream: true }); assert.equal(stream.status, 200);
  const reader = stream.body.getReader();
  assert.match(new TextDecoder().decode((await reader.read()).value), /first/);
  assert.equal((await service.health()).active, 1);
  gate.resolve(); while (!(await reader.read()).done) {}
  assert.ok(seen.every(options => options.servername === 'localhost' && options.rejectUnauthorized === true));
  assert.equal((await service.health()).active, 0);
});
