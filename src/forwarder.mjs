import http from 'node:http';
import https from 'node:https';
import { isIP } from 'node:net';
import { randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { resolve, dirname } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { clientHeaders, tools } from './profile.mjs';
import { collectCompletion } from './sse.mjs';

export const defaults = {
  host: '127.0.0.1', port: 9999, concurrency: 4, stateDir: './state',
  baseURL: 'https://opencode.ai/inference/openai/v1',
  modelsURL: 'https://opencode.ai/inference/v1/models',
  requestTimeoutMs: 120000, maxBodyBytes: 8 * 1024 * 1024,
};

export const fault = (status, code, message) => Object.assign(new Error(message), { status, code });

export async function loadConfig(file = 'config.json') {
  const configFile = resolve(file);
  let custom = {};
  try { custom = JSON.parse(await readFile(configFile, 'utf8')); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  if (!custom || typeof custom !== 'object' || Array.isArray(custom)) throw new Error('配置必须为 JSON 对象');
  for (const key of Object.keys(custom)) if (!(key in defaults)) throw new Error(`未知配置项: ${key}，请按 config.example.json 更新配置`);
  const config = { ...defaults, ...custom };
  const hosts = Array.isArray(config.host) ? config.host : [config.host];
  if (!hosts.length || hosts.some(host => typeof host !== 'string' || (!isIP(host) && host !== 'localhost')))
    throw new Error('host 必须是 IP 地址、localhost 或这些地址组成的非空数组');
  config.host = [...new Set(hosts)];
  if (config.host.includes('0.0.0.0') && config.host.some(host => isIP(host) === 4 && host !== '0.0.0.0'))
    throw new Error('0.0.0.0 已覆盖所有 IPv4 地址，不要同时填写其他 IPv4 地址');
  for (const key of ['port', 'concurrency', 'requestTimeoutMs', 'maxBodyBytes'])
    if (!Number.isSafeInteger(config[key]) || config[key] < 1) throw new Error(`${key} 必须为正整数`);
  if (config.port > 65535) throw new Error('port 必须小于 65536');
  for (const key of ['baseURL', 'modelsURL']) {
    const url = new URL(config[key]);
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.hash || url.search)
      throw new Error(`${key} 必须是不含凭证、查询或片段的 HTTP(S) 地址`);
  }
  config.stateDir = resolve(dirname(configFile), config.stateDir);
  config.apiKey = process.env.ZEN_API_KEY || 'public';
  config.localApiKey = process.env.ZEN_LOCAL_API_KEY || '';
  if (/[\r\n]/.test(config.apiKey + config.localApiKey)) throw new Error('密钥包含非法字符');
  return config;
}

let lastTime = 0, counter = 0;
export function requestID() {
  const now = Date.now();
  counter = now === lastTime ? counter + 1 : 1;
  lastTime = now;
  const bytes = Buffer.alloc(6);
  bytes.writeUIntBE(Number((BigInt(now) * 4096n + BigInt(counter)) & 0xffffffffffffn), 0, 6);
  const alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
  return `msg_${bytes.toString('hex')}${[...randomBytes(14)].map(byte => alphabet[byte % 62]).join('')}`;
}

export async function sessions(config) {
  let text;
  try { text = await readFile(resolve(config.stateDir, 'sessions.txt'), 'utf8'); }
  catch (error) { if (error.code === 'ENOENT') return []; throw error; }
  const ids = [...new Set(text.replace(/^\uFEFF/, '').split(/\r?\n/).map(line => line.trim()).filter(line => line && !line.startsWith('#')))];
  if (ids.some(id => !/^ses_[0-9a-f]{12}[A-Za-z0-9]{14}$/.test(id)))
    throw fault(503, 'invalid_sessions', 'sessions.txt 存在格式无效的会话 ID');
  return ids;
}

export function readLimited(stream, limit, signal) {
  return new Promise((accept, reject) => {
    let length = 0;
    const chunks = [];
    const finish = error => {
      stream.off('data', onData); stream.off('end', onEnd); stream.off('error', finish);
      signal?.removeEventListener('abort', onAbort);
      if (error) { stream.pause(); reject(error); }
      else accept(Buffer.concat(chunks));
    };
    const onEnd = () => finish();
    const onAbort = () => finish(signal.reason);
    const onData = chunk => {
      length += chunk.length;
      if (length > limit) return finish(fault(413, 'body_too_large', `数据超过 ${limit} 字节限制`));
      chunks.push(chunk);
    };
    if (signal?.aborted) { reject(signal.reason); return; }
    stream.on('data', onData); stream.once('end', onEnd); stream.once('error', finish);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

function replyJSON(response, status, value) {
  response.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  response.end(JSON.stringify(value));
}

function responseHeaders(headers) {
  return Object.fromEntries(Object.entries(headers).filter(([key]) =>
    ['content-type', 'content-encoding', 'cache-control', 'retry-after', 'cf-ray', 'x-opencode-log-id'].includes(key)
    || key.startsWith('x-ratelimit-')));
}

export function createForwarder(config, { sessionIds, log = event => console.log(JSON.stringify(event)) } = {}) {
  if (!Array.isArray(sessionIds)) throw new Error('必须在监听之前加载会话列表');
  const pool = Object.freeze([...sessionIds]);
  const agents = {
    'http:': new http.Agent({ keepAlive: true, proxyEnv: {} }),
    'https:': new https.Agent({ keepAlive: true, proxyEnv: {} }),
  };
  const jobs = new Set();
  let head = null, tail = null, queued = 0, active = 0, sessionIndex = 0;
  let scheduled = false, stopping = false;

  function schedule() {
    if (scheduled || stopping) return;
    scheduled = true;
    queueMicrotask(drain);
  }

  function finish(job) {
    if (job.done) return;
    job.done = true;
    clearTimeout(job.timer);
    job.request.off('aborted', job.onDisconnect);
    job.response.off('close', job.onClose);
    job.aborter.signal.removeEventListener('abort', job.onAbort);
    if (job.node) { job.node.value = null; job.node = null; queued--; }
    job.payload = null;
    job.upstream?.destroy();
    if (job.running) { active--; job.running = false; }
    jobs.delete(job);
    log({ event: 'complete', trace: job.trace, status: job.response.statusCode, ms: Date.now() - job.started });
    schedule();
  }

  function fail(job, error) {
    if (job.done) return;
    const cause = job.aborter.signal.aborted ? job.aborter.signal.reason : error;
    const status = cause?.status || error.status || 502;
    const { request, response } = job;
    if (!response.headersSent && !request.readableEnded) { response.setHeader('connection', 'close'); request.resume(); }
    if (!response.headersSent && !response.destroyed)
      replyJSON(response, status, { error: { type: cause?.code || error.code || 'proxy_error', message: status === 502 ? '上游请求失败' : cause?.message || error.message } });
    else if (!response.destroyed) response.destroy();
    job.upstreamRequest?.destroy();
    finish(job);
  }

  function enqueue(job) {
    if (job.done) return;
    const node = { value: job, next: null };
    job.node = node;
    if (tail) tail.next = node;
    else head = node;
    tail = node;
    queued++;
    schedule();
  }

  function drain() {
    scheduled = false;
    while (!stopping && head && (!head.value || active < config.concurrency)) {
      const node = head;
      head = node.next;
      if (!head) tail = null;
      const job = node.value;
      node.value = null; node.next = null;
      if (!job) continue;
      job.node = null;
      queued--;
      job.running = true;
      active++;
      dispatch(job);
    }
  }

  function dispatch(job) {
    try {
      const protocol = job.target.protocol === 'https:' ? https : http;
      const outgoing = protocol.request(job.target, {
        method: job.chat ? 'POST' : 'GET', headers: job.headers,
        signal: job.aborter.signal, agent: agents[job.target.protocol],
      }, async upstream => {
        upstream.on('error', () => {});
        if (job.done) { upstream.destroy(); return; }
        job.upstream = upstream;
        const { response } = job;
        try {
          const headers = responseHeaders(upstream.headers);
          if (job.chat) {
            response.setHeader('x-zen-model', job.chat.model);
            response.setHeader('x-zen-attempts', '1');
            response.setHeader('x-zen-compatibility', job.chat.compatibility);
            response.setHeader('x-zen-upstream-stream', 'true');
          }
          if (job.chat && upstream.statusCode === 200 && !job.chat.stream && upstream.headers['content-type']?.includes('text/event-stream')) {
            const completion = await collectCompletion(upstream, { signal: job.aborter.signal, maxBytes: config.maxBodyBytes });
            if (job.done) return;
            delete headers['content-encoding'];
            response.writeHead(200, { ...headers, 'content-type': 'application/json; charset=utf-8' });
            await new Promise(accept => response.end(JSON.stringify(completion), accept));
          } else {
            if (job.chat?.stream) headers['x-accel-buffering'] = 'no';
            response.writeHead(upstream.statusCode, headers);
            response.flushHeaders();
            await pipeline(upstream, response);
          }
          finish(job);
        } catch (error) { fail(job, error); }
      });
      job.upstreamRequest = outgoing;
      outgoing.once('error', error => fail(job, error));
      outgoing.end(job.payload);
      job.payload = null;
    } catch (error) { fail(job, error); }
  }

  async function handle(request, response) {
    const job = { request, response, started: Date.now(), trace: randomUUID(), aborter: new AbortController(), done: false };
    job.onDisconnect = () => job.aborter.abort(fault(499, 'client_closed', '客户端已断开'));
    job.onClose = () => { if (!response.writableFinished) job.onDisconnect(); };
    job.onAbort = () => fail(job, job.aborter.signal.reason);
    job.timer = setTimeout(() => job.aborter.abort(fault(504, 'request_timeout', '排队或上游请求超过总时间预算')), config.requestTimeoutMs);
    jobs.add(job);
    request.on('error', () => {});
    request.once('aborted', job.onDisconnect);
    response.once('close', job.onClose);
    job.aborter.signal.addEventListener('abort', job.onAbort, { once: true });
    response.setHeader('x-zen-request-id', job.trace);
    try {
      if (config.localApiKey) {
        const actual = Buffer.from(request.headers.authorization || '');
        const expected = Buffer.from('Bearer ' + config.localApiKey);
        if (actual.length !== expected.length || !timingSafeEqual(actual, expected))
          throw fault(401, 'unauthorized', '本地 API 密钥无效');
      }
      if (stopping) throw fault(503, 'server_closing', '转发服务正在停止');
      const url = new URL(request.url, 'http://localhost');
      if (request.method === 'GET' && url.pathname === '/healthz') {
        replyJSON(response, 200, { ok: true, mode: 'serve', sessions: pool.length, active, queued, concurrency: config.concurrency });
        finish(job);
        return;
      }
      const models = request.method === 'GET' && ['/v1/model', '/v1/models'].includes(url.pathname);
      if (!models && !(request.method === 'POST' && url.pathname === '/v1/chat/completions'))
        throw fault(404, 'not_found', '支持 /healthz、/v1/model、/v1/models、/v1/chat/completions');
      if (models) {
        job.target = new URL(config.modelsURL);
        job.target.search = url.search;
        job.headers = { authorization: 'Bearer ' + config.apiKey, 'accept-encoding': 'identity', accept: 'application/json' };
        enqueue(job);
        return;
      }

      const bytes = await readLimited(request, config.maxBodyBytes, job.aborter.signal);
      if (job.done) return;
      let body;
      try { body = JSON.parse(bytes.toString()); } catch { throw fault(400, 'invalid_json', '请求体必须为 JSON'); }
      if (!body || typeof body !== 'object' || Array.isArray(body) || typeof body.model !== 'string' || !body.model.trim())
        throw fault(400, 'invalid_model', 'model 必须是非空字符串');
      if (!Array.isArray(body.messages) || !body.messages.length || body.messages.some(item => !item || typeof item.role !== 'string'))
        throw fault(400, 'invalid_messages', 'messages 必须是非空消息数组');
      if (body.stream !== undefined && typeof body.stream !== 'boolean') throw fault(400, 'invalid_stream', 'stream 必须为布尔值');
      if (body.stream_options !== undefined && (!body.stream_options || typeof body.stream_options !== 'object' || Array.isArray(body.stream_options)))
        throw fault(400, 'invalid_stream_options', 'stream_options 必须为对象');
      if (!pool.length) throw fault(503, 'session_missing', '请先在 sessions.txt 中准备会话 ID，再启动服务');
      const session = pool[sessionIndex];
      sessionIndex = (sessionIndex + 1) % pool.length;
      const injectTools = body.tools === undefined && body.tool_choice === undefined;
      const outgoing = { ...body, ...(injectTools ? { tools, tool_choice: 'none' } : {}),
        stream: true, stream_options: { include_usage: true, ...body.stream_options } };
      job.payload = Buffer.from(JSON.stringify(outgoing));
      job.target = new URL(config.baseURL.replace(/\/$/, '') + '/chat/completions');
      job.headers = { ...clientHeaders, authorization: 'Bearer ' + config.apiKey, 'x-opencode-request': requestID(),
        'x-opencode-session': session, 'x-opencode-session-id': session,
        'content-type': 'application/json', 'content-length': job.payload.length, 'accept-encoding': 'identity', accept: '*/*' };
      job.chat = { model: body.model, stream: body.stream === true, compatibility: injectTools ? 'builtin-tools-none' : 'passthrough' };
      enqueue(job);
    } catch (error) { fail(job, error); }
  }

  function stop() {
    if (stopping) return;
    stopping = true;
    for (const job of jobs) if (!job.running) fail(job, fault(503, 'server_closing', '转发服务正在停止'));
    head = tail = null;
  }

  return {
    createServer() {
      const server = http.createServer(handle);
      server.requestTimeout = config.requestTimeoutMs;
      server.headersTimeout = Math.min(10000, config.requestTimeoutMs);
      return server;
    },
    stop,
    close() {
      stop();
      for (const job of jobs) fail(job, fault(503, 'server_closing', '转发服务已停止'));
      for (const agent of Object.values(agents)) agent.destroy();
    },
  };
}

export async function startServers(config, options) {
  const sessionIds = await sessions(config);
  const forwarder = createForwarder(config, { ...options, sessionIds });
  const servers = [];
  async function close() {
    forwarder.stop();
    const timer = setTimeout(() => { for (const server of servers) server.closeAllConnections(); }, 2000);
    timer.unref();
    try { await Promise.all(servers.map(server => new Promise(accept => server.close(accept)))); }
    finally { clearTimeout(timer); await forwarder.close(); }
  }
  try {
    for (const host of Array.isArray(config.host) ? config.host : [config.host]) {
      const server = forwarder.createServer();
      servers.push(server);
      await new Promise((accept, reject) => {
        const onError = error => { server.off('listening', onListen); reject(error); };
        const onListen = () => { server.off('error', onError); accept(); };
        server.once('error', onError); server.once('listening', onListen);
        server.listen({ host, port: config.port, ipv6Only: isIP(host) === 6 });
      });
    }
  } catch (error) { await close(); throw error; }
  return { servers, close };
}
