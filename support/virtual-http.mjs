import http from 'node:http';
import net from 'node:net';
import { duplexPair, Readable } from 'node:stream';
import { mock } from 'node:test';

// Real Node HTTP parsers over in-memory duplex streams; no OS socket is opened.
const servers = new Map();
let port = 20000;
const realNetwork = process.env.ZEN_TEST_NETWORK === '1';
function virtualSocket(options) {
  const entry = servers.get(Number(options.port));
  if (!entry) throw new Error(`Unexpected virtual HTTP destination: ${options.host}:${options.port}`);
  const [client, server] = duplexPair();
  for (const socket of [client, server]) {
    let timer;
    socket.setTimeout = function (ms, callback) {
      clearTimeout(timer);
      if (callback) socket.once('timeout', callback);
      if (ms) { timer = setTimeout(() => socket.emit('timeout'), ms); timer.unref(); }
      return socket;
    };
    socket.setNoDelay = socket.setKeepAlive = socket.ref = socket.unref = () => socket;
    socket.on('error', () => {});
    socket.once('close', () => { clearTimeout(timer); entry.sockets.delete(socket); });
    entry.sockets.add(socket);
  }
  client.once('close', () => server.destroy());
  server.once('close', () => client.destroy());
  entry.server.emit('connection', server);
  return client;
}
if (!realNetwork) {
  const connect = mock.method(net, 'createConnection', function (options, callback) {
    const socket = virtualSocket(options);
    if (callback) socket.once('connect', callback);
    queueMicrotask(() => socket.emit('connect'));
    return socket;
  });
  net.connect = connect;
}

export async function listen(server) {
  if (realNetwork) {
    await new Promise((accept, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', accept); });
    return `http://127.0.0.1:${server.address().port}`;
  }
  const id = ++port;
  servers.set(id, { server, sockets: new Set() });
  return `http://127.0.0.1:${id}`;
}

export async function close(server) {
  if (realNetwork) {
    server.closeAllConnections();
    await new Promise(accept => server.close(accept));
    return;
  }
  for (const [id, entry] of servers) {
    if (entry.server !== server) continue;
    for (const socket of entry.sockets) socket.destroy();
    servers.delete(id);
  }
  server.emit('close');
}

export function fetch(url, options = {}) {
  if (realNetwork) return globalThis.fetch(url, options);
  return new Promise((accept, reject) => {
    const request = http.request(url, {
      method: options.method || 'GET', headers: options.headers, signal: options.signal,
      agent: new http.Agent({ keepAlive: false, proxyEnv: {} }),
    }, response => accept(new Response(Readable.toWeb(response), { status: response.statusCode, headers: response.headers })));
    request.once('error', reject);
    request.end(options.body);
  });
}
