import net from 'node:net';
import tls from 'node:tls';
import { readFile } from 'node:fs/promises';

const DAY = 86400000;
const socksError = message => Object.assign(new Error(message), { code: 'socks_proxy_error' });

export function parseProxies(text) {
  const unique = new Map();
  for (const [index, source] of text.replace(/^\uFEFF/, '').split(/\r?\n/).entries()) {
    const line = source.trim();
    if (!line || line.startsWith('#')) continue;
    try {
      let host, port, username = '', password = '';
      if (/^socks5h?:\/\//i.test(line)) {
        const url = new URL(line);
        if (!['socks5:', 'socks5h:'].includes(url.protocol) || url.search || url.hash || !['', '/'].includes(url.pathname)) throw new Error();
        host = url.hostname;
        port = Number(url.port || 1080);
        username = decodeURIComponent(url.username);
        password = decodeURIComponent(url.password);
      } else {
        const match = /^(\[[^\]]+\]|[^:\s]+):(\d+)(?::([^:]+):(.*))?$/.exec(line);
        if (!match) throw new Error();
        [, host, port, username = '', password = ''] = match;
        port = Number(port);
      }
      const address = new URL(`http://${host}`);
      if (!address.hostname || address.username || address.password || address.port || address.pathname !== '/' || address.search || address.hash) throw new Error();
      host = address.hostname.replace(/^\[|\]$/g, '');
      if (!Number.isInteger(port) || port < 1 || port > 65535 || (!username && password)) throw new Error();
      if (username && [username, password].some(value => !Buffer.byteLength(value) || Buffer.byteLength(value) > 255 || /[\r\n\0]/.test(value))) throw new Error();
      const proxy = Object.freeze({ host, port, username, password });
      unique.set(JSON.stringify(proxy), proxy);
    } catch {
      throw new Error(`SOCKS5 号池第 ${index + 1} 行格式无效，要求 host:port[:username:password] 或 socks5:// 地址`);
    }
  }
  if (!unique.size) throw new Error('SOCKS5 号池为空');
  return [...unique.values()];
}

export async function loadProxies(file) {
  return parseProxies(await readFile(file, 'utf8'));
}

export function createProxyPool(proxies, now = Date.now) {
  const entries = Object.freeze([...proxies]), disabled = new Set();
  let day = Math.floor(now() / DAY), cursor = 0;
  function refresh() {
    const time = now(), today = Math.floor(time / DAY);
    if (today !== day) { disabled.clear(); day = today; }
    return time;
  }
  return {
    next() {
      const time = refresh();
      for (let count = 0; count < entries.length; count++) {
        const proxy = entries[cursor];
        cursor = (cursor + 1) % entries.length;
        if (!disabled.has(proxy)) return proxy;
      }
      throw Object.assign(new Error('SOCKS5 号池已全部停用，等待 UTC 00:00 重置'), {
        status: 503, code: 'proxy_pool_exhausted', retryAfter: Math.max(1, Math.ceil(((day + 1) * DAY - time) / 1000)),
      });
    },
    disable(proxy) { refresh(); disabled.add(proxy); },
    status() {
      refresh();
      return { total: entries.length, available: entries.length - disabled.size, disabled: disabled.size,
        resetAt: new Date((day + 1) * DAY).toISOString() };
    },
  };
}

function waitFor(socket, event, signal) {
  return new Promise((accept, reject) => {
    function finish(error) {
      socket.off(event, ready); socket.off('error', finish); socket.off('close', closed); socket.off('end', closed);
      signal?.removeEventListener('abort', aborted);
      if (error) reject(error); else accept();
    }
    const ready = () => finish();
    const closed = () => finish(socksError('SOCKS5 连接提前关闭'));
    const aborted = () => finish(signal.reason);
    socket.once(event, ready); socket.once('error', finish); socket.once('close', closed); socket.once('end', closed);
    signal?.addEventListener('abort', aborted, { once: true });
    if (signal?.aborted) aborted();
    else if (socket.destroyed || socket.readableEnded) closed();
  });
}

async function readBytes(socket, size, signal) {
  const parts = [];
  let length = 0;
  while (length < size) {
    signal?.throwIfAborted();
    const bytes = socket.read();
    if (bytes) {
      const used = Math.min(bytes.length, size - length);
      parts.push(bytes.subarray(0, used)); length += used;
      if (used < bytes.length) socket.unshift(bytes.subarray(used));
    }
    else await waitFor(socket, 'readable', signal);
  }
  return Buffer.concat(parts, size);
}

function destination(target) {
  const host = target.hostname.replace(/^\[|\]$/g, '');
  const family = net.isIP(host);
  let address;
  if (family === 4) address = Buffer.from([1, ...host.split('.').map(Number)]);
  else if (family === 6) {
    const [left, right = ''] = host.split('::');
    const first = left ? left.split(':') : [], last = right ? right.split(':') : [];
    const words = [...first, ...Array(8 - first.length - last.length).fill('0'), ...last];
    address = Buffer.alloc(17); address[0] = 4;
    words.forEach((word, index) => address.writeUInt16BE(parseInt(word, 16), 1 + index * 2));
  } else {
    const bytes = Buffer.from(host);
    if (!bytes.length || bytes.length > 255) throw socksError('SOCKS5 目标域名长度无效');
    address = Buffer.concat([Buffer.from([3, bytes.length]), bytes]);
  }
  const port = Buffer.alloc(2);
  port.writeUInt16BE(Number(target.port || (target.protocol === 'https:' ? 443 : 80)));
  return { host, request: Buffer.concat([Buffer.from([5, 1, 0]), address, port]) };
}

// RFC 1928 CONNECT with optional RFC 1929 authentication; destination DNS stays at the proxy.
export async function openSocks5(proxy, target, signal) {
  signal?.throwIfAborted();
  const { host, request } = destination(target);
  const socket = net.createConnection({ host: proxy.host, port: proxy.port });
  let connection = socket;
  const abort = () => { connection.destroy(); socket.destroy(); };
  socket.on('error', () => {});
  signal?.addEventListener('abort', abort, { once: true });
  try {
    await waitFor(socket, 'connect', signal);
    const method = proxy.username ? 2 : 0;
    socket.write(Buffer.from([5, 1, method]));
    const greeting = await readBytes(socket, 2, signal);
    if (greeting[0] !== 5 || greeting[1] !== method) throw socksError('SOCKS5 认证方法协商失败');
    if (method === 2) {
      const user = Buffer.from(proxy.username), pass = Buffer.from(proxy.password);
      socket.write(Buffer.concat([Buffer.from([1, user.length]), user, Buffer.from([pass.length]), pass]));
      const auth = await readBytes(socket, 2, signal);
      if (auth[0] !== 1 || auth[1] !== 0) throw socksError('SOCKS5 用户名或密码认证失败');
    }
    socket.write(request);
    const reply = await readBytes(socket, 4, signal);
    if (reply[0] !== 5 || reply[1] !== 0 || reply[2] !== 0) throw socksError('SOCKS5 无法连接目标');
    let length;
    if (reply[3] === 1) length = 4;
    else if (reply[3] === 4) length = 16;
    else if (reply[3] === 3) length = (await readBytes(socket, 1, signal))[0];
    else throw socksError('SOCKS5 返回了无效的地址类型');
    await readBytes(socket, length + 2, signal);
    if (target.protocol === 'https:') {
      connection = tls.connect({ socket, host, servername: net.isIP(host) ? undefined : host, rejectUnauthorized: true, ALPNProtocols: ['http/1.1'] });
      connection.on('error', () => {});
      await waitFor(connection, 'secureConnect', signal);
    }
    signal?.throwIfAborted();
    return connection;
  } catch (error) {
    connection.destroy(); socket.destroy();
    throw error;
  } finally { signal?.removeEventListener('abort', abort); }
}
