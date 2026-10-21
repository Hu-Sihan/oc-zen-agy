import net from 'node:net';
import { Transform } from 'node:stream';
import { listen, close } from './virtual-http.mjs';

export async function socksServer(t, options = {}) {
  const sockets = new Set(), targets = [], credentials = [], connections = [];
  const server = net.createServer(socket => {
    sockets.add(socket); connections.push(socket);
    socket.on('error', () => {});
    socket.once('close', () => sockets.delete(socket));
    let buffer = Buffer.alloc(0), stage = 'greeting';
    function send(bytes, end = false) {
      if (options.fragment) {
        socket.write(bytes.subarray(0, 1));
        setImmediate(() => end ? socket.end(bytes.subarray(1)) : socket.write(bytes.subarray(1)));
      } else if (end) socket.end(bytes); else socket.write(bytes);
    }
    function consume(length) {
      const bytes = buffer.subarray(0, length); buffer = buffer.subarray(length); return bytes;
    }
    function receive(bytes) {
      buffer = Buffer.concat([buffer, bytes]);
      for (;;) {
        if (stage === 'greeting') {
          if (buffer.length < 2 || buffer.length < 2 + buffer[1]) return;
          consume(2 + buffer[1]);
          stage = options.auth ? 'auth' : 'connect';
          if (options.stall === 'greeting') { stage = 'stalled'; return; }
          send(options.greeting || Buffer.from([5, options.auth ? 2 : 0]), options.truncateGreeting);
        } else if (stage === 'auth') {
          if (buffer.length < 2 || buffer.length < 3 + buffer[1]) return;
          const userLength = buffer[1], passLength = buffer[2 + userLength];
          if (buffer.length < 3 + userLength + passLength) return;
          const bytes = consume(3 + userLength + passLength);
          const username = bytes.subarray(2, 2 + userLength).toString();
          const password = bytes.subarray(3 + userLength).toString();
          credentials.push({ username, password });
          const allowed = username === options.auth.username && password === options.auth.password;
          stage = allowed ? 'connect' : 'stalled';
          send(Buffer.from([1, allowed ? 0 : 1]), !allowed);
        } else if (stage === 'connect') {
          if (buffer.length < 5) return;
          const type = buffer[3], length = type === 1 ? 4 : type === 4 ? 16 : buffer[4];
          const offset = type === 3 ? 5 : 4;
          if (buffer.length < offset + length + 2) return;
          const bytes = consume(offset + length + 2), address = bytes.subarray(offset, offset + length);
          const host = type === 1 ? [...address].join('.') : type === 4 ? address.toString('hex') : address.toString();
          targets.push({ host, port: bytes.readUInt16BE(offset + length), type });
          stage = 'relay';
          if (options.connectReply) { send(options.connectReply, true); return; }
          if (options.stall === 'connect') return;
          const destination = new URL(options.targetURL);
          const peer = net.createConnection({ host: destination.hostname, port: Number(destination.port) });
          sockets.add(peer); peer.on('error', () => socket.destroy());
          socket.once('close', () => peer.destroy());
          peer.once('close', () => { sockets.delete(peer); socket.destroy(); });
          socket.pause(); socket.off('data', receive);
          peer.once('connect', () => {
            send(options.successReply || Buffer.from([5, 0, 0, 1, 127, 0, 0, 1, 0, 0]));
            if (buffer.length) peer.write(buffer);
            // A real wire delivers data asynchronously, including TLS handshake replies.
            const wire = () => new Transform({ transform(bytes, encoding, callback) { setImmediate(callback, null, bytes); } });
            const outgoing = wire(), incoming = wire();
            outgoing.on('error', () => socket.destroy()); incoming.on('error', () => socket.destroy());
            socket.once('close', () => { outgoing.destroy(); incoming.destroy(); });
            socket.pipe(outgoing).pipe(peer); peer.pipe(incoming).pipe(socket); socket.resume();
          });
          return;
        } else return;
      }
    }
    socket.on('data', receive);
  });
  const url = await listen(server);
  t.after(async () => { for (const socket of sockets) socket.destroy(); await close(server); });
  return { url, host: '127.0.0.1', port: Number(new URL(url).port), targets, credentials, connections };
}
