import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { loadConfig, startServers } from './forwarder.mjs';

export async function main(args = process.argv.slice(2)) {
  const [command = 'serve', ...options] = args;
  if (command === '--help' || command === 'help') {
    console.log('用法: node src/server.mjs [serve|probe] [--config config.json]');
    return;
  }
  if (options.length && (options.length !== 2 || options[0] !== '--config')) throw new Error('仅支持 --config <文件>');
  const [major, minor] = process.versions.node.split('.').map(Number);
  if (major < 24 || (major === 24 && minor < 5)) throw new Error('请使用 Node.js 24.5 或更高版本');
  const config = await loadConfig(options[1] || 'config.json');
  if (command === 'serve') {
    const app = await startServers(config);
    for (const host of config.host) console.error(`转发服务: http://${host.includes(':') ? `[${host}]` : host}:${config.port}/v1`);
    let stopping = false;
    const stop = () => {
      if (stopping) return;
      stopping = true;
      app.close().catch(error => { console.error(error.message); process.exitCode = 1; });
    };
    process.once('SIGINT', stop); process.once('SIGTERM', stop);
    return app;
  }
  if (command !== 'probe') throw new Error(`未知命令: ${command}`);
  let host = config.host[0];
  if (host === '0.0.0.0') host = '127.0.0.1';
  if (host === '::') host = '::1';
  const response = await fetch(`http://${host.includes(':') ? `[${host}]` : host}:${config.port}/v1/chat/completions`, {
    method: 'POST', headers: { 'content-type': 'application/json', ...(config.localApiKey ? { authorization: `Bearer ${config.localApiKey}` } : {}) },
    body: JSON.stringify({ model: process.env.ZEN_PROBE_MODEL || 'big-pickle', messages: [{ role: 'user', content: 'Reply only OK.' }], max_tokens: 256, stream: false }),
    signal: AbortSignal.timeout(config.requestTimeoutMs + 1000),
  });
  console.log(JSON.stringify({ status: response.status, actualModel: response.headers.get('x-zen-model'),
    compatibility: response.headers.get('x-zen-compatibility'), trace: response.headers.get('x-zen-request-id'), body: (await response.text()).slice(0, 4096) }, null, 2));
  if (!response.ok) process.exitCode = 1;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href)
  main().catch(error => { console.error(error.message); process.exitCode = 1; });
