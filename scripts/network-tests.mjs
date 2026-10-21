import { spawn } from 'node:child_process';

const child = spawn(process.execPath, ['--test', 'test/forwarder.test.mjs', 'test/socks5.test.mjs'], {
  stdio: 'inherit', env: { ...process.env, ZEN_TEST_NETWORK: '1' },
});
child.once('error', error => { console.error(error.message); process.exitCode = 1; });
child.once('exit', code => { process.exitCode = code ?? 1; });
