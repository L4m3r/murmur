import { spawn, spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { resolve, dirname, delimiter } from 'node:path';
import { fileURLToPath } from 'node:url';
const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const candidates = [process.env.CELLD_BIN, resolve(root, '.tools/bin/celld'), resolve(root, '../celld-tools-v063/bin/celld'), resolve(root, '../celld-tools/bin/celld')].filter(Boolean);
const bin = candidates.find(existsSync) || 'celld';
const version = spawnSync(bin, ['--version'], {encoding:'utf8'});
const found = /(?:^|\s)v?(\d+)\.(\d+)\.(\d+)/.exec(version.stdout || '');
if(version.status!==0 || !found || (Number(found[1])===0 && (Number(found[2])<6 || (Number(found[2])===6 && Number(found[3])<3)))) {
  console.error('celld >=0.6.3 is required (ownership-loss safety fix). Run npm run setup:celld.');
  process.exit(1);
}
const child = spawn(bin, ['dev', root, '--host', '127.0.0.1', '--port', process.env.PORT || '9876', ...process.argv.slice(2)], {
  cwd: root, stdio: 'inherit', env: {...process.env, PATH: resolve(root, 'node_modules/.bin') + delimiter + process.env.PATH}
});
child.on('error', error => { console.error(`${error.message}. Run npm run setup:celld first.`); process.exitCode=1; });
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, () => child.kill(sig));
child.on('exit', code => { process.exitCode = code ?? 0; });
