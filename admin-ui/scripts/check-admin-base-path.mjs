import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const mainSource = await readFile(path.resolve(scriptDir, '../src/main.tsx'), 'utf8');
const appSource = await readFile(path.resolve(scriptDir, '../src/App.tsx'), 'utf8');
const shellSource = await readFile(path.resolve(scriptDir, '../src/components/AppShell.tsx'), 'utf8');
const viteSource = await readFile(path.resolve(scriptDir, '../vite.config.ts'), 'utf8');

assert.match(
  mainSource,
  /<BrowserRouter basename=\{window\.location\.pathname === '\/admin' \|\| window\.location\.pathname\.startsWith\('\/admin\/'\) \? '\/admin' : undefined\}/,
  'BrowserRouter must preserve the documented /admin deployment mount',
);
assert.match(appSource, /<Route path="\/login"/);
assert.doesNotMatch(appSource, /path="\/admin\/login"/);
assert.doesNotMatch(shellSource, /navigate\('\/admin\/login'\)/);
assert.match(viteSource, /base: process\.env\.NODE_ENV === 'production' \? '\/admin\/' : '\/'/);
assert.match(viteSource, /['"]\/admin\/auth['"][\s\S]*['"]\/admin\/v1['"]/);
assert.doesNotMatch(viteSource, /proxy:\s*\{\s*['"]\/admin['"]\s*:/);

console.log('Admin base-path routing contract: ok');
