import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const panelPath = path.resolve(scriptDir, '../src/components/ResourceListPanel.tsx');
const panelSource = await readFile(panelPath, 'utf8');

assert.match(
  panelSource,
  /pageSize=\{list\.pageSize\}[\s\S]*hasMore=\{list\.hasMore\}[\s\S]*onPageChange=\{list\.changePage\}/,
  'ResourceListPanel must forward hasMore from useCursorResourceList to ResourceTable',
);

console.log('ResourceListPanel pagination contract: ok');
