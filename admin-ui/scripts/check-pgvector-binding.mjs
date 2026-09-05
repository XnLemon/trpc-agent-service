import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const editorPath = path.resolve(scriptDir, '../src/components/BindingsEditor.tsx');
const detailPath = path.resolve(scriptDir, '../src/pages/tenant/BackendDetailPage.tsx');
const docsPath = path.resolve(scriptDir, '../../docs/docs/admin-web-ui.md');
const editorSource = await readFile(editorPath, 'utf8');
const detailSource = await readFile(detailPath, 'utf8');
const docsSource = await readFile(docsPath, 'utf8');

for (const key of ['schema', 'collection', 'embedding_model', 'embedding_version', 'dimension', 'queue_size', 'workers', 'max_attempts']) {
  assert.match(editorSource, new RegExp(`['"]${key}['"]`), `pgvector editor must expose ${key}`);
}
assert.match(editorSource, /provider\.trim\(\)\.toLowerCase\(\) === 'pgvector'/);
assert.match(editorSource, /min=\{1\} max=\{2000\}/, 'dimension must use the server dimension bounds');
assert.match(editorSource, /min=\{1\} max=\{10000\}/, 'queue size must use the server queue bounds');
assert.match(editorSource, /disabled=\{isPGVectorKnowledge\(binding\)\}/, 'pgvector must not accept a tenant secret reference');
assert.match(detailSource, /pending → ready/);
assert.match(detailSource, /dead-letter/);
assert.match(detailSource, /相似度排序前应用授权过滤/);
assert.match(docsSource, /Backend Profile 中的 `knowledge` 能力如果选择 `pgvector`/);

console.log('pgvector binding UI contract: ok');
