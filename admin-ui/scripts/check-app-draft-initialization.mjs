import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const pagePath = path.resolve(scriptDir, '../src/pages/tenant/AppDetailPage.tsx');
const pageSource = await readFile(pagePath, 'utf8');

assert.match(
  pageSource,
  /onClick=\{\(\) => \{[\s\S]*?if \(draftForm\) \{\s*void createDraft\(\);\s*\} else \{\s*setDraftForm\(emptyDraftForm\(\)\);\s*\}[\s\S]*?\}\}/,
  'the create-draft action must initialize an empty form first and submit it on the next click',
);
assert.match(
  pageSource,
  /if \(!app \|\| !draftForm \|\| draftBusy\) \{\s*return;\s*\}/,
  'createDraft must keep its guard until the editor has a form value',
);

console.log('App draft initialization contract: ok');
