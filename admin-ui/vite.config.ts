import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(dirname, '..');
const tdesignComponents = path.join(repoRoot, 'third_party/tdesign-react/packages/components');

// Bare dependencies imported by the in-repo TDesign source. The submodule has
// no node_modules of its own, so resolve them into admin-ui/node_modules.
// The (?=/|$) lookahead keeps e.g. /^react/ from matching react-router-dom.
const tdesignRuntimeDeps = [
  'react',
  'react-dom',
  'react-is',
  'react-transition-group',
  'classnames',
  'dayjs',
  'lodash-es',
  'mitt',
  'raf',
  'react-fast-compare',
  'hoist-non-react-statics',
  'sortablejs',
  'validator',
  'tslib',
  'tinycolor2',
  'prop-types',
  '@popperjs/core',
  'tdesign-icons-react',
];

const escapeRegExp = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

// The Admin UI consumes the in-repo TDesign submodule source directly
// (third_party/tdesign-react/packages/components) instead of the npm package.
export default defineConfig({
  plugins: [react()],
  // Production static hosting mounts the SPA at /admin; keep Vite's root
  // asset paths for the development server's existing /connect entry.
  base: process.env.NODE_ENV === 'production' ? '/admin/' : '/',
  resolve: {
    alias: [
      {
        find: /^tdesign-react\/es\/locale\/(.*)$/,
        replacement: path.join(tdesignComponents, 'locale/$1.ts'),
      },
      {
        find: /^tdesign-react$/,
        replacement: path.join(tdesignComponents, 'index.ts'),
      },
      {
        // Used by the in-repo TDesign source (e.g. locale files).
        find: /^@tdesign\/common-js/,
        replacement: path.join(repoRoot, 'third_party/tdesign-react/packages/common/js'),
      },
      ...tdesignRuntimeDeps.map((dep) => ({
        find: new RegExp(`^${escapeRegExp(dep)}(?=/|$)`),
        replacement: path.join(dirname, 'node_modules', dep),
      })),
      { find: '@', replacement: path.join(dirname, 'src') },
    ],
    dedupe: ['react', 'react-dom', 'react-is', 'react-transition-group'],
  },
  define: {
    __VERSION__: JSON.stringify('1.18.2+repo'),
  },
  optimizeDeps: {
    exclude: ['tdesign-react'],
  },
  server: {
    port: 5173,
    fs: {
      // Allow Vite to serve the TDesign submodule outside admin-ui/.
      allow: [repoRoot],
    },
    proxy: {
      // Dev convenience: same-origin proxy to a locally running gateway so the
      // browser never needs CORS. Override with VITE_API_TARGET.
      '/admin': {
        target: process.env.VITE_API_TARGET || 'http://127.0.0.1:8080',
        // Keep the browser Host for local same-origin CSRF checks. Production
        // proxies should forward the public Host and protocol as well.
        changeOrigin: false,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
    chunkSizeWarningLimit: 4096,
  },
});
