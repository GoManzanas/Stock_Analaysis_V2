import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import { resolve } from 'path';
import {
  copyFileSync,
  mkdirSync,
  existsSync,
  readdirSync,
  rmSync,
  readFileSync,
  writeFileSync,
} from 'fs';
import { build } from 'vite';

function copyManifestAndAssets() {
  return {
    name: 'copy-manifest-and-assets',
    async closeBundle() {
      const distDir = resolve(__dirname, 'dist');

      // Copy manifest.json
      copyFileSync(resolve(__dirname, 'manifest.json'), resolve(distDir, 'manifest.json'));

      // Copy icons
      const iconsDir = resolve(__dirname, 'public/icons');
      const distIconsDir = resolve(distDir, 'icons');
      if (!existsSync(distIconsDir)) {
        mkdirSync(distIconsDir, { recursive: true });
      }
      if (existsSync(iconsDir)) {
        for (const file of readdirSync(iconsDir)) {
          copyFileSync(resolve(iconsDir, file), resolve(distIconsDir, file));
        }
      }

      // Move popup HTML from dist/src/popup/ to dist/popup/ and fix asset paths
      const srcPopupHtml = resolve(distDir, 'src/popup/index.html');
      const destPopupDir = resolve(distDir, 'popup');
      const destPopupHtml = resolve(destPopupDir, 'index.html');
      if (existsSync(srcPopupHtml)) {
        mkdirSync(destPopupDir, { recursive: true });
        let html = readFileSync(srcPopupHtml, 'utf-8');
        html = html.replace(/="\.\.\/\.\.\//g, '="../');
        html = html.replace(/="\.\.\/popup\//g, '="./');
        writeFileSync(destPopupHtml, html);
      }

      // Clean up dist/src/ after moving HTML pages
      if (existsSync(resolve(distDir, 'src'))) {
        rmSync(resolve(distDir, 'src'), { recursive: true, force: true });
      }

      // Build content script as IIFE (content scripts cannot use ES modules in MV3)
      await build({
        configFile: false,
        publicDir: false,
        resolve: {
          alias: {
            '@': resolve(__dirname, 'src'),
          },
        },
        build: {
          outDir: resolve(distDir, 'content'),
          emptyOutDir: true,
          lib: {
            entry: resolve(__dirname, 'src/content/index.ts'),
            formats: ['iife'],
            name: 'ExtensionContent',
            fileName: () => 'index.js',
          },
          rollupOptions: {
            output: {
              extend: true,
            },
          },
          target: 'esnext',
          minify: false,
        },
      });
    },
  };
}

export default defineConfig({
  plugins: [react(), tailwindcss(), copyManifestAndAssets()],
  resolve: {
    alias: {
      '@': resolve(__dirname, 'src'),
    },
  },
  base: './',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      input: {
        background: resolve(__dirname, 'src/background/index.ts'),
        popup: resolve(__dirname, 'src/popup/index.html'),
      },
      output: {
        entryFileNames: (chunkInfo) => {
          return `${chunkInfo.name}/index.js`;
        },
        chunkFileNames: 'chunks/[name]-[hash].js',
        assetFileNames: 'assets/[name]-[hash].[ext]',
      },
    },
    target: 'esnext',
    minify: false,
  },
});
