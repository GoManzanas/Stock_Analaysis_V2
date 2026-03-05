# Learnings

## Chrome Extension MV3

- Service workers terminate after ~30s of inactivity. Never store state only in memory — always persist to `chrome.storage.local` or IndexedDB
- Content scripts cannot use ES modules in MV3. Build them as IIFE bundles via Vite's lib mode
- Vite preserves source paths for HTML entries (popup ends up at `dist/src/popup/`). The post-build plugin relocates and fixes paths
- `jsdom` must be explicitly installed for Vitest's jsdom environment — it's not bundled
- `@types/chrome` provides type definitions for all Chrome APIs. Include `"chrome"` in tsconfig `types` array
- Use `chrome.alarms` instead of `setInterval` in service workers — intervals don't survive SW termination
- Content scripts should use an idempotency guard (`window.__extension_initialized__`) to prevent double-running on SPA navigations

## Vite Multi-Entry Build

- Main build handles background (ES module) and popup (HTML entry)
- Each content script needs a separate IIFE sub-build in the `closeBundle` hook
- Adding a new content script: add another `build()` call in the plugin, add to `manifest.json` content_scripts array
