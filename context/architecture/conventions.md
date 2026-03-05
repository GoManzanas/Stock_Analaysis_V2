# Extension Conventions

## Package Manager

- **pnpm** is the package manager. Use `pnpm add`, `pnpm install`, `pnpm dev`, etc.

## Styling

- **Tailwind CSS v4** integrated via `@tailwindcss/vite` plugin in `vite.config.ts`
- No `tailwind.config.js` — Tailwind v4 uses CSS-based configuration
- Import in popup CSS: `@import 'tailwindcss';`

## Linting & Formatting

- **ESLint 9** with flat config (`eslint.config.js`) + typescript-eslint
- **Prettier** for formatting (`.prettierrc`)

## Path Aliases

- `@/*` resolves to `./src/*` (configured in both `tsconfig.json` and `vite.config.ts`)

## Environment Variables

- **None.** Chrome extensions cannot use env vars at runtime. All configuration is stored in `chrome.storage.local`.

## Build System

- **Vite 6** with a multi-entry build strategy:
  - Main build: `src/background/index.ts` (ES module) + `src/popup/index.html` (HTML entry)
  - IIFE sub-build in `closeBundle` hook: `src/content/index.ts`
  - Content scripts must be IIFE — Chrome MV3 does not support ES modules in content scripts
- Post-build plugin handles: manifest copying, icon copying, HTML path fixing

## Chrome API Conventions

- Prefer Promise-based Chrome APIs over callbacks
- Handle service worker termination gracefully — persist state before any async operation
- Type-safe messaging via discriminated unions (see `src/shared/types/messages.ts`)
- Use `chrome.alarms` for periodic tasks, not `setInterval` (SW may be terminated)
- Content scripts: use idempotency guards (`window.__extension_initialized__`)

## Testing

- **Vitest 3** with jsdom environment
- Global chrome mock in `src/__tests__/mocks/chrome.ts`
- Tests run with `pnpm test`

## Git Conventions

- Branch naming: descriptive slugs
- Commit messages: explain _why_, not just _what_
