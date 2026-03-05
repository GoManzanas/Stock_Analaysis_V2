# Project: Stub Extension

## First Run — Customize This Scaffold

This is a generic Chrome MV3 extension scaffold. **On the first development session**, the agent should:

1. Ask the user what the extension does (purpose, target users, key features)
2. Update `manifest.json`: name, description, permissions, host_permissions, content_scripts match patterns
3. Update this file: replace "Stub Extension" with the real name, write the Overview and Architecture sections
4. Update `context/architecture/concepts.md` with actual entities and data flow
5. Update `.claude/rules/code-conventions.md` with extension-specific patterns
6. Add additional content scripts or execution contexts as needed (e.g., YouTube-specific script, offscreen document)

## Overview

A Chrome browser extension (Manifest V3). Built with React 19, TypeScript, Vite 6, and Tailwind CSS v4.

## Architecture

Organized by **execution context** (not feature-first), since Chrome extensions have isolated JavaScript realms:

- **Background Service Worker** (`src/background/`): Central coordinator — handles messages, manages state via chrome.storage.local
- **Content Script** (`src/content/`): Injected into web pages — extracts data, injects UI
- **Popup** (`src/popup/`): React 19 UI accessible from the extension icon
- **Shared** (`src/shared/`): Types, messaging helpers, constants — used across all contexts

## Key Files

| Path | Purpose |
|------|---------|
| `src/background/index.ts` | Service worker: message handling, storage |
| `src/content/index.ts` | Content script: page interaction |
| `src/popup/` | React popup UI |
| `src/shared/types/messages.ts` | Discriminated union message types |
| `src/shared/messaging.ts` | Type-safe messaging wrappers |
| `src/shared/constants.ts` | Shared constants and storage keys |
| `src/__tests__/` | Test setup and Chrome API mocks |
| `manifest.json` | Chrome MV3 extension manifest |
| `vite.config.ts` | Vite multi-entry build (ESM + IIFE) |
| `context/architecture/` | Architecture docs, conventions |
| `context/product/` | Decisions, plans |
| `context/process/` | Learnings, retrospectives |

## Development

```bash
pnpm install          # Install dependencies
pnpm dev              # Watch mode build (vite build --watch)
pnpm build            # Production build (tsc + vite)
pnpm test             # Run tests (vitest)
pnpm test:watch       # Watch mode tests
pnpm lint             # ESLint
pnpm format           # Prettier
pnpm typecheck        # TypeScript type checking
```

To test: load `dist/` as an unpacked extension in `chrome://extensions/`.

## Testing

### Unit Tests (Vitest + jsdom)

Tests mock at the **Chrome messaging boundary** — never mock Chrome internals directly in component tests.

Pattern: Mock `chrome.runtime.sendMessage`, `chrome.storage.local`, and `chrome.tabs` — then test handler logic and UI behavior.

See `.claude/rules/testing-strategy.md` for the full testing strategy.

## Conventions

### Before Making Changes

- Read the relevant file(s) first
- Check `context/product/decisions.md` for prior decisions on the topic
- Check `context/process/learnings.md` for known gotchas
- Use `context7` tool to verify library syntax for React 19, Tailwind v4, Chrome Extension APIs, Vite 6

### After Making Changes

- If the change involved a non-obvious decision, log it in `context/product/decisions.md`
- If we learned something useful, add it to `context/process/learnings.md`

### Code Style

- Organize by execution context: background, content, popup, shared
- Type-safe messaging: use discriminated unions and `sendToBackground()`/`sendToContent()` helpers
- No raw `chrome.runtime.sendMessage` calls — always go through typed wrappers
- Service worker: no DOM access, persist all state to `chrome.storage.local`
- Tailwind CSS v4 for all styling in popup
- Use `@/` path alias for imports
- Use mermaid diagrams for all diagrams

@context/architecture/concepts.md
@context/architecture/conventions.md
@context/process/learnings.md
