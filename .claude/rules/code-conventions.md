---
alwaysApply: true
---

# Core Architecture & Code Conventions

## Architecture Overview

This is a Chrome MV3 extension organized by **execution context** — each directory under `src/` corresponds to an isolated JavaScript realm:

```
Content Script (data) → Background SW (process) → Content Script (response)
Popup (settings) → Background SW (storage) → chrome.storage.local
```

1. **Context-Based Organization (CRITICAL):**
   - `src/background/` — Service worker code. No DOM access. Owns all chrome.storage and core logic.
   - `src/content/` — Content script. Injected into web pages. Extracts data, injects UI.
   - `src/popup/` — React 19 UI. Extension popup interface.
   - `src/shared/` — Types, messaging helpers, constants. Imported by all contexts.

2. **Messaging Rules:**
   - **ALWAYS** use typed messaging wrappers: `sendToBackground()`, `sendToContent()`
   - **NEVER** call `chrome.runtime.sendMessage` or `chrome.tabs.sendMessage` directly
   - All message types are defined as a discriminated union in `src/shared/types/messages.ts`
   - Message handlers in background use a switch statement on `message.type`

3. **Service Worker Constraints:**
   - No DOM access (no `document`, no `window`)
   - Terminates after ~30s of inactivity — persist ALL state to `chrome.storage.local`
   - Use `chrome.alarms` for periodic tasks, not `setInterval`

4. **Content Script Constraints:**
   - No direct `chrome.storage` writes — send messages to background
   - Use Shadow DOM for injected overlays to prevent style conflicts
   - Idempotency guard: check `window.__extension_initialized__` before running

## Component Strategy (Popup)

- **Styling**: Tailwind CSS v4 via `@tailwindcss/vite` plugin
- **No env vars**: Chrome extensions use `chrome.storage.local` for all configuration

## State Management

- **No global state library** — chrome.storage.local is the source of truth
- **Popup state**: `useState` + `useEffect` fetching from background via messaging
- **Background state**: In-memory cache backed by `chrome.storage.local`

## Key Pattern References

| Pattern | Example File |
|---------|-------------|
| Service worker | `src/background/index.ts` |
| Content script | `src/content/index.ts` |
| Popup component | `src/popup/App.tsx` |
| Message types | `src/shared/types/messages.ts` |
| Messaging wrappers | `src/shared/messaging.ts` |

# External Knowledge (Context7)

**CRITICAL:** Our stack uses modern/bleeding-edge libraries (React 19, Tailwind v4, Vite 6, Chrome MV3 APIs).

**Rule:** When writing code for ANY external library:

1. **Verify First:** If you are not 100% certain of the syntax for the _specific version_ we are using, you **MUST** use the `context7` tool to fetch the documentation.
2. **Scope:** This applies to:
   - Core Frameworks (React 19)
   - Styling (Tailwind CSS v4)
   - Build Tools (Vite 6)
   - Chrome Extension APIs (MV3)
3. **Do Not Guess:** Do not rely on your training data, as it often contains outdated examples that will break our build.
