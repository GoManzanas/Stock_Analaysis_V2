---
description: When writing tests or creating test files
---

# Testing Strategy

## Unit Tests (Vitest + jsdom)

### The Boundary Rule

- Mock at the **Chrome messaging boundary** — `chrome.runtime.sendMessage`, `chrome.storage.local`, `chrome.tabs`
- Never mock Chrome internals directly in component tests

### Pattern

1. **Global Chrome mock** in `src/__tests__/mocks/chrome.ts` provides baseline mocking
2. **Override in individual tests** when you need specific behavior:

   ```typescript
   vi.mocked(chrome.runtime.sendMessage).mockResolvedValue({ type: 'PONG', timestamp: 123 });
   ```

3. **Test the UI**: Verify rendering, interactions, and state changes

### What to Test

- Message handler logic (background script switch cases)
- Popup component rendering and interactions
- Utility functions and data transformations
- Content script initialization logic

### What NOT to Test

- Chrome API internals (they're mocked)
- Simple pass-through components
- Static configuration/constants
- Third-party library behavior

## Manual E2E Testing

Load `dist/` as an unpacked extension in `chrome://extensions/` and test the full flow manually.

## Important Notes

- `jsdom` must be explicitly installed as a dev dependency (not bundled with Vitest)
- Chrome mock is set up globally via `src/__tests__/setup.ts`
- Use `vi.mocked()` for type-safe mock access
