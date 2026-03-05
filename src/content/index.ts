// Idempotency guard — prevents double initialization on SPA navigations
declare global {
  interface Window {
    __extension_initialized__?: boolean;
  }
}

if (!window.__extension_initialized__) {
  window.__extension_initialized__ = true;

  // TODO: Add your content script logic here
  // Example: Extract page metadata, inject UI, observe DOM changes

  console.log('Content script initialized');
}

export {};
