import type { ExtensionMessage } from '@/shared/types/messages';

// Message handler
chrome.runtime.onMessage.addListener(
  (message: ExtensionMessage, _sender, sendResponse) => {
    switch (message.type) {
      case 'PING':
        sendResponse({ type: 'PONG', timestamp: Date.now() });
        break;
      // TODO: Add your message handlers here
    }
    return false; // Set to true if you need async sendResponse
  },
);

console.log('Background service worker started');
