import type { ExtensionMessage } from '@/shared/types/messages';

export function sendToBackground(message: ExtensionMessage): Promise<unknown> {
  return chrome.runtime.sendMessage(message);
}

export function sendToContent(tabId: number, message: ExtensionMessage): Promise<unknown> {
  return chrome.tabs.sendMessage(tabId, message);
}

export function isExtensionMessage(message: unknown): message is ExtensionMessage {
  return (
    typeof message === 'object' &&
    message !== null &&
    'type' in message &&
    typeof (message as Record<string, unknown>).type === 'string'
  );
}
