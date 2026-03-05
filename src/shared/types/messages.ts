// Discriminated union for all extension messages.
// Add new message types here as the extension grows.

export interface PingMessage {
  type: 'PING';
}

export interface PongMessage {
  type: 'PONG';
  timestamp: number;
}

// TODO: Add your extension's message types here
// Example:
// export interface PageDataMessage {
//   type: 'PAGE_DATA';
//   url: string;
//   title: string;
// }

export type ExtensionMessage = PingMessage | PongMessage;
