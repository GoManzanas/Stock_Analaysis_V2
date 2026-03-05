const storageMock: Record<string, unknown> = {};

const chromeMock = {
  runtime: {
    sendMessage: vi.fn(),
    onMessage: {
      addListener: vi.fn(),
      removeListener: vi.fn(),
      hasListener: vi.fn(),
    },
    getURL: vi.fn((path: string) => `chrome-extension://mock-id/${path}`),
    id: 'mock-extension-id',
  },
  storage: {
    local: {
      get: vi.fn((keys: string | string[]) => {
        if (typeof keys === 'string') {
          return Promise.resolve({ [keys]: storageMock[keys] });
        }
        const result: Record<string, unknown> = {};
        for (const key of keys) {
          result[key] = storageMock[key];
        }
        return Promise.resolve(result);
      }),
      set: vi.fn((items: Record<string, unknown>) => {
        Object.assign(storageMock, items);
        return Promise.resolve();
      }),
      remove: vi.fn((keys: string | string[]) => {
        const keysArr = typeof keys === 'string' ? [keys] : keys;
        for (const key of keysArr) {
          delete storageMock[key];
        }
        return Promise.resolve();
      }),
    },
  },
  tabs: {
    sendMessage: vi.fn(),
    query: vi.fn(() => Promise.resolve([])),
  },
  alarms: {
    create: vi.fn(),
    clear: vi.fn(),
    onAlarm: {
      addListener: vi.fn(),
      removeListener: vi.fn(),
    },
  },
};

Object.defineProperty(globalThis, 'chrome', {
  value: chromeMock,
  writable: true,
});
