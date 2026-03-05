describe('background service worker', () => {
  it('responds to PING with PONG', () => {
    let messageHandler: (
      message: { type: string },
      sender: unknown,
      sendResponse: (response: unknown) => void,
    ) => boolean | undefined;

    vi.mocked(chrome.runtime.onMessage.addListener).mockImplementation((handler) => {
      messageHandler = handler as typeof messageHandler;
    });

    // Import background script to register the handler
    import('@/background/index');

    // Wait for dynamic import
    return new Promise<void>((resolve) => {
      setTimeout(() => {
        expect(messageHandler).toBeDefined();

        const sendResponse = vi.fn();
        messageHandler({ type: 'PING' }, {}, sendResponse);

        expect(sendResponse).toHaveBeenCalledWith(
          expect.objectContaining({
            type: 'PONG',
            timestamp: expect.any(Number),
          }),
        );
        resolve();
      }, 100);
    });
  });
});
