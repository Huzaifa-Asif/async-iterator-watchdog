import { WatchdogTimeoutError, watchAsyncIterable } from 'async-iterator-watchdog';

async function* providerEvents() {
  yield { type: 'response.created' };
  yield { type: 'ping' };
  yield { type: 'response.output_text.delta', delta: 'Hello' };
}

try {
  const watched = watchAsyncIterable(providerEvents(), {
    firstProgressMs: 10_000,
    idleMs: 30_000,
    totalMs: 5 * 60_000,
    classify: (event) => {
      if (event.type === 'ping') return 'heartbeat';
      if (event.type === 'response.output_text.delta') return 'progress';
      return 'ignore';
    }
  });

  for await (const event of watched) console.log(event);
} catch (error) {
  if (error instanceof WatchdogTimeoutError) {
    console.error(`Stream timed out in phase: ${error.phase}`);
  } else {
    throw error;
  }
}
