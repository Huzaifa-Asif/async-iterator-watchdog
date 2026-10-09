export type WatchdogClassification = 'progress' | 'heartbeat' | 'ignore';

export interface WatchdogOptions<T> {
  /** Maximum time from iteration start until the first item classified as progress. */
  readonly firstProgressMs?: number;
  /** Maximum gap between activity after progress begins. Progress and heartbeat items count as activity. */
  readonly idleMs?: number;
  /** Maximum total iteration duration, regardless of activity. */
  readonly totalMs?: number;
  /** Caller-owned cancellation. Its reason is rethrown unchanged. */
  readonly signal?: AbortSignal;
  /** Classify each yielded item for deadline accounting. All items are still yielded. */
  readonly classify?: (value: T, index: number) => WatchdogClassification;
}

export class WatchdogTimeoutError extends Error {
  constructor(details: {
    readonly phase: 'first-progress' | 'idle' | 'total';
    readonly timeoutMs: number;
    readonly elapsedMs: number;
    readonly itemsSeen: number;
    readonly progressItems: number;
  });
  readonly code: 'ASYNC_ITERATOR_TIMEOUT';
  readonly phase: 'first-progress' | 'idle' | 'total';
  readonly timeoutMs: number;
  readonly elapsedMs: number;
  readonly itemsSeen: number;
  readonly progressItems: number;
}

export function watchAsyncIterable<T>(
  source: AsyncIterable<T> | AsyncIterator<T>,
  options: WatchdogOptions<T>
): AsyncIterable<T>;

export const classifications: readonly WatchdogClassification[];
