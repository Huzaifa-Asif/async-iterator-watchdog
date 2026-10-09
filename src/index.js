const MAX_TIMER_MS = 2_147_483_647;
const CLASSIFICATIONS = Object.freeze(['progress', 'heartbeat', 'ignore']);

export class WatchdogTimeoutError extends Error {
  constructor({ phase, timeoutMs, elapsedMs, itemsSeen, progressItems }) {
    super(`Async iterator ${phase} deadline exceeded after ${timeoutMs} ms`);
    this.name = 'WatchdogTimeoutError';
    this.code = 'ASYNC_ITERATOR_TIMEOUT';
    this.phase = phase;
    this.timeoutMs = timeoutMs;
    this.elapsedMs = elapsedMs;
    this.itemsSeen = itemsSeen;
    this.progressItems = progressItems;
  }
}

function normalizeDuration(value, name) {
  if (value === undefined || value === Infinity) return Infinity;
  if (!Number.isSafeInteger(value) || value < 1 || value > MAX_TIMER_MS) {
    throw new TypeError(`${name} must be a positive safe integer no greater than ${MAX_TIMER_MS}, or Infinity`);
  }
  return value;
}

function monotonicNow() {
  return globalThis.performance?.now?.() ?? Date.now();
}

function abortReason(signal) {
  if (signal.reason !== undefined) return signal.reason;
  const error = new Error('The operation was aborted');
  error.name = 'AbortError';
  error.code = 'ABORT_ERR';
  return error;
}

function getIterator(source) {
  if (source === null || source === undefined) throw new TypeError('source must be an AsyncIterable or AsyncIterator');
  const factory = source[Symbol.asyncIterator];
  const iterator = typeof factory === 'function' ? factory.call(source) : source;
  if (!iterator || typeof iterator.next !== 'function') throw new TypeError('source must be an AsyncIterable or AsyncIterator');
  return iterator;
}

function closeBestEffort(iterator, state) {
  if (state.closed || typeof iterator.return !== 'function') return;
  state.closed = true;
  try {
    Promise.resolve(iterator.return()).catch(() => {});
  } catch {
    // Cleanup is intentionally best effort so a broken return() cannot mask the primary outcome.
  }
}

function earliestDeadline(state, config) {
  const candidates = [];
  if (config.totalMs !== Infinity) candidates.push({ phase: 'total', timeoutMs: config.totalMs, dueAt: state.startedAt + config.totalMs });
  if (state.progressItems === 0 && config.firstProgressMs !== Infinity) {
    candidates.push({ phase: 'first-progress', timeoutMs: config.firstProgressMs, dueAt: state.startedAt + config.firstProgressMs });
  }
  if (state.progressItems > 0 && config.idleMs !== Infinity) {
    candidates.push({ phase: 'idle', timeoutMs: config.idleMs, dueAt: state.lastActivityAt + config.idleMs });
  }
  candidates.sort((left, right) => left.dueAt - right.dueAt);
  return candidates[0];
}

function waitForNext(iterator, state, config) {
  if (config.signal?.aborted) return Promise.reject(abortReason(config.signal));
  let next;
  try {
    next = iterator.next();
  } catch (error) {
    return Promise.reject(error);
  }
  const deadline = earliestDeadline(state, config);
  if (!deadline && !config.signal) return Promise.resolve(next);

  return new Promise((resolve, reject) => {
    let timer;
    let settled = false;
    const finish = (callback, value) => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      config.signal?.removeEventListener('abort', onAbort);
      callback(value);
    };
    const onAbort = () => finish(reject, abortReason(config.signal));
    config.signal?.addEventListener('abort', onAbort, { once: true });
    if (config.signal?.aborted) {
      onAbort();
      return;
    }
    if (deadline) {
      const delay = Math.max(0, deadline.dueAt - monotonicNow());
      timer = setTimeout(() => {
        finish(reject, new WatchdogTimeoutError({
          phase: deadline.phase,
          timeoutMs: deadline.timeoutMs,
          elapsedMs: Math.max(0, Math.round(monotonicNow() - state.startedAt)),
          itemsSeen: state.itemsSeen,
          progressItems: state.progressItems
        }));
      }, delay);
    }
    Promise.resolve(next).then(
      (result) => finish(resolve, result),
      (error) => finish(reject, error)
    );
  });
}

export function watchAsyncIterable(source, options = {}) {
  if (!options || typeof options !== 'object' || Array.isArray(options)) throw new TypeError('options must be an object');
  const config = {
    firstProgressMs: normalizeDuration(options.firstProgressMs, 'firstProgressMs'),
    idleMs: normalizeDuration(options.idleMs, 'idleMs'),
    totalMs: normalizeDuration(options.totalMs, 'totalMs'),
    signal: options.signal,
    classify: options.classify ?? (() => 'progress')
  };
  if (config.signal !== undefined && (!config.signal || typeof config.signal.addEventListener !== 'function' || typeof config.signal.removeEventListener !== 'function' || typeof config.signal.aborted !== 'boolean')) {
    throw new TypeError('signal must be an AbortSignal');
  }
  if (typeof config.classify !== 'function') throw new TypeError('classify must be a function');
  if (config.firstProgressMs === Infinity && config.idleMs === Infinity && config.totalMs === Infinity && !config.signal) {
    throw new TypeError('at least one deadline or signal is required');
  }

  return {
    async *[Symbol.asyncIterator]() {
      const iterator = getIterator(source);
      const closeState = { closed: false };
      const state = { startedAt: monotonicNow(), lastActivityAt: 0, itemsSeen: 0, progressItems: 0 };
      let completed = false;
      try {
        while (true) {
          const result = await waitForNext(iterator, state, config);
          if (!result || typeof result !== 'object') throw new TypeError('iterator.next() must resolve to an iterator result object');
          if (result.done) {
            completed = true;
            return;
          }
          const index = state.itemsSeen;
          state.itemsSeen += 1;
          const classification = config.classify(result.value, index);
          if (!CLASSIFICATIONS.includes(classification)) {
            throw new TypeError('classify must return "progress", "heartbeat", or "ignore"');
          }
          const observedAt = monotonicNow();
          if (classification === 'progress') {
            state.progressItems += 1;
            state.lastActivityAt = observedAt;
          } else if (classification === 'heartbeat' && state.progressItems > 0) {
            state.lastActivityAt = observedAt;
          }
          yield result.value;
        }
      } finally {
        if (!completed) closeBestEffort(iterator, closeState);
      }
    }
  };
}

export const classifications = CLASSIFICATIONS;
