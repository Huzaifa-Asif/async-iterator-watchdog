import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import test from 'node:test';
import { WatchdogTimeoutError, classifications, watchAsyncIterable } from '../src/index.js';

const require = createRequire(import.meta.url);
const delay = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

async function collect(iterable) {
  const values = [];
  for await (const value of iterable) values.push(value);
  return values;
}

function hangingIterator(values = []) {
  let index = 0;
  let returns = 0;
  return {
    next() {
      if (index < values.length) return Promise.resolve({ done: false, value: values[index++] });
      return new Promise(() => {});
    },
    return() {
      returns += 1;
      return Promise.resolve({ done: true });
    },
    get returns() { return returns; },
    [Symbol.asyncIterator]() { return this; }
  };
}

test('exports stable classifications', () => {
  assert.deepEqual(classifications, ['progress', 'heartbeat', 'ignore']);
  assert.equal(Object.isFrozen(classifications), true);
});

test('passes every value through in order', async () => {
  async function* source() { yield 1; yield 2; yield 3; }
  assert.deepEqual(await collect(watchAsyncIterable(source(), { totalMs: 100 })), [1, 2, 3]);
});

test('deterministic fuzz preserves mixed classified sequences', async () => {
  let seed = 0x5eed1234;
  const random = () => {
    seed = (Math.imul(seed, 1_664_525) + 1_013_904_223) >>> 0;
    return seed / 0x1_0000_0000;
  };
  const labels = ['progress', 'heartbeat', 'ignore'];
  for (let run = 0; run < 100; run += 1) {
    const values = Array.from({ length: Math.floor(random() * 25) }, (_, index) => ({ index, kind: labels[Math.floor(random() * labels.length)] }));
    async function* source() { yield* values; }
    const actual = await collect(watchAsyncIterable(source(), { totalMs: 1_000, classify: (item) => item.kind }));
    assert.deepEqual(actual, values);
  }
});

test('supports a bare AsyncIterator', async () => {
  let next = 0;
  const iterator = { next: async () => next++ === 0 ? { value: 'ok', done: false } : { done: true } };
  assert.deepEqual(await collect(watchAsyncIterable(iterator, { totalMs: 100 })), ['ok']);
});

test('first-progress deadline reports typed metadata and closes', async () => {
  const iterator = hangingIterator();
  await assert.rejects(collect(watchAsyncIterable(iterator, { firstProgressMs: 20 })), (error) => {
    assert.ok(error instanceof WatchdogTimeoutError);
    assert.equal(error.code, 'ASYNC_ITERATOR_TIMEOUT');
    assert.equal(error.phase, 'first-progress');
    assert.equal(error.timeoutMs, 20);
    assert.equal(error.itemsSeen, 0);
    assert.equal(error.progressItems, 0);
    assert.ok(error.elapsedMs >= 0);
    return true;
  });
  assert.equal(iterator.returns, 1);
});

test('idle deadline starts only after first progress', async () => {
  const iterator = hangingIterator(['first']);
  const output = watchAsyncIterable(iterator, { firstProgressMs: 100, idleMs: 20 });
  const seen = [];
  await assert.rejects((async () => { for await (const item of output) seen.push(item); })(), (error) => {
    assert.equal(error.phase, 'idle');
    assert.equal(error.itemsSeen, 1);
    assert.equal(error.progressItems, 1);
    return true;
  });
  assert.deepEqual(seen, ['first']);
});

test('heartbeat activity resets idle after progress', async () => {
  async function* source() {
    yield { type: 'text', value: 'a' };
    await delay(12);
    yield { type: 'ping' };
    await delay(12);
    yield { type: 'text', value: 'b' };
  }
  const values = await collect(watchAsyncIterable(source(), {
    idleMs: 20,
    totalMs: 200,
    classify: (item) => item.type === 'ping' ? 'heartbeat' : 'progress'
  }));
  assert.equal(values.length, 3);
});

test('heartbeats do not satisfy the first-progress deadline', async () => {
  async function* source() {
    while (true) { await delay(6); yield 'ping'; }
  }
  await assert.rejects(collect(watchAsyncIterable(source(), {
    firstProgressMs: 25,
    classify: () => 'heartbeat'
  })), (error) => {
    assert.equal(error.phase, 'first-progress');
    assert.ok(error.itemsSeen >= 1);
    assert.equal(error.progressItems, 0);
    return true;
  });
});

test('ignored items do not reset idle', async () => {
  async function* source() {
    yield 'progress';
    while (true) { await delay(6); yield 'noise'; }
  }
  await assert.rejects(collect(watchAsyncIterable(source(), {
    idleMs: 25,
    classify: (item) => item === 'noise' ? 'ignore' : 'progress'
  })), (error) => {
    assert.equal(error.phase, 'idle');
    assert.ok(error.itemsSeen >= 2);
    return true;
  });
});

test('total deadline is absolute despite continuing progress', async () => {
  async function* source() { while (true) { await delay(6); yield 'chunk'; } }
  await assert.rejects(collect(watchAsyncIterable(source(), { idleMs: 30, totalMs: 25 })), (error) => {
    assert.equal(error.phase, 'total');
    assert.equal(error.timeoutMs, 25);
    return true;
  });
});

test('external abort rethrows the caller reason and closes', async () => {
  const controller = new AbortController();
  const reason = new Error('stop now');
  const iterator = hangingIterator();
  const promise = collect(watchAsyncIterable(iterator, { totalMs: 500, signal: controller.signal }));
  setTimeout(() => controller.abort(reason), 10);
  await assert.rejects(promise, (error) => error === reason);
  assert.equal(iterator.returns, 1);
});

test('already-aborted signal does not pull the iterator', async () => {
  const reason = new Error('already stopped');
  const controller = new AbortController();
  controller.abort(reason);
  let calls = 0;
  const iterator = { next() { calls += 1; return new Promise(() => {}); }, return() {}, [Symbol.asyncIterator]() { return this; } };
  await assert.rejects(collect(watchAsyncIterable(iterator, { signal: controller.signal })), (error) => error === reason);
  assert.equal(calls, 0);
});

test('detects abort triggered synchronously inside next', async () => {
  const reason = new Error('aborted during pull');
  const controller = new AbortController();
  const iterator = {
    next() { controller.abort(reason); return new Promise(() => {}); },
    return() { return { done: true }; },
    [Symbol.asyncIterator]() { return this; }
  };
  await assert.rejects(collect(watchAsyncIterable(iterator, { totalMs: 500, signal: controller.signal })), (error) => error === reason);
});

test('preserves an upstream asynchronous error', async () => {
  const expected = new Error('provider failed');
  const iterator = { next: () => Promise.reject(expected), [Symbol.asyncIterator]() { return this; } };
  await assert.rejects(collect(watchAsyncIterable(iterator, { totalMs: 100 })), (error) => error === expected);
});

test('preserves a synchronous next error', async () => {
  const expected = new Error('bad next');
  const iterator = { next() { throw expected; }, [Symbol.asyncIterator]() { return this; } };
  await assert.rejects(collect(watchAsyncIterable(iterator, { totalMs: 100 })), (error) => error === expected);
});

test('rejects malformed iterator results', async () => {
  let closed = false;
  const iterator = { next: async () => null, return() { closed = true; }, [Symbol.asyncIterator]() { return this; } };
  await assert.rejects(collect(watchAsyncIterable(iterator, { totalMs: 100 })), /iterator result object/);
  assert.equal(closed, true);
});

test('rejects invalid classifier output and closes', async () => {
  const iterator = hangingIterator(['value']);
  await assert.rejects(collect(watchAsyncIterable(iterator, { totalMs: 100, classify: () => 'other' })), /classify must return/);
  assert.equal(iterator.returns, 1);
});

test('preserves a classifier error', async () => {
  const expected = new Error('classifier failed');
  const iterator = hangingIterator(['value']);
  await assert.rejects(collect(watchAsyncIterable(iterator, { totalMs: 100, classify() { throw expected; } })), (error) => error === expected);
});

test('consumer break requests upstream cleanup once', async () => {
  const iterator = hangingIterator([1, 2]);
  for await (const value of watchAsyncIterable(iterator, { totalMs: 100 })) {
    assert.equal(value, 1);
    break;
  }
  await delay(0);
  assert.equal(iterator.returns, 1);
});

test('normal completion does not call return', async () => {
  let returns = 0;
  let index = 0;
  const iterator = {
    next: async () => index++ === 0 ? { done: false, value: 1 } : { done: true },
    return() { returns += 1; },
    [Symbol.asyncIterator]() { return this; }
  };
  assert.deepEqual(await collect(watchAsyncIterable(iterator, { totalMs: 100 })), [1]);
  assert.equal(returns, 0);
});

test('a throwing return does not mask a timeout', async () => {
  const iterator = { next: () => new Promise(() => {}), return() { throw new Error('cleanup failed'); }, [Symbol.asyncIterator]() { return this; } };
  await assert.rejects(collect(watchAsyncIterable(iterator, { firstProgressMs: 15 })), (error) => error.phase === 'first-progress');
});

test('a rejecting return does not mask a timeout', async () => {
  const iterator = { next: () => new Promise(() => {}), return: () => Promise.reject(new Error('cleanup failed')), [Symbol.asyncIterator]() { return this; } };
  await assert.rejects(collect(watchAsyncIterable(iterator, { firstProgressMs: 15 })), (error) => error.phase === 'first-progress');
  await delay(0);
});

test('validates every duration boundary', () => {
  for (const value of [0, -1, 1.5, NaN, -Infinity, MAX_SAFE_PLUS_ONE(), 2_147_483_648, '10', null]) {
    assert.throws(() => watchAsyncIterable(hangingIterator(), { firstProgressMs: value }), /firstProgressMs/);
  }
  assert.doesNotThrow(() => watchAsyncIterable(hangingIterator(), { firstProgressMs: 1 }));
  assert.doesNotThrow(() => watchAsyncIterable(hangingIterator(), { firstProgressMs: 2_147_483_647 }));
  assert.doesNotThrow(() => watchAsyncIterable(hangingIterator(), { firstProgressMs: Infinity, totalMs: 1 }));
});

function MAX_SAFE_PLUS_ONE() { return Number.MAX_SAFE_INTEGER + 1; }

test('validates options, classifier, signal, and configured policy', () => {
  assert.throws(() => watchAsyncIterable(hangingIterator(), null), /options/);
  assert.throws(() => watchAsyncIterable(hangingIterator(), []), /options/);
  assert.throws(() => watchAsyncIterable(hangingIterator(), { classify: true, totalMs: 1 }), /classify/);
  assert.throws(() => watchAsyncIterable(hangingIterator(), { signal: {}, totalMs: 1 }), /AbortSignal/);
  assert.throws(() => watchAsyncIterable(hangingIterator(), {}), /at least one/);
  assert.throws(() => watchAsyncIterable(hangingIterator(), { totalMs: Infinity }), /at least one/);
});

test('rejects an invalid source when iteration begins', async () => {
  await assert.rejects(collect(watchAsyncIterable({}, { totalMs: 100 })), /AsyncIterable or AsyncIterator/);
});

test('CJS exports mirror ESM and execute the same policy', async () => {
  const cjs = require('../src/index.cjs');
  assert.deepEqual(Object.keys(cjs).sort(), ['WatchdogTimeoutError', 'classifications', 'watchAsyncIterable'].sort());
  async function* source() { yield 'cjs'; }
  assert.deepEqual(await collect(cjs.watchAsyncIterable(source(), { totalMs: 100 })), ['cjs']);
});
