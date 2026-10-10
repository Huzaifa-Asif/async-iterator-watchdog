# async-iterator-watchdog

Enforce first-progress, idle, and total deadlines on JavaScript `AsyncIterable` streams.

`async-iterator-watchdog` is a small, zero-dependency liveness wrapper for long-lived streams. It distinguishes useful progress from heartbeat traffic, preserves every yielded item, supports caller cancellation, and requests upstream cleanup when iteration stops.

It does **not** retry requests, parse a provider protocol, repair streams, or guarantee that an uncooperative producer has stopped.

## Why this exists

A request timeout often covers only connection setup or one promise. Streaming consumers need different answers to three questions:

- How long may the stream take to produce its first meaningful item?
- How long may it remain inactive after useful output begins?
- How long may the whole operation run even while heartbeats or chunks continue?

These are separate failure modes. Official SDK issue reports describe indefinitely hanging clients, missing per-chunk/first-chunk controls, and heartbeat events needed for liveness. This package provides a provider-neutral primitive; it does not claim the underlying timeout or abort techniques are novel.

## Install

```sh
npm install async-iterator-watchdog
```

Node.js 18 or newer is supported. The runtime has no dependencies and uses standard async iteration, timers, and `AbortSignal` APIs.

## Quick start

```js
import {
  WatchdogTimeoutError,
  watchAsyncIterable
} from 'async-iterator-watchdog';

const watched = watchAsyncIterable(providerEvents, {
  firstProgressMs: 10_000,
  idleMs: 30_000,
  totalMs: 5 * 60_000,
  classify: (event) => {
    if (event.type === 'ping') return 'heartbeat';
    if (event.type === 'text_delta') return 'progress';
    return 'ignore';
  }
});

try {
  for await (const event of watched) {
    consume(event);
  }
} catch (error) {
  if (error instanceof WatchdogTimeoutError) {
    console.error(error.phase, error.itemsSeen, error.progressItems);
  } else {
    throw error;
  }
}
```

CommonJS is supported too:

```js
const { watchAsyncIterable } = require('async-iterator-watchdog');
```

## Classification model

The optional `classify(value, index)` callback returns one of:

| Result | Item is yielded | Satisfies first progress | Resets idle deadline |
| --- | --- | --- | --- |
| `progress` | yes | yes | yes |
| `heartbeat` | yes | no | yes, after progress starts |
| `ignore` | yes | no | no |

Without `classify`, every item is `progress`.

Heartbeats intentionally do not satisfy `firstProgressMs`: a connection that only pings has not produced useful output. `idleMs` starts after the first progress item, so an idle-only policy does not bound initial silence. Configure `firstProgressMs` when that matters.

## API

### `watchAsyncIterable(source, options)`

Returns a new `AsyncIterable` that yields the source values unchanged and in order.

`source` may be an `AsyncIterable<T>` or a bare `AsyncIterator<T>`.

At least one deadline or `signal` is required.

#### `options.firstProgressMs`

Maximum milliseconds from the start of iteration until the first item classified as `progress`.

#### `options.idleMs`

Maximum milliseconds without `progress` or `heartbeat` activity after the first progress item.

#### `options.totalMs`

Maximum total iteration duration. Continuing chunks and heartbeats do not extend it.

Each deadline must be a positive integer no greater than `2_147_483_647`. Omit it or pass `Infinity` to disable it.

#### `options.signal`

An optional caller-owned `AbortSignal`. Its `reason` is thrown unchanged. The wrapper never aborts or mutates the supplied controller.

#### `options.classify`

```ts
(value: T, index: number) => 'progress' | 'heartbeat' | 'ignore'
```

The callback is synchronous. A thrown error passes through unchanged. An invalid return value raises `TypeError`.

### `WatchdogTimeoutError`

Timeouts reject iteration with:

```ts
class WatchdogTimeoutError extends Error {
  code: 'ASYNC_ITERATOR_TIMEOUT';
  phase: 'first-progress' | 'idle' | 'total';
  timeoutMs: number;
  elapsedMs: number;
  itemsSeen: number;
  progressItems: number;
}
```

`elapsedMs` is diagnostic timing, not a real-time guarantee. JavaScript timer scheduling may run later under load.

### `classifications`

A frozen array containing the three accepted classification strings.

## Cancellation and cleanup

When the wrapper observes a timeout, caller abort, classifier failure, malformed iterator result, or consumer break, it calls the upstream iterator's `return()` once when available. That call is best effort and is not awaited, because a producer with a stuck `next()` can also leave `return()` pending forever. Cleanup errors—including a throwing `return` getter—are suppressed so they cannot replace the primary timeout or abort reason.

An async generator does not execute while its consumer is paused at a yielded value. If the caller aborts or a deadline expires during that pause, the wrapper observes it and requests cleanup when the consumer next calls `next()`/`return()` or resumes `for await`. If the consumer abandons the iterator without closing or resuming it, JavaScript provides no hook for this wrapper to run cleanup.

Use the provider or transport's own cancellation mechanism as well. If it accepts an `AbortSignal`, pass that signal into the provider request and into this wrapper. A JavaScript wrapper cannot forcibly stop arbitrary work that ignores cancellation.

```js
const controller = new AbortController();
const stream = client.stream({ signal: controller.signal });

for await (const event of watchAsyncIterable(stream, {
  idleMs: 30_000,
  signal: controller.signal
})) {
  // ...
}
```

This package does not automatically abort `controller` when its own deadline expires because the controller is caller-owned. Catch `WatchdogTimeoutError` and cancel the transport explicitly if required.

## Choosing deadlines

- Base deadlines on observed service latency, not the model's nominal token rate.
- Leave room for queueing, tool execution, and temporary backpressure.
- Treat protocol pings as `heartbeat`, not `progress`.
- Use `totalMs` as a hard operational budget rather than a substitute for idle detection.
- Start conservatively and measure timeout phases before tightening values.

## Limits

- The wrapper observes only when `iterator.next()` settles. It cannot see network bytes hidden inside an SDK.
- It does not cancel an underlying HTTP request unless that producer shares and honors a caller-controlled signal.
- The classifier is synchronous and runs in the consumer's event loop.
- Multiple concurrent `next()` calls are serialized by the returned async generator; this is a pull-stream utility, not a multicast primitive.
- Deadlines use the runtime's monotonic clock when available, but timer callbacks can be delayed by event-loop stalls.
- Cleanup is best effort, and late settlement of an already timed-out `next()` is ignored.
- Deadlines are checked before each pull and before accepting each settled result, so immediately resolved promises cannot starve them. An expiry while the consumer is paused surfaces on the consumer's next interaction with the iterator.
- The package has no built-in metrics, logging, retry, buffering, or provider-specific event knowledge.

## Troubleshooting

### The stream times out before content even though pings arrive

Return `heartbeat` for pings and choose a larger `firstProgressMs`. Heartbeats intentionally do not count as meaningful first progress.

### `idleMs` never fires before the first item

That is expected. Idle monitoring begins after the first progress item. Add `firstProgressMs` to bound startup silence.

### The timeout fired but the HTTP request is still active

The producer is not being forcibly cancelled. Share an `AbortSignal` with the underlying SDK or transport, and abort its controller when handling the timeout.

### Metadata events keep the stream alive

Classify metadata as `ignore`. All events are still yielded; classification only affects watchdog accounting.

### I need automatic retries

Keep retries outside this wrapper. Retrying a partially consumed stream can duplicate output or side effects and requires application-specific resume/idempotency rules.

## Evidence and alternatives

The design was motivated by public issue reports, including [Vercel AI SDK #22073](https://github.com/vercel/ai/issues/22073), [Vercel AI SDK #22379](https://github.com/vercel/ai/issues/22379), [Anthropic TypeScript SDK #867](https://github.com/anthropics/anthropic-sdk-typescript/issues/867), and [Anthropic TypeScript SDK #749](https://github.com/anthropics/anthropic-sdk-typescript/issues/749). These reports demonstrate failure modes; they are not endorsements of this package.

Use [`p-timeout`](https://www.npmjs.com/package/p-timeout) when you only need to bound one promise. Use [`abortable-iterator`](https://www.npmjs.com/package/abortable-iterator) when a separate component already owns the abort policy. Use a provider SDK's native stream deadlines when they fully cover your runtime and semantics.

## Runnable example and related tool

[`examples/heartbeat-aware.mjs`](examples/heartbeat-aware.mjs) demonstrates
separate heartbeat and progress classification. It is executed by
`npm run check` and ships in the npm tarball.

For checking whether decoded OpenAI Responses or Anthropic Messages events
formed a valid, successful lifecycle, see
[`ai-stream-auditor`](https://www.npmjs.com/package/ai-stream-auditor).

## Development

```sh
npm ci --ignore-scripts
npm run check
```

See [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and the runnable [heartbeat-aware example](examples/heartbeat-aware.mjs).

## License

MIT © Huzaifa Asif. See [LICENSE](LICENSE) and [NOTICE.md](NOTICE.md).
