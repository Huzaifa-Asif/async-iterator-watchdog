# Contributing

Thanks for helping improve `async-iterator-watchdog`.

## Before opening a change

- Search existing issues and describe the concrete async-iteration failure mode.
- Keep the core provider-neutral and dependency-free.
- Do not add automatic retry or protocol-specific parsing to the core API.
- Discuss breaking API changes before implementation.

## Development

Use Node.js 18 or newer.

```sh
npm ci --ignore-scripts
npm run check
```

Changes should include focused tests for timing boundaries, cancellation, cleanup, and both module formats when relevant. Avoid flaky assertions that depend on an exact callback timestamp.

## Pull requests

Explain the behavior change, its operational tradeoffs, and the verification performed. By contributing, you agree that your contribution is licensed under the project's MIT License.

Please follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). Report vulnerabilities through [SECURITY.md](SECURITY.md), not a public issue.
